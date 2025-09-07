import os
import pandas as pd
from flask import Flask, jsonify, render_template, request
from decimal import Decimal, getcontext
import io
from collections import deque

# Set precision for Decimal
getcontext().prec = 28

app = Flask(__name__)

def format_timedelta(td):
    """Formats a timedelta object into a D days, HH:MM:SS string."""
    days = td.days
    hours, remainder = divmod(td.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{days}d {hours:02}:{minutes:02}:{seconds:02}"

def transform_legacy_to_uta(df):
    """Transforms a legacy dataframe to the UTA format."""
    if 'Time' in df.columns:
        df.rename(columns={'Time': 'Time(UTC)'}, inplace=True)
    
    if 'Direction' in df.columns:
        df['Action'] = df['Direction'].apply(lambda x: 'OPEN' if pd.notna(x) and 'Open' in x else ('CLOSE' if pd.notna(x) and 'Close' in x else None))

    if 'Type' in df.columns:
        df['Type'] = df['Type'].str.upper()
        type_mapping = {'FUNDING': 'SETTLEMENT'}
        df['Type'] = df['Type'].replace(type_mapping)

    # Ensure all required columns exist
    required_cols = ['Time(UTC)', 'Contract', 'Type', 'Direction', 'Quantity', 'Filled Price', 'Fee Paid', 'Cash Flow', 'Action', 'Funding', 'Change']
    for col in required_cols:
        if col not in df.columns:
            df[col] = pd.NA
            
    return df

def load_and_process_files(files):
    df_list = []
    file_types = []
    for f in files:
        try:
            f.seek(0)
            content = f.read().decode('utf-8')
            stream = io.StringIO(content)
            df = pd.read_csv(stream, skiprows=1, thousands=',')

            if 'Coin' in df.columns and 'Amount' in df.columns:
                file_types.append('spot')
                df_list.append(df)
            elif 'Contract' in df.columns or 'Direction' in df.columns:
                file_types.append('contract')
                if 'Action' not in df.columns and 'Direction' in df.columns:
                    df = transform_legacy_to_uta(df)
                df_list.append(df)
            else:
                print(f"Skipping unknown file type: {f.filename}")

        except Exception as e:
            print(f"Error reading file stream for {f.filename}: {e}")
            continue
            
    if not df_list:
        return None, []
        
    return pd.concat(df_list, ignore_index=True), list(set(file_types))

def analyze_spot_trades(df):
    try:
        df['Time(UTC)'] = pd.to_datetime(df['Time(UTC)'])
        df.sort_values(by='Time(UTC)', inplace=True)

        # Convert Amount to Decimal, coercing errors
        df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce').fillna(Decimal(0)).apply(Decimal)

        # Separate trades from fees
        trades_df = df[df['Type'] == 'trade'].copy()
        fees_df = df[df['Type'] == 'tradingFee'].copy()

        # Process trades to get buys and sells
        buys = trades_df[trades_df['Amount'] > 0].copy()
        sells = trades_df[trades_df['Amount'] < 0].copy()
        sells['Amount'] = sells['Amount'].abs()

        # Calculate total fees per coin
        total_fees = fees_df.groupby('Coin')['Amount'].sum().abs().to_dict()

        realized_pnl = []
        inventory = {}

        # Merge buys and sells and sort by time
        all_trades = pd.concat([buys, sells]).sort_values(by='Time(UTC)')

        for _, row in all_trades.iterrows():
            coin = row['Coin']
            time = row['Time(UTC)']
            amount = row['Amount']
            
            if coin not in inventory:
                inventory[coin] = deque()

            # It's a buy if the original amount was positive
            is_buy = row['Amount'] > 0

            if is_buy:
                # Find the corresponding USDT transaction to get the price
                usdt_trade = trades_df[(trades_df['Time(UTC)'] == time) & (trades_df['Coin'] == 'USDT')]
                if not usdt_trade.empty:
                    usdt_amount = abs(usdt_trade.iloc[0]['Amount'])
                    price = usdt_amount / amount
                    inventory[coin].append({'qty': amount, 'price': price, 'time': time})
            else: # It's a sell
                usdt_trade = trades_df[(trades_df['Time(UTC)'] == time) & (trades_df['Coin'] == 'USDT')]
                if not usdt_trade.empty:
                    sell_price_usdt = usdt_trade.iloc[0]['Amount']
                    sell_price = sell_price_usdt / amount
                    
                    qty_to_sell = amount
                    while qty_to_sell > 0 and inventory[coin]:
                        buy_lot = inventory[coin][0]
                        matched_qty = min(qty_to_sell, buy_lot['qty'])
                        
                        pnl = (sell_price - buy_lot['price']) * matched_qty
                        
                        realized_pnl.append({
                            'coin': coin,
                            'pnl': pnl,
                            'quantity': matched_qty,
                            'buy_price': buy_lot['price'],
                            'sell_price': sell_price,
                            'buy_time': buy_lot['time'],
                            'sell_time': time
                        })
                        
                        buy_lot['qty'] -= matched_qty
                        qty_to_sell -= matched_qty
                        
                        if buy_lot['qty'] <= Decimal('1e-9'):
                            inventory[coin].popleft()

        if not realized_pnl:
            return {"error": "No realized PnL from spot trades could be calculated."}

        pnl_df = pd.DataFrame(realized_pnl)
        total_pnl = pnl_df['pnl'].sum()
        total_fees_paid = sum(total_fees.values())
        net_pnl = total_pnl - total_fees_paid

        # Basic chart
        pnl_df['cumulative_pnl'] = pnl_df['pnl'].cumsum()
        chart_labels = [pnl_df['buy_time'].min().strftime('%Y-%m-%d %H:%M')]
        chart_data = [0]
        for _, row in pnl_df.iterrows():
            chart_labels.append(row['sell_time'].strftime('%Y-%m-%d %H:%M'))
            chart_data.append(float(row['cumulative_pnl']))

        return {
            "kpi": {
                "totalPnl": float(net_pnl),
                "tradeCount": len(pnl_df),
                "totalFees": float(total_fees_paid)
            },
            "pnlChart": {
                "labels": chart_labels,
                "data": chart_data
            },
            "trades": [
                {
                    "id": f"S-{i+1}",
                    "contract": t["coin"], # Using 'contract' field for coin name
                    "pnl": float(t["pnl"]),
                    "open_time": t['buy_time'].strftime('%Y-%m-%d %H:%M:%S'),
                    "close_time": t['sell_time'].strftime('%Y-%m-%d %H:%M:%S'),
                    "quantity": float(t['quantity']),
                    "buy_price": float(t['buy_price']),
                    "sell_price": float(t['sell_price'])
                } for i, t in pnl_df.iterrows()
            ]
        }

    except Exception as e:
        import traceback
        return {"error": f"An error occurred during spot analysis: {e}\n{traceback.format_exc()}"}

def analyze_contract_trades(df, threshold_hours=24):
    try:
        threshold_seconds = threshold_hours * 3600

        if df.empty:
            return {"error": "No valid contract trade data found in uploaded files."}

        # Drop rows where essential columns are all NaN
        essential_cols = ['Time(UTC)', 'Contract', 'Type', 'Quantity', 'Filled Price']
        df.dropna(subset=essential_cols, how='all', inplace=True)

        df['Time(UTC)'] = pd.to_datetime(df['Time(UTC)'])
        numeric_cols = ['Quantity', 'Filled Price', 'Fee Paid', 'Cash Flow', 'Funding', 'Change']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(Decimal(0)).apply(Decimal)
            else:
                df[col] = Decimal(0)

        df.sort_values(by='Time(UTC)', inplace=True)

        funding_df = df[df['Type'] == 'SETTLEMENT'].copy()
        if 'Change' in funding_df.columns:
            funding_df['Funding'] = funding_df['Change']
        else:
            funding_df['Funding'] = Decimal(0)


        trade_actions_df = df[df['Action'].isin(['OPEN', 'CLOSE']) & (df['Type'] == 'TRADE')].copy()

        classified_trades = []

        for contract, group in trade_actions_df.groupby('Contract'):
            open_positions = deque()
            group = group.sort_values(by='Time(UTC)')

            for _, row in group.iterrows():
                if row['Action'] == 'OPEN':
                    open_positions.append(row.to_dict())
                elif row['Action'] == 'CLOSE':
                    qty_to_close = row['Quantity']
                    
                    while qty_to_close > 0 and open_positions:
                        open_pos = open_positions[0]
                        matched_qty = min(qty_to_close, open_pos['Quantity'])

                        pnl_ratio = matched_qty / row['Quantity'] if row['Quantity'] > 0 else Decimal(0)
                        close_pnl_part = row['Cash Flow'] * pnl_ratio
                        close_fee_part = row['Fee Paid'] * pnl_ratio
                        
                        open_fee_ratio = matched_qty / open_pos['Quantity'] if open_pos['Quantity'] > 0 else Decimal(0)
                        open_fee_part = open_pos['Fee Paid'] * open_fee_ratio

                        relevant_funding_df = funding_df[
                            (funding_df['Contract'] == contract) & 
                            (funding_df['Time(UTC)'] > open_pos['Time(UTC)']) & 
                            (funding_df['Time(UTC)'] <= row['Time(UTC)'])
                        ]
                        funding_sum = relevant_funding_df['Funding'].sum()

                        total_open_qty_in_period = group[
                            (group['Action'] == 'OPEN') & 
                            (group['Time(UTC)'] >= open_pos['Time(UTC)']) & 
                            (group['Time(UTC)'] <= row['Time(UTC)'])
                        ]['Quantity'].sum()
                        
                        funding_part = funding_sum * (matched_qty / total_open_qty_in_period) if total_open_qty_in_period > 0 else Decimal(0)

                        net_pnl = close_pnl_part + open_fee_part + close_fee_part + funding_part
                        trade_fees = open_fee_part + close_fee_part
                        holding_period = row['Time(UTC)'] - open_pos['Time(UTC)']
                        
                        classified_trades.append({
                            "contract": contract,
                            "pnl": net_pnl,
                            "holding_period_seconds": holding_period.total_seconds(),
                            "open_time": open_pos['Time(UTC)'],
                            "close_time": row['Time(UTC)'],
                            "funding_fee": funding_part,
                            "trade_fees": trade_fees,
                            "quantity": matched_qty
                        })

                        open_pos['Quantity'] -= matched_qty
                        qty_to_close -= matched_qty

                        if open_pos['Quantity'] <= Decimal('1e-9'):
                            open_positions.popleft()

        if not classified_trades:
            return {"error": "No trades could be classified from the uploaded files."}
            
        trades_df = pd.DataFrame(classified_trades)
        trades_df['open_time'] = pd.to_datetime(trades_df['open_time'])
        trades_df['close_time'] = pd.to_datetime(trades_df['close_time'])
        trades_df['holding_period_seconds'] = trades_df['holding_period_seconds'].apply(Decimal)

        def weighted_avg(df, values, weights):
            if df[weights].sum() == 0:
                return 0
            return (df[values] * df[weights]).sum() / df[weights].sum()

        agg_funcs = {
            'pnl': 'sum',
            'trade_fees': 'sum',
            'funding_fee': 'sum',
            'quantity': 'sum',
            'open_time': 'min',
            'contract': 'first'
        }
        
        agg_trades = trades_df.groupby('close_time').agg(agg_funcs)
        weighted_holding = trades_df.groupby('close_time').apply(weighted_avg, 'holding_period_seconds', 'quantity')
        agg_trades['holding_period_seconds'] = weighted_holding
        agg_trades = agg_trades.reset_index()

        grouped_trades = agg_trades.to_dict('records')
        grouped_trades.sort(key=lambda x: x['close_time'])

        cumulative_pnl = Decimal(0)
        cumulative_fees = Decimal(0)
        for i, trade in enumerate(grouped_trades):
            trade['id'] = f"T-{i+1}"
            cumulative_pnl += trade['pnl']
            cumulative_fees += trade['trade_fees']
            trade['cumulative_pnl'] = cumulative_pnl
            trade['cumulative_fees'] = cumulative_fees
            trade['type'] = '스윙' if float(trade['holding_period_seconds']) > threshold_seconds else '단타'

        total_net_pnl = sum(t['pnl'] for t in grouped_trades)
        day_trade_pnl = sum(t['pnl'] for t in grouped_trades if t['type'] == '단타')
        swing_trade_pnl = sum(t['pnl'] for t in grouped_trades if t['type'] == '스윙')

        chart_labels = [df['Time(UTC)'].min().strftime('%Y-%m-%d %H:%M')]
        chart_data = [0]
        for trade in grouped_trades:
            chart_labels.append(trade['close_time'].strftime('%Y-%m-%d %H:%M'))
            chart_data.append(float(trade['cumulative_pnl']))

        analysis_result = {
            "kpi": {
                "totalPnl": float(total_net_pnl),
                "tradeCount": len(grouped_trades),
                "dayTradePnl": float(day_trade_pnl),
                "swingTradePnl": float(swing_trade_pnl),
            },
            "pnlChart": {
                "labels": chart_labels,
                "data": chart_data
            },
            "trades": [
                {
                    "id": t["id"],
                    "contract": t["contract"],
                    "type": t["type"],
                    "pnl": float(t["pnl"]),
                    "holding_period": format_timedelta(pd.to_timedelta(float(t["holding_period_seconds"]), unit='s')),
                    "open_time": t['open_time'].strftime('%Y-%m-%d %H:%M:%S'),
                    "close_time": t['close_time'].strftime('%Y-%m-%d %H:%M:%S'),
                    "funding_fee": float(t['funding_fee']),
                    "trade_fees": float(t['trade_fees']),
                    "cumulative_pnl": float(t['cumulative_pnl']),
                    "cumulative_fees": float(t['cumulative_fees'])
                } for t in grouped_trades
            ]
        }
        return analysis_result

    except Exception as e:
        import traceback
        return {"error": f"An error occurred: {e}\n{traceback.format_exc()}"}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/analyze', methods=['POST'])
def analyze_uploaded_files():
    if 'files' not in request.files:
        return jsonify({"error": "No files part in the request"}), 400
    
    files = request.files.getlist('files')
    if not files or files[0].filename == '':
        return jsonify({"error": "No selected files"}), 400

    csv_files = [f for f in files if f.filename.lower().endswith('.csv')]
    if not csv_files:
        return jsonify({"error": "No CSV files found."}), 400

    df, file_types = load_and_process_files(csv_files)

    if df is None or df.empty:
        return jsonify({"error": "No valid data found in files."}), 400

    if len(file_types) > 1:
        return jsonify({"error": "mixed_files", "message": "선물과 현물 거래내역 파일을 함께 분석할 수 없습니다. 하나씩 업로드해주세요."}), 400
    
    analysis_type = file_types[0] if file_types else None
    threshold = int(request.form.get('threshold_hours', 24))

    if analysis_type == 'contract':
        data = analyze_contract_trades(df, threshold_hours=threshold)
    elif analysis_type == 'spot':
        data = analyze_spot_trades(df)
    else:
        return jsonify({"error": "Could not determine analysis type."}), 400

    if "error" in data:
        return jsonify(data), 500
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True, port=5001)