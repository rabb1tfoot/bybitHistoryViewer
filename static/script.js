document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('upload-form').addEventListener('submit', handleUpload);
});

// Global state
let fullTradesData = [];
let currentPage = 1;
let itemsPerPage = 10;
let currentContract = 'ALL';
let pnlChart = null;

async function handleUpload(event) {
    event.preventDefault();
    const form = event.target;
    const files = form.querySelector('#csv-files').files;
    const threshold = form.querySelector('#threshold-hours').value;
    const spinner = document.getElementById('upload-spinner');
    const mainDashboard = document.getElementById('dashboard-main');

    if (files.length === 0) {
        alert('Please select files to upload.');
        return;
    }

    const formData = new FormData();
    for (const file of files) {
        formData.append('files', file);
    }
    formData.append('threshold_hours', threshold);

    spinner.style.display = 'block';
    mainDashboard.style.display = 'none';

    try {
        const response = await fetch('/api/analyze', {
            method: 'POST',
            body: formData
        });

        const data = await response.json();

        if (!response.ok) {
            if (data.error === 'mixed_files') {
                alert(data.message);
                window.location.href = '/';
                return;
            }
            throw new Error(data.error || 'Failed to analyze files.');
        }

        fullTradesData = data.trades.sort((a, b) => new Date(a.close_time) - new Date(b.close_time));
        
        populateContractFilter();
        setupItemsPerPageControls();
        updateDashboard();
        
        mainDashboard.style.display = 'grid';

    } catch (error) {
        console.error('Error during analysis:', error);
        alert(`Error: ${error.message}`);
    } finally {
        spinner.style.display = 'none';
    }
}

function populateContractFilter() {
    const selector = document.getElementById('contract-filter');
    selector.innerHTML = ''; // Clear previous options

    const allOption = document.createElement('option');
    allOption.value = 'ALL';
    allOption.textContent = 'All Contracts';
    selector.appendChild(allOption);

    const contracts = [...new Set(fullTradesData.map(t => t.contract))];
    contracts.sort().forEach(contract => {
        const option = document.createElement('option');
        option.value = contract;
        option.textContent = contract;
        selector.appendChild(option);
    });

    selector.addEventListener('change', (e) => {
        currentContract = e.target.value;
        currentPage = 1;
        updateDashboard();
    });
}

function getFilteredTrades() {
    if (currentContract === 'ALL') {
        return fullTradesData;
    }
    return fullTradesData.filter(t => t.contract === currentContract);
}

function updateDashboard() {
    const filteredTrades = getFilteredTrades();
    updateKPIs(filteredTrades);
    renderPnlChart(filteredTrades);
    renderTableAndPagination(filteredTrades);
}

function updateKPIs(trades) {
    const totalPnl = trades.reduce((sum, t) => sum + t.pnl, 0);
    const dayTrades = trades.filter(t => t.type === '단타');
    const swingTrades = trades.filter(t => t.type === '스윙');

    document.getElementById('total-pnl').textContent = formatCurrency(totalPnl);
    setDynamicColor(document.getElementById('total-pnl'), totalPnl);
    document.getElementById('trade-count').textContent = trades.length;
    document.getElementById('day-trade-pnl').textContent = formatCurrency(dayTrades.reduce((sum, t) => sum + t.pnl, 0));
    setDynamicColor(document.getElementById('day-trade-pnl'), dayTrades.reduce((sum, t) => sum + t.pnl, 0));
    document.getElementById('swing-trade-pnl').textContent = formatCurrency(swingTrades.reduce((sum, t) => sum + t.pnl, 0));
    setDynamicColor(document.getElementById('swing-trade-pnl'), swingTrades.reduce((sum, t) => sum + t.pnl, 0));
}

function renderPnlChart(trades) {
    let cumulativePnl = 0;
    const chartData = trades.map(t => {
        cumulativePnl += t.pnl;
        return { x: new Date(t.close_time), y: cumulativePnl };
    });

    const ctx = document.getElementById('pnl-chart').getContext('2d');
    if(pnlChart) {
        pnlChart.destroy();
    }
    pnlChart = new Chart(ctx, {
        type: 'line',
        data: {
            datasets: [{
                label: '누적 손익',
                data: chartData,
                borderColor: '#4299e1',
                backgroundColor: 'rgba(66, 153, 225, 0.1)',
                fill: true,
                tension: 0.1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    type: 'time',
                    time: {
                        unit: 'day'
                    }
                }
            }
        }
    });
}

function renderTableAndPagination(trades) {
    updateTradesTable(trades);
    setupPagination(trades);
}

function updateTradesTable(trades) {
    const tableBody = document.querySelector('#trades-table tbody');
    tableBody.innerHTML = '';

    const startIndex = (currentPage - 1) * itemsPerPage;
    const endIndex = startIndex + itemsPerPage;
    const paginatedTrades = trades.slice(startIndex, endIndex);

    if (paginatedTrades.length === 0) {
        tableBody.innerHTML = `<tr><td colspan="11">No trades to display for this filter.</td></tr>`;
        return;
    }

    paginatedTrades.forEach(trade => {
        const row = document.createElement('tr');
        const pnlCell = document.createElement('td');
        pnlCell.textContent = formatCurrency(trade.pnl);
        setDynamicColor(pnlCell, trade.pnl);

        const cumulativePnlCell = document.createElement('td');
        cumulativePnlCell.textContent = formatCurrency(trade.cumulative_pnl);
        setDynamicColor(cumulativePnlCell, trade.cumulative_pnl);

        row.innerHTML = `
            <td>${trade.id}</td>
            <td>${trade.open_time}</td>
            <td>${trade.close_time}</td>
            <td>${trade.contract}</td>
            <td>${trade.type}</td>
            <td>${trade.holding_period}</td>
            <td>${formatCurrency(trade.trade_fees)}</td>
            <td>${formatCurrency(trade.funding_fee)}</td>
        `;
        row.appendChild(pnlCell);
        row.appendChild(cumulativePnlCell);

        const cumulativeFeesCell = document.createElement('td');
        cumulativeFeesCell.textContent = formatCurrency(trade.cumulative_fees);
        row.appendChild(cumulativeFeesCell);

        tableBody.appendChild(row);
    });
}

function setupPagination(trades) {
    const paginationControls = document.getElementById('pagination-controls');
    paginationControls.innerHTML = '';
    const pageCount = Math.ceil(trades.length / itemsPerPage);
    if (pageCount <= 1) return;

    const maxButtons = 10;
    let startPage, endPage;

    if (pageCount <= maxButtons) {
        startPage = 1;
        endPage = pageCount;
    } else {
        const maxPagesBeforeCurrent = Math.floor(maxButtons / 2);
        const maxPagesAfterCurrent = Math.ceil(maxButtons / 2) - 1;
        if (currentPage <= maxPagesBeforeCurrent) {
            startPage = 1;
            endPage = maxButtons;
        } else if (currentPage + maxPagesAfterCurrent >= pageCount) {
            startPage = pageCount - maxButtons + 1;
            endPage = pageCount;
        } else {
            startPage = currentPage - maxPagesBeforeCurrent;
            endPage = currentPage + maxPagesAfterCurrent;
        }
    }

    const prevButton = document.createElement('button');
    prevButton.innerHTML = '&laquo;';
    prevButton.disabled = currentPage === 1;
    prevButton.addEventListener('click', () => {
        currentPage--;
        updateDashboard();
    });
    paginationControls.appendChild(prevButton);

    for (let i = startPage; i <= endPage; i++) {
        const button = document.createElement('button');
        button.innerText = i;
        if (i === currentPage) {
            button.classList.add('active');
        }
        button.addEventListener('click', () => {
            currentPage = i;
            updateDashboard();
        });
        paginationControls.appendChild(button);
    }

    const nextButton = document.createElement('button');
    nextButton.innerHTML = '&raquo;';
    nextButton.disabled = currentPage === pageCount;
    nextButton.addEventListener('click', () => {
        currentPage++;
        updateDashboard();
    });
    paginationControls.appendChild(nextButton);
}

function setupItemsPerPageControls() {
    const buttons = document.querySelectorAll('.items-per-page-selector button');
    buttons.forEach(button => {
        button.addEventListener('click', () => {
            buttons.forEach(btn => btn.classList.remove('active'));
            button.classList.add('active');
            itemsPerPage = parseInt(button.dataset.value, 10);
            currentPage = 1;
            updateDashboard();
        });
    });
}

function formatCurrency(value) {
    if (typeof value !== 'number') return value;
    return value.toLocaleString('en-US', { style: 'currency', currency: 'USD' });
}

function setDynamicColor(element, value) {
    element.classList.remove('positive', 'negative');
    if (value > 0) {
        element.classList.add('positive');
    } else if (value < 0) {
        element.classList.add('negative');
    }
}