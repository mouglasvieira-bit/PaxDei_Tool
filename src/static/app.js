const API_BASE = '/api';

// Formatting helpers
const formatCurrency = (val) => {
    const num = parseFloat(val);
    if (isNaN(num)) return '-';
    return `${num.toFixed(1)}g`;
};
const formatPercent = (val) => {
    const num = parseFloat(val);
    if (isNaN(num)) return '-';
    return `${num.toFixed(1)}%`;
};

async function fetchJSON(endpoint) {
    try {
        const response = await fetch(`${API_BASE}${endpoint}`);
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        return await response.json();
    } catch (e) {
        console.error("Fetch Error:", e);
        return null;
    }
}

function renderTable(containerId, columns, data) {
    const container = document.getElementById(containerId);
    if (!container) return;

    if (!data || data.length === 0) {
        container.innerHTML = `
            <div class="loading-state">
                <i class="ri-inbox-archive-line" style="font-size: 2rem; margin-bottom: 0.5rem; opacity: 0.5"></i>
                <p>No data available</p>
            </div>`;
        return;
    }

    try {
        let html = `<table><thead><tr>`;
        columns.forEach(col => html += `<th>${col.header}</th>`);
        html += `</tr></thead><tbody>`;

        data.forEach(item => {
            html += `<tr>`;
            columns.forEach(col => {
                try {
                    html += `<td>${col.render(item)}</td>`;
                } catch (err) {
                    console.error("Render Error Row:", item, err);
                    html += `<td>Err</td>`;
                }
            })
            html += `</tr>`;
        });

        html += `</tbody></table>`;
        container.innerHTML = html;
    } catch (err) {
        console.error("Render Error Table:", containerId, err);
        container.innerHTML = `<p style="color:red; text-align:center">Error rendering data</p>`;
    }
}

// --- Tabs ---
function switchTab(tabId) {
    // Hide all
    document.querySelectorAll('.tab-content').forEach(el => el.style.display = 'none');
    document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));

    // Show target
    document.getElementById(`tab-${tabId}`).style.display = 'block';

    // Active btn
    const btn = document.querySelector(`button[onclick="switchTab('${tabId}')"]`);
    if (btn) btn.classList.add('active');
}

// --- Features ---

async function loadCrafting() {
    const container = document.getElementById('crafting-container');
    if (container) container.innerHTML = '<div class="loading-state"><div class="spinner"></div><p>Calculating Smart Sourcing...</p></div>';

    const data = await fetchJSON('/crafting/opportunities?top=15');

    renderTable('crafting-container', [
        { header: 'Product', render: i => `<strong style="color:var(--text-main)">${i.Produto}</strong>` },
        { header: 'Cost', render: i => `<span class="font-mono" style="color:var(--text-muted)">${formatCurrency(i.Custo_Manufatura)}</span>` },
        { header: 'Sell Price', render: i => `<span class="font-mono">${formatCurrency(i.Preco_Venda)}</span> <sup style="color:#fbbf24">Median</sup>` },
        { header: 'Spread', render: i => `<span class="font-mono profit-positive">+${formatCurrency(i.Spread)}</span>` },
        { header: 'Mrg', render: i => `<span class="font-mono">${formatPercent(i.Margem_Perc)}</span>` },
        { header: 'Strategy', render: i => `<small title="${i.Sourcing_Insumos}" style="cursor:help; border-bottom:1px dotted #666">Details</small>` },
    ], data);
}

async function loadLiquidity() {
    const container = document.getElementById('liquidity-container');
    if (container) container.innerHTML = '<div class="loading-state"><div class="spinner"></div><p>Fetching market volume...</p></div>';

    const data = await fetchJSON('/market/liquidity');
    const topData = data ? data.slice(0, 10) : [];

    renderTable('liquidity-container', [
        { header: 'Item', render: i => `<span>${i.Item}</span>` },
        { header: 'Units Sold', render: i => `<span class="font-mono">${i.Units_Sold}</span> <span style="font-size:0.8em; color:var(--text-muted)">/ day</span>` },
        { header: 'Top Zone', render: i => `<span class="zone-tag" style="background: rgba(59, 130, 246, 0.1); color: #60a5fa">${i.Top_Zone || 'Unknown'}</span>` },
    ], topData);
}

async function loadArbitrage() {
    const container = document.getElementById('arbitrage-container');
    if (container) container.innerHTML = '<div class="loading-state"><div class="spinner"></div><p>Scoring by Volume...</p></div>';

    const data = await fetchJSON('/logistics/arbitrage');

    renderTable('arbitrage-container', [
        { header: 'Item', render: i => `<strong>${i.Item}</strong>` },
        { header: 'Buy', render: i => `<div><span class="font-mono">${formatCurrency(i.Buy_Price)}</span> <br><small style="color:var(--text-muted)">${i.Buy_Zone}</small></div>` },
        { header: 'Sell Median', render: i => `<span class="font-mono">${formatCurrency(i.Avg_Sale_Price)}</span>` },
        { header: 'Margin', render: i => `<span class="font-mono profit-positive">+${formatCurrency(i.Unit_Profit)}</span>` },
        { header: 'Daily Vol', render: i => `<span class="font-mono">${i.Units_Sold}</span>` },
        { header: 'Score', render: i => `<strong style="color:#fbbf24">${Math.round(i.Score)}</strong>` },
    ], data);
}

async function triggerFetch() {
    const btn = document.querySelector('.btn-primary');
    const originalText = btn.innerHTML;
    btn.innerHTML = '<div class="spinner" style="width:20px;height:20px;border-width:2px"></div> Fetching...';
    btn.disabled = true;

    try {
        const res = await fetch('/api/admin/fetch-prices', { method: 'POST' });
        const data = await res.json();
        if (data.status === 'success') {
            alert("Prices updated! Refreshing view.");
            loadAllData();
        } else {
            alert("Error: " + data.message);
        }
    } catch (e) {
        alert("Request failed: " + e);
    } finally {
        btn.innerHTML = originalText;
        btn.disabled = false;
    }
}

async function loadOrders() {
    const data = await fetchJSON('/logistics/orders');

    // Split Data
    const constantOrders = data.filter(i => i.Order_Type === 'Constant');
    const onetimeOrders = data.filter(i => i.Order_Type !== 'Constant');

    const cols = [
        { header: 'Client', render: i => `<strong>${i.Client}</strong>` },
        { header: 'Item', render: i => `<span>${i.Item}</span>` },
        { header: 'Qty', render: i => `<span class="font-mono">${i.Quantity || '∞'}</span>` },
        { header: 'Target', render: i => `<span class="zone-tag" style="background:#374151;color:#fff">${i.Target_Price}</span>` },
    ];

    renderTable('constant-orders-container', cols, constantOrders);
    renderTable('onetime-orders-container', cols, onetimeOrders);
}


async function loadSuppliers() {
    const data = await fetchJSON('/logistics/suppliers');
    renderTable('suppliers-container', [
        { header: 'Supplier', render: i => `<strong>${i.Supplier}</strong>` },
        { header: 'Item', render: i => `<span>${i.Item}</span>` },
        { header: 'Unit Price', render: i => `<span class="font-mono">${i.Unit_Price ? formatCurrency(i.Unit_Price) : '-'}</span>` },
        { header: 'Location', render: i => `<span class="zone-tag">${i.Location}</span>` },
        { header: 'Notes', render: i => `<small style="color:var(--text-muted)">${i.Notes || ''}</small>` }
    ], data);
}

// --- Search & Modal ---

const searchInput = document.getElementById('global-search');
const resultsBox = document.getElementById('search-results');

if (searchInput) {
    // 1. Input Listener: Fetch and render
    searchInput.addEventListener('input', async (e) => {
        const query = e.target.value;
        if (query.length < 3) {
            resultsBox.style.display = 'none';
            return;
        }

        const matches = await fetchJSON(`/market/search?query=${query}`);
        if (matches && matches.length > 0) {
            resultsBox.innerHTML = matches.map((item, index) => `
                <div class="search-item" data-item="${item}" onclick="loadItemAnalysis('${item}')">
                    ${item}
                </div>
            `).join('');
            resultsBox.style.display = 'block';
        } else {
            resultsBox.style.display = 'none';
        }
    });

    // 2. Keydown Listener: Select Top Result
    searchInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
            // Check if results are visible
            if (resultsBox.style.display !== 'none' && resultsBox.firstElementChild) {
                // Trigger the click logic on the first item
                const firstItem = resultsBox.firstElementChild;
                const itemName = firstItem.getAttribute('data-item');
                if (itemName) {
                    loadItemAnalysis(itemName);
                    searchInput.blur();
                    resultsBox.style.display = 'none'; // UI Cleanup
                }
            }
        }
    });

    // 3. Close on Click Outside
    document.addEventListener('click', (e) => {
        if (!searchInput.contains(e.target) && !resultsBox.contains(e.target)) {
            resultsBox.style.display = 'none';
        }
    });
}


// Item Analysis Logic
let priceChart = null;
let demandChart = null;

async function loadItemAnalysis(itemName) {
    if (!itemName) return;

    // Switch to Analysis Tab
    switchTab('analysis');

    // Update Header
    const titleEl = document.getElementById('analysis-title');
    if (titleEl) titleEl.innerHTML = `Item Analysis: <span style="color:var(--text-accent)">${itemName}</span>`;

    // Clear Search Input
    document.getElementById('search-results').style.display = 'none';
    if (searchInput) searchInput.value = '';

    // Load History
    const history = await fetchJSON(`/market/item/${itemName}/history`);

    // --- Chart 1: Price ---
    const ctxPrice = document.getElementById('priceChart').getContext('2d');
    if (priceChart) priceChart.destroy();

    if (history && history.length > 0) {
        priceChart = new Chart(ctxPrice, {
            type: 'line',
            data: {
                labels: history.map(h => h.SnapshotDate.split(' ')[0]),
                datasets: [
                    {
                        label: 'Median Price',
                        data: history.map(h => h.Median_Price != null ? h.Median_Price : h.Avg_Price),
                        borderColor: '#fbbf24',
                        tension: 0.1,
                        borderWidth: 2
                    },
                    {
                        label: 'Min Price',
                        data: history.map(h => h.Min_Price),
                        borderColor: '#374151',
                        borderDash: [5, 5],
                        tension: 0.1
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: { y: { grid: { color: '#222' } } }
            }
        });
    }

    // --- Chart 2: Demand ---
    const ctxDemand = document.getElementById('demandChart').getContext('2d');
    if (demandChart) demandChart.destroy();

    if (history && history.length > 0) {
        demandChart = new Chart(ctxDemand, {
            type: 'bar',
            data: {
                labels: history.map(h => h.SnapshotDate.split(' ')[0]),
                datasets: [
                    {
                        label: 'Units Sold (Daily Churn)',
                        data: history.map(h => h.Units_Sold_Since_Last),
                        backgroundColor: '#22c55e',
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: { grid: { color: '#222' }, beginAtZero: true }
                }
            }
        });
    }

    // Load Producers
    const producers = await fetchJSON(`/market/item/${itemName}/producers`);
    const prodList = document.getElementById('analysis-producers-list');
    if (producers && producers.length > 0) {
        prodList.innerHTML = producers.map(p => `
            <div class="producer-tag">
                <span class="hub-icon ${p.Zone.includes('Kerys') ? 'hub-active' : ''}">📍</span>
                <strong>${p.Zone}</strong> 
                <small>(${p.Unique_Producers} Sellers)</small>
            </div>
        `).join('');
    } else {
        prodList.innerHTML = '<p style="color:var(--text-muted)">No specific producer data found.</p>';
    }
}

function loadAllData() {
    loadCrafting();
    loadLiquidity();
    loadArbitrage();
    loadOrders();
    loadSuppliers();

    // Default Item Analysis
    loadItemAnalysis('Charcoal');
}

// Init
document.addEventListener('DOMContentLoaded', () => {
    loadAllData();

    // Smooth scrolling for anchor links
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
        anchor.addEventListener('click', function (e) {
            e.preventDefault();
            const target = document.querySelector(this.getAttribute('href'));
            if (target) {
                target.scrollIntoView({
                    behavior: 'smooth'
                });
            }
        });
    });
});
