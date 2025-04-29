// Chart instances
let transactionThroughputChart = null;
let finalityChart = null;
let shardTransactionThroughputGauge = null;
let totalTransactionThroughputGauge = null;
let finalityConsensusGauge = null;
let finalityClientGauge = null;
let networkBandwidthChart = null;
let messagesThroughputChart = null;

// Theme toggle functionality
const themeToggle = document.getElementById('theme-toggle');

// Check for saved theme preference or default to dark mode
if (localStorage.getItem('theme') === 'light') {
    document.documentElement.setAttribute('data-theme', 'light');
    themeToggle.checked = false;
}
else
{
    document.documentElement.setAttribute('data-theme', 'dark');
    themeToggle.checked = true;
}	

// Theme switch handler
themeToggle.addEventListener('change', function() {
    if (this.checked) {
        document.documentElement.setAttribute('data-theme', 'dark');
        localStorage.setItem('theme', 'dark');
    } else {
        document.documentElement.setAttribute('data-theme', 'light');
        localStorage.setItem('theme', 'light');
    }
    
    // Reinitialize charts with new theme colors
    initializeCharts();
    // Force an immediate data update
    updateData();
});

function createGaugeConfig(max) {
    return {
        type: 'doughnut',
        data: {
            datasets: [{
                data: [0, max],
                backgroundColor: ['#10B981', document.documentElement.getAttribute('data-theme') === 'dark' ? '#4B5563' : '#E5E7EB'],
                borderWidth: 0
            }]
        },
        options: {
            responsive: true,
			maintainAspectRatio: false,
            circumference: 180,
            rotation: 270,
            cutout: '80%',
            plugins: {
                legend: { display: false },
                tooltip: { enabled: false }
            },
            layout: {
                padding: 0
            }
        }
    };
}

function updateGauge(chart, value, max) {
    if (!chart) return;
    
    let color = '#10B981';  // Green
    if (value > max * 0.9) {
        color = '#EF4444';  // Red
    } else if (value > max * 0.8) {
        color = '#F59E0B';  // Yellow
    }
    
    chart.data.datasets[0].data = [value, Math.max(0, max - value)];
    chart.data.datasets[0].backgroundColor = [color, document.documentElement.getAttribute('data-theme') === 'dark' ? '#4B5563' : '#E5E7EB'];
    chart.update();
}

function initializeCharts() {
    // Destroy existing charts if they exist
    if (shardTransactionThroughputGauge) shardTransactionThroughputGauge.destroy();
    if (totalTransactionThroughputGauge) totalTransactionThroughputGauge.destroy();
    if (finalityConsensusGauge) finalityConsensusGauge.destroy();
    if (finalityClientGauge) finalityClientGauge.destroy();
    if (transactionThroughputChart) transactionThroughputChart.destroy();
    if (finalityChart) finalityChart.destroy();
	if (networkBandwidthChart) networkBandwidthChart.destroy();
	if (messagesThroughputChart) messagesThroughputChart.destroy();

    shardTransactionThroughputGauge = new Chart(
        document.getElementById('shardTransactionThroughputGauge'),
        createGaugeConfig(5000)
    );

    totalTransactionThroughputGauge = new Chart(
        document.getElementById('totalTransactionThroughputGauge'),
        createGaugeConfig(10000)
    );

    finalityConsensusGauge = new Chart(
        document.getElementById('finalityConsensusGauge'),
        createGaugeConfig(12000)
    );

    finalityClientGauge = new Chart(
        document.getElementById('finalityClientGauge'),
        createGaugeConfig(12000)
    );

    const chartOptions = {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        plugins: {
            legend: {
                position: 'bottom',
                labels: {
                    usePointStyle: false,
                    boxWidth: 15,
                    color: document.documentElement.getAttribute('data-theme') === 'dark' ? '#ffffff' : '#1a1a1a'
                }
            }
        },
        scales: {
            x: {
                grid: {
                    display: false
                },
                ticks: { 
                    color: document.documentElement.getAttribute('data-theme') === 'dark' ? '#ffffff' : '#1a1a1a'
                }
            },
            y: {
                grid: {
                    display: true
                },
                ticks: { 
                    color: document.documentElement.getAttribute('data-theme') === 'dark' ? '#ffffff' : '#1a1a1a'
                },
				min: 0,
				beginAtZero: true
            }
        },
        elements: {
            line: {
                tension: 0.4,
                borderWidth: 2,
                fill: false
            },
            point: {
                radius: 0
            }
        },
        interaction: {
            intersect: false,
            mode: 'index'
        }
    };

    transactionThroughputChart = new Chart(
        document.getElementById('transactionThroughputChart'),
        {
            type: 'line',
            data: {
                labels: [],
                datasets: [
                    {
                        label: 'Transfers',
                        borderColor: '#10B981',
                        data: []
                    },
                    {
                        label: 'Swaps',
                        borderColor: '#F59E0B',
                        data: []
                    },
                    {
                        label: 'Blobs',
                        borderColor: '#3B82F6',
                        data: []
                    },
                    {
                        label: 'Other',
                        borderColor: '#9CA3AF',
                        data: []
                    }
                ]
            },
            options: chartOptions
        }
    );

    finalityChart = new Chart(
        document.getElementById('finalityChart'),
        {
            type: 'line',
            data: {
                labels: [],
                datasets: [
                    {
                        label: 'Consensus',
                        borderColor: '#F59E0B',
                        data: []
                    },
                    {
                        label: 'Client',
                        borderColor: '#10B981',
                        data: []
                    }
                ]
            },
            options: chartOptions
        }
    );
	
	networkBandwidthChart = new Chart(
		document.getElementById('networkBandwidthChart'),
		{
			type: 'line',
			data: {
				labels: [],
				datasets: [
					{
						label: 'Inbound',
						borderColor: '#3B82F6',  // Blue
						data: []
					},
					{
						label: 'Outbound',
						borderColor: '#10B981',  // Green
						data: []
					}
				]
			},
            options: chartOptions
        }
    );

	messagesThroughputChart = new Chart(
		document.getElementById('messagesThroughputChart'),
		{
			type: 'line',
			data: {
				labels: [],
				datasets: [
					{
						label: 'Inbound',
						borderColor: '#3B82F6',  // Blue
						data: []
					},
					{
						label: 'Outbound',
						borderColor: '#10B981',  // Green
						data: []
					}
				]
			},
            options: chartOptions
        }
    );
}

async function updateData() {
    try {
		// Fetch node information
		const nodeResponse = await fetch('/api/node');
		const nodeData = await nodeResponse.json();
		
		// Update node information
		document.getElementById('nodeIdentityValue').textContent = truncateStringMiddle((nodeData.node.identity || '-').replace(':i:', ''), 80);
		document.getElementById('nodeShardGroupValue').textContent = nodeData.node.shard_group;
        document.getElementById('nodeUpTimeValue').textContent = nodeData.uptime?.duration || '-';

        const ledgerStatisticsResponse = await fetch('/api/ledger/statistics');
        const ledgerStatistics = await ledgerStatisticsResponse.json();
	
		//
        const shardTransactionThroughput = ledgerStatistics.statistics?.throughput?.executions?.local?.executions < 1 ? ledgerStatistics.statistics?.throughput?.executions?.local?.executions : Math.trunc(ledgerStatistics.statistics?.throughput?.executions?.local?.executions || 0);
        const totalTransactionThroughput = ledgerStatistics.statistics?.throughput?.executions?.total?.executions < 1 ? ledgerStatistics.statistics?.throughput?.executions?.total?.executions : Math.trunc(ledgerStatistics.statistics?.throughput?.executions?.total?.executions || 0);
        const finalityConsensus = ledgerStatistics.statistics?.throughput?.finality?.consensus || 0;
        const finalityClient = ledgerStatistics.statistics?.throughput?.finality?.client || 0;

		// Update ledger statistics 
        document.getElementById('universeShardCountValue').textContent = ledgerStatistics.statistics?.shard_groups || 0;

        document.getElementById('proposalTimeValue').textContent = new Date(ledgerStatistics.head?.timestamp).toLocaleString() || '-';
        document.getElementById('proposalHeightValue').textContent = (ledgerStatistics.head?.height).toLocaleString() || '-';
        document.getElementById('proposalHashValue').textContent = (ledgerStatistics.head?.hash || '-').replace(':h:', '');
        document.getElementById('proposalSizeValue').textContent = `${((ledgerStatistics.statistics?.proposals?.size_average || 0) / 1024 / 1024).toFixed(2)} MB`;

        document.getElementById('shardTransactionThroughputValue').textContent = `${shardTransactionThroughput.toLocaleString()} TPS`;
        document.getElementById('totalTransactionThroughputValue').textContent = `${totalTransactionThroughput.toLocaleString()} TPS`;
        document.getElementById('finalityConsensusValue').textContent = `${finalityConsensus.toLocaleString()} ms`;
        document.getElementById('finalityClientValue').textContent = `${finalityClient.toLocaleString()} ms`;

        const intervals = ledgerStatistics.statistics?.proposals?.intervals || {};
        document.getElementById('phaseValue').textContent = `${intervals.phase || 0} ms`;
        document.getElementById('commitValue').textContent = `${intervals.commit || 0} ms`;
        document.getElementById('roundValue').textContent = `${intervals.round || 0} ms`;
        document.getElementById('pendingTx').textContent = (ledgerStatistics.statistics?.processed?.transactions?.pending || 0).toLocaleString();
        document.getElementById('failedTx').textContent = (ledgerStatistics.statistics?.processed?.transactions?.failed || 0).toLocaleString();
        document.getElementById('txPerShard').textContent = (ledgerStatistics.statistics?.processed?.executions?.local || 0).toLocaleString();
        document.getElementById('totalTx').textContent = (ledgerStatistics.statistics?.processed?.executions?.total || 0).toLocaleString();
        document.getElementById('csrValue').textContent = (ledgerStatistics.statistics?.throughput?.transactions?.shards_average || 1).toFixed(3);

        // Update gauges
        updateGauge(shardTransactionThroughputGauge, shardTransactionThroughput, 5000);
        updateGauge(totalTransactionThroughputGauge, totalTransactionThroughput, 5000 * ledgerStatistics.statistics.shard_groups);
        updateGauge(finalityConsensusGauge, finalityConsensus, 15000);
        updateGauge(finalityClientGauge, finalityClient, 15000);

        // Fetch timeseries data
        const executionsResponse = await fetch('/api/statistics/timeseries/executions');
        const executionsData = await executionsResponse.json();
        const executions = executionsData.executions || [];

        const finalityResponse = await fetch('/api/statistics/timeseries/finality');
        const finalityData = await finalityResponse.json();
        const finality = finalityData.finality || [];
		
		const networkBandwidthResponse = await fetch('/api/statistics/timeseries/bandwidth');
		const networkBandwidthData = await networkBandwidthResponse.json();
		const networkBandwidth = networkBandwidthData.bandwidth || [];

		const messagesThroughputResponse = await fetch('/api/statistics/timeseries/messages');
		const messagesThroughputData = await messagesThroughputResponse.json();
		const messagesThroughput = messagesThroughputData.messages || [];
		
        // Update charts
        if (transactionThroughputChart && executions.length > 0) {
            executions.sort((a, b) => a.timestamp - b.timestamp);
            
            transactionThroughputChart.data.labels = executions.map(e => 
                new Date(e.timestamp * 1000).toLocaleTimeString()
            );

            transactionThroughputChart.data.datasets[0].data = executions.map(e => e.total?.transfer || 0);
            transactionThroughputChart.data.datasets[1].data = executions.map(e => e.total?.swap || 0);
            transactionThroughputChart.data.datasets[2].data = executions.map(e => e.total?.blob || 0);
            transactionThroughputChart.data.datasets[3].data = executions.map(e => e.total?.set || 0);
            transactionThroughputChart.update();
        }

        if (finalityChart && finality.length > 0) {
            finality.sort((a, b) => a.timestamp - b.timestamp);
            
            finalityChart.data.labels = finality.map(f => 
                new Date(f.timestamp * 1000).toLocaleTimeString()
            );

            finalityChart.data.datasets[0].data = finality.map(f => f.consensus || 0);
            finalityChart.data.datasets[1].data = finality.map(f => f.client || 0);
            finalityChart.update();
        }
		
		if (networkBandwidthChart && networkBandwidth.length > 0) {
			networkBandwidth.sort((a, b) => a.timestamp - b.timestamp);
    
			networkBandwidthChart.data.labels = networkBandwidth.map(n => 
				new Date(n.timestamp * 1000).toLocaleTimeString()
			);
    
			networkBandwidthChart.data.datasets[0].data = networkBandwidth.map(n => n.inbound);
			networkBandwidthChart.data.datasets[1].data = networkBandwidth.map(n => n.outbound);
			networkBandwidthChart.update();
		}
		
		if (messagesThroughputChart && messagesThroughput.length > 0) {
			messagesThroughput.sort((a, b) => a.timestamp - b.timestamp);
    
			messagesThroughputChart.data.labels = messagesThroughput.map(n => 
				new Date(n.timestamp * 1000).toLocaleTimeString()
			);
    
			messagesThroughputChart.data.datasets[0].data = messagesThroughput.map(n => n.inbound);
			messagesThroughputChart.data.datasets[1].data = messagesThroughput.map(n => n.outbound);
			messagesThroughputChart.update();
		}
		
		// Update network stats
        const networkStatisticsResponse = await fetch('/api/network/statistics');
        const networkStatistics = await networkStatisticsResponse.json();
        document.getElementById('inboundBytesTotalValue').textContent = (networkStatistics.statistics?.bandwidth?.transferred?.inbound || 0).toLocaleString();
        document.getElementById('outboundBytesTotalValue').textContent = (networkStatistics.statistics?.bandwidth?.transferred?.outbound || 0).toLocaleString();
        document.getElementById('inboundMessagesTotalValue').textContent = (networkStatistics.statistics?.messages?.transferred?.inbound || 0).toLocaleString();
        document.getElementById('outboundMessagesTotalValue').textContent = (networkStatistics.statistics?.messages?.transferred?.outbound || 0).toLocaleString();

		 // Fetch network connections
        const connectionsResponse = await fetch('/api/network/connections');
        const connectionsData = await connectionsResponse.json();

        // Update connection tables
		updateConnectionTable('syncGroupTable', connectionsData.connections.sync, false);
		updateConnectionTable('shardGroupTable', connectionsData.connections.shardgroups, true);		
		
		// Update sync status indicator
		const syncIndicator = document.querySelector('.sync-indicator');
		if (syncIndicator) {
			if (nodeData.node.synced) {
				syncIndicator.classList.add('synced');
			} else {
				syncIndicator.classList.remove('synced');
			}
		}		
    } catch (error) {
        console.error('Error fetching data:', error);
    }
}

function formatDuration(timestamp) {
    const now = Date.now();
    const duration = now - timestamp;
    
    const seconds = Math.floor(duration / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    
    if (hours > 0) {
        return `${hours}h ${minutes % 60}m`;
    } else if (minutes > 0) {
        return `${minutes}m ${seconds % 60}s`;
    } else {
        return `${seconds}s`;
    }
}

function formatRemaining(timestamp) {
    const now = Date.now();
    const duration = timestamp - now;
    
    const seconds = Math.floor(duration / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    
    if (hours > 0) {
        return `${hours}h ${minutes % 60}m`;
    } else if (minutes > 0) {
        return `${minutes}m ${seconds % 60}s`;
    } else {
        return `${seconds}s`;
    }
}

function createDirectionIcon(direction) {
    const color = document.documentElement.getAttribute('data-theme') === 'dark' ? '#fff' : '#000';
    return `
        <span class="direction-icon">
            <svg class="${direction.toLowerCase()}-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="${color}" stroke-width="2">
                <line x1="5" y1="12" x2="19" y2="12"></line>
                <polyline points="12 5 19 12 12 19"></polyline>
            </svg>
            ${direction}
        </span>
    `;
}

function updateConnectionTable(tableId, connections, isShardTable = false) {
    const table = document.getElementById(tableId);
    if (!table) return;
    
    const tbody = table.querySelector('tbody');
    tbody.innerHTML = '';
    
    if (isShardTable) {
        // Handle shard connections
        Object.entries(connections).forEach(([shardId, shardConns]) => {
            shardConns.forEach(conn => {
                const row = document.createElement('tr');
                row.innerHTML = `
					<td class=\"tiny\">${createSyncIndicator(conn.sync)} ${shardId} ${parseInt(conn.head.slice(3, 19), 16)}</td>
                    <td class=\"medium\">${conn.host} | ${conn.strikes} </td>
                    <td class=\"small\">${createDirectionIcon(conn.direction)}</td>
					<td class=\"small\">${formatDuration(conn.connected_at)} | ${formatRemaining(conn.shuffle_at)}</td>
                    <td class=\"tiny\">${conn.latency}ms</td>
					<td class=\"small\">${(conn.statistics.throughput.egress || 0).toLocaleString()} : ${(conn.statistics.throughput.ingress || 0).toLocaleString()}</td>
					<td class=\"small\">${(conn.statistics.transferred.egress || 0).toLocaleString()} : ${(conn.statistics.transferred.ingress || 0).toLocaleString()}</td>
					<td class=\"small\">${(conn.statistics.requests.total || 0).toLocaleString()} : ${(conn.statistics.requested.total || 0).toLocaleString()} | ${(conn.statistics.requests.recent || 0).toLocaleString()} : ${(conn.statistics.requested.recent || 0).toLocaleString()} ${(conn.statistics.timeout || 5000)}ms</td>
                    <td></td>
                `;
                tbody.appendChild(row);
            });
        });
    } else {
        // Handle sync connections
        const connectionsArray = Array.isArray(connections) ? connections : Object.values(connections);
        connectionsArray.forEach(conn => {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td class=\"tiny\">${createSyncIndicator(conn.sync)} ${parseInt(conn.head.slice(3, 19), 16)}</td>
                <td class=\"medium\">${conn.host} | ${conn.strikes} </td>
                <td class=\"small\">${createDirectionIcon(conn.direction)}</td>
                <td class=\"small\">${formatDuration(conn.connected_at)} | ${formatRemaining(conn.shuffle_at)}</td>
                <td class=\"tiny\">${conn.latency}ms</td>
                <td class=\"small\">${(conn.statistics.throughput.egress || 0).toLocaleString()} : ${(conn.statistics.throughput.ingress || 0).toLocaleString()}</td>
                <td class=\"small\">${(conn.statistics.transferred.egress || 0).toLocaleString()} : ${(conn.statistics.transferred.ingress || 0).toLocaleString()}</td>
                <td class=\"small\">${(conn.statistics.requests.total || 0).toLocaleString()} : ${(conn.statistics.requested.total || 0).toLocaleString()} | ${(conn.statistics.requests.recent || 0).toLocaleString()} : ${(conn.statistics.requested.recent || 0).toLocaleString()} ${(conn.statistics.timeout || 5000)}ms</td>
            `;
            tbody.appendChild(row);
        });
    }
}

// Function to create a visual sync indicator using the existing sync-indicator class
function createSyncIndicator(isSynced) 
{
    let className = "connection-sync-indicator";
    if (isSynced === true) 
        className += " synced"; // Add synced class for green color (defined in your CSS)
    
    return `<div class="${className}"></div>`;
}

function truncateStringMiddle(str, maxLength) 
{
  if (str.length <= maxLength) 
	  return str;
  
  const ellipsis = '...';
  const charsToShow = maxLength - ellipsis.length;
  const frontChars = Math.ceil(charsToShow/2);
  const backChars = Math.floor(charsToShow/2);
  
  return str.slice(0, frontChars) + ellipsis + str.slice(str.length - backChars);
}

// Initialize and start updates
initializeCharts();
updateData();
setInterval(updateData, 3000);