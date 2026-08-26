/**
 * DINIS — Enterprise-Scale ICMP Network Monitor Web Application
 * Handles 20,000+ monitored endpoints with Subnet Heatmaps, Exception Queues,
 * Time-Series Rollups, and Virtualized Rendering.
 */

(function() {
  'use strict';

  // Application State
  const state = {
    currentView: 'matrix', // 'matrix', 'outliers', 'explorer'
    hosts: new Map(),      // IP -> Host object (current page / cache)
    subnetsMatrix: [],     // SubnetMatrixBlock[]
    outliers: [],          // OutlierHost[]
    summary: null,
    discoveryStatus: {
      isScanning: false,
      lastRun: null,
      nextRun: null,
      lastDiscoveredCount: 0,
      subnetCapacity: 0,
      intervalMin: 240
    },
    cidrs: [],
    exclusions: [],
    activeAlerts: [],
    alertHistory: [],
    settings: {
      intervalSec: 60.0,
      timeoutMs: 1000,
      failThreshold: 2,
      concurrency: 100,
      discoveryIntervalMin: 240,
      autoDiscovery: true
    },
    // Explorer pagination & filters
    filter: 'all',
    search: '',
    sort: 'status',
    viewMode: 'grid',
    currentPage: 1,
    pageSize: 50,
    totalPages: 1,
    totalHostsCount: 0,

    // Detail modal & historical charts
    selectedHostIP: null,
    selectedHostData: null,
    chartWindow: 'realtime', // 'realtime', '1h', '24h', '168h'
    historyPoints: [],

    sseConnected: false,

    // Throttled render flag
    renderPending: false
  };

  // DOM Elements
  const el = {
    // Header & KPIs
    liveStatusBadge: document.getElementById('liveStatusBadge'),
    liveStatusText: document.getElementById('liveStatusText'),
    btnRunDiscovery: document.getElementById('btnRunDiscovery'),
    discRadarIcon: document.getElementById('discRadarIcon'),
    btnDiscoveryText: document.getElementById('btnDiscoveryText'),
    kpiTotal: document.getElementById('kpiTotal'),
    kpiCidrCount: document.getElementById('kpiCidrCount'),
    kpiUp: document.getElementById('kpiUp'),
    kpiHealthRate: document.getElementById('kpiHealthRate'),
    kpiDown: document.getElementById('kpiDown'),
    kpiUnackCount: document.getElementById('kpiUnackCount'),
    kpiAlerts: document.getElementById('kpiAlerts'),
    kpiAvgLatency: document.getElementById('kpiAvgLatency'),
    kpiPacingContainer: document.getElementById('kpiPacingContainer'),
    kpiPacingRate: document.getElementById('kpiPacingRate'),
    kpiExcluded: document.getElementById('kpiExcluded'),
    kpiExclRuleCount: document.getElementById('kpiExclRuleCount'),
    cardKpiDown: document.getElementById('cardKpiDown'),
    cardKpiAlerts: document.getElementById('cardKpiAlerts'),
    btnQuickAckAll: document.getElementById('btnQuickAckAll'),
    navAlertBadge: document.getElementById('navAlertBadge'),
    unackAlertBanner: document.getElementById('unackAlertBanner'),
    bannerAlertTitle: document.getElementById('bannerAlertTitle'),
    bannerAlertDesc: document.getElementById('bannerAlertDesc'),
    btnBannerViewAlerts: document.getElementById('btnBannerViewAlerts'),
    btnBannerAckAll: document.getElementById('btnBannerAckAll'),

    // Master Navigation Tabs
    tabViewMatrix: document.getElementById('tabViewMatrix'),
    tabViewOutliers: document.getElementById('tabViewOutliers'),
    tabViewExplorer: document.getElementById('tabViewExplorer'),
    outliersCountBadge: document.getElementById('outliersCountBadge'),
    viewMatrixSection: document.getElementById('viewMatrixSection'),
    viewOutliersSection: document.getElementById('viewOutliersSection'),
    viewExplorerSection: document.getElementById('viewExplorerSection'),

    // View 1: Subnet Matrix
    matrixGridContainer: document.getElementById('matrixGridContainer'),
    matrixSubnetCount: document.getElementById('matrixSubnetCount'),

    // View 2: Outliers Board
    outliersTableBody: document.getElementById('outliersTableBody'),
    btnRefreshOutliers: document.getElementById('btnRefreshOutliers'),

    // View 3: Explorer Toolbar & Controls
    hostSearchInput: document.getElementById('hostSearchInput'),
    btnClearSearch: document.getElementById('btnClearSearch'),
    filterChips: document.querySelectorAll('.filter-chips .chip'),
    countAll: document.getElementById('countAll'),
    countDown: document.getElementById('countDown'),
    countAck: document.getElementById('countAck'),
    countUp: document.getElementById('countUp'),
    countExcluded: document.getElementById('countExcluded'),
    sortSelect: document.getElementById('sortSelect'),
    btnViewGrid: document.getElementById('btnViewGrid'),
    btnViewTable: document.getElementById('btnViewTable'),
    hostsGrid: document.getElementById('hostsGrid'),
    hostsTableWrapper: document.getElementById('hostsTableWrapper'),
    hostsTableBody: document.getElementById('hostsTableBody'),
    emptyState: document.getElementById('emptyState'),

    // Pagination
    paginationBar: document.getElementById('paginationBar'),
    pageRangeStart: document.getElementById('pageRangeStart'),
    pageRangeEnd: document.getElementById('pageRangeEnd'),
    pageTotalCount: document.getElementById('pageTotalCount'),
    currentPageNum: document.getElementById('currentPageNum'),
    totalPageCount: document.getElementById('totalPageCount'),
    btnPrevPage: document.getElementById('btnPrevPage'),
    btnNextPage: document.getElementById('btnNextPage'),

    // Header buttons
    btnOpenAlerts: document.getElementById('btnOpenAlerts'),
    btnOpenCIDR: document.getElementById('btnOpenCIDR'),
    btnOpenExclusions: document.getElementById('btnOpenExclusions'),
    btnOpenSettings: document.getElementById('btnOpenSettings'),

    // Alerts Drawer
    alertsDrawer: document.getElementById('alertsDrawer'),
    btnCloseAlertsDrawer: document.getElementById('btnCloseAlertsDrawer'),
    drawerAlertCount: document.getElementById('drawerAlertCount'),
    alertsSummaryCount: document.getElementById('alertsSummaryCount'),
    btnDrawerAckAll: document.getElementById('btnDrawerAckAll'),
    activeAlertsList: document.getElementById('activeAlertsList'),
    alertHistoryList: document.getElementById('alertHistoryList'),
    drawerTabs: document.querySelectorAll('.drawer-tab'),
    tabActiveAlerts: document.getElementById('tabActiveAlerts'),
    tabAlertHistory: document.getElementById('tabAlertHistory'),

    // CIDR Modal
    cidrModal: document.getElementById('cidrModal'),
    btnCloseCidrModal: document.getElementById('btnCloseCidrModal'),
    btnCidrModalDiscoverAll: document.getElementById('btnCidrModalDiscoverAll'),
    formAddCIDR: document.getElementById('formAddCIDR'),
    inputCIDR: document.getElementById('inputCIDR'),
    inputCIDRDesc: document.getElementById('inputCIDRDesc'),
    checkIncludeNetBcast: document.getElementById('checkIncludeNetBcast'),
    cidrFeedback: document.getElementById('cidrFeedback'),
    cidrTableBody: document.getElementById('cidrTableBody'),

    // Exclusions Modal
    exclusionsModal: document.getElementById('exclusionsModal'),
    btnCloseExclusionsModal: document.getElementById('btnCloseExclusionsModal'),
    formAddExclusion: document.getElementById('formAddExclusion'),
    inputExclRule: document.getElementById('inputExclRule'),
    inputExclReason: document.getElementById('inputExclReason'),
    exclusionTableBody: document.getElementById('exclusionTableBody'),

    // Acknowledge Modal
    ackModal: document.getElementById('ackModal'),
    btnCloseAckModal: document.getElementById('btnCloseAckModal'),
    btnCancelAck: document.getElementById('btnCancelAck'),
    formAcknowledgeAlert: document.getElementById('formAcknowledgeAlert'),
    ackTargetIP: document.getElementById('ackTargetIP'),
    ackTargetID: document.getElementById('ackTargetID'),
    ackModalTargetInfo: document.getElementById('ackModalTargetInfo'),
    inputAckBy: document.getElementById('inputAckBy'),
    inputAckNote: document.getElementById('inputAckNote'),

    // Host Detail Modal
    hostDetailModal: document.getElementById('hostDetailModal'),
    btnCloseHostDetailModal: document.getElementById('btnCloseHostDetailModal'),
    detailStatusDot: document.getElementById('detailStatusDot'),
    detailHostIP: document.getElementById('detailHostIP'),
    detailHostAlias: document.getElementById('detailHostAlias'),
    detailChartTitle: document.getElementById('detailChartTitle'),
    detailCurrentRTT: document.getElementById('detailCurrentRTT'),
    detailSparklineCanvas: document.getElementById('detailSparklineCanvas'),
    detailWindowTabs: document.getElementById('detailWindowTabs'),
    detailMinRTT: document.getElementById('detailMinRTT'),
    detailAvgRTT: document.getElementById('detailAvgRTT'),
    detailP95RTT: document.getElementById('detailP95RTT'),
    detailMaxRTT: document.getElementById('detailMaxRTT'),
    detailLoss: document.getElementById('detailLoss'),
    detailJitter: document.getElementById('detailJitter'),
    detailPackets: document.getElementById('detailPackets'),
    detailLastSeen: document.getElementById('detailLastSeen'),
    detailAlertSection: document.getElementById('detailAlertSection'),
    detailAlertTitle: document.getElementById('detailAlertTitle'),
    detailAlertBadge: document.getElementById('detailAlertBadge'),
    detailAlertMsg: document.getElementById('detailAlertMsg'),
    detailAckInfo: document.getElementById('detailAckInfo'),
    btnDetailAck: document.getElementById('btnDetailAck'),
    btnDetailPingNow: document.getElementById('btnDetailPingNow'),
    btnDetailToggleExclude: document.getElementById('btnDetailToggleExclude'),
    btnDetailUnenroll: document.getElementById('btnDetailUnenroll'),
    detailExcludeText: document.getElementById('detailExcludeText'),
    formHostMeta: document.getElementById('formHostMeta'),
    inputHostAlias: document.getElementById('inputHostAlias'),
    inputHostNotes: document.getElementById('inputHostNotes'),

    // Settings Modal
    settingsModal: document.getElementById('settingsModal'),
    btnCloseSettingsModal: document.getElementById('btnCloseSettingsModal'),
    btnCancelSettings: document.getElementById('btnCancelSettings'),
    formSettings: document.getElementById('formSettings'),
    inputDiscoveryInterval: document.getElementById('inputDiscoveryInterval'),
    inputInterval: document.getElementById('inputInterval'),
    inputTimeout: document.getElementById('inputTimeout'),
    inputFailThreshold: document.getElementById('inputFailThreshold'),
    inputConcurrency: document.getElementById('inputConcurrency'),

    // Toast Container
    toastContainer: document.getElementById('toastContainer')
  };

  // Create single floating tooltip for the Subnet Matrix
  const matrixTooltip = document.createElement('div');
  matrixTooltip.className = 'matrix-floating-tooltip';
  matrixTooltip.style.display = 'none';
  document.body.appendChild(matrixTooltip);

  // Initialize Application
  async function init() {
    setupEventListeners();
    await Promise.all([
      fetchSettings(),
      fetchDiscoveryStatus(),
      fetchCIDRs(),
      fetchExclusions(),
      fetchSummary(),
      fetchSubnetsMatrix(),
      fetchOutliers(),
      fetchHosts(1),
      fetchAlerts()
    ]);
    connectSSE();
    renderAll();

    // Periodic auto-sync (every 5 seconds)
    setInterval(() => {
      fetchSummary();
      if (state.currentView === 'matrix') {
        fetchSubnetsMatrix();
      } else if (state.currentView === 'outliers') {
        fetchOutliers();
      }
    }, 5000);
  }

  // Master View Navigation
  function switchView(viewName) {
    state.currentView = viewName;

    // Update Tab Styles
    el.tabViewMatrix.classList.toggle('active', viewName === 'matrix');
    el.tabViewOutliers.classList.toggle('active', viewName === 'outliers');
    el.tabViewExplorer.classList.toggle('active', viewName === 'explorer');

    // Toggle Section Visibility
    el.viewMatrixSection.style.display = (viewName === 'matrix') ? 'block' : 'none';
    el.viewOutliersSection.style.display = (viewName === 'outliers') ? 'block' : 'none';
    el.viewExplorerSection.style.display = (viewName === 'explorer') ? 'block' : 'none';

    // Trigger on-demand data loads
    if (viewName === 'matrix') {
      fetchSubnetsMatrix();
    } else if (viewName === 'outliers') {
      fetchOutliers();
    } else if (viewName === 'explorer') {
      fetchHosts(state.currentPage);
    }
  }

  // Event Listeners
  function setupEventListeners() {
    // Master view navigation
    el.tabViewMatrix.addEventListener('click', () => switchView('matrix'));
    el.tabViewOutliers.addEventListener('click', () => switchView('outliers'));
    el.tabViewExplorer.addEventListener('click', () => switchView('explorer'));

    // Outliers refresh
    if (el.btnRefreshOutliers) {
      el.btnRefreshOutliers.addEventListener('click', () => fetchOutliers());
    }

    // Explorer Pagination
    el.btnPrevPage.addEventListener('click', () => {
      if (state.currentPage > 1) {
        fetchHosts(state.currentPage - 1);
      }
    });
    el.btnNextPage.addEventListener('click', () => {
      if (state.currentPage < state.totalPages) {
        fetchHosts(state.currentPage + 1);
      }
    });

    // Explorer Search with 200ms debounce
    let searchDebounceTimer = null;
    el.hostSearchInput.addEventListener('input', (e) => {
      state.search = e.target.value.trim().toLowerCase();
      el.btnClearSearch.style.display = state.search ? 'block' : 'none';
      clearTimeout(searchDebounceTimer);
      searchDebounceTimer = setTimeout(() => {
        state.currentPage = 1;
        fetchHosts(1);
      }, 200);
    });

    el.btnClearSearch.addEventListener('click', () => {
      el.hostSearchInput.value = '';
      state.search = '';
      el.btnClearSearch.style.display = 'none';
      state.currentPage = 1;
      fetchHosts(1);
    });

    // Filter Chips
    el.filterChips.forEach(chip => {
      chip.addEventListener('click', () => {
        el.filterChips.forEach(c => c.classList.remove('active'));
        chip.classList.add('active');
        state.filter = chip.dataset.filter;
        state.currentPage = 1;
        fetchHosts(1);
      });
    });

    // Sort Dropdown
    el.sortSelect.addEventListener('change', (e) => {
      state.sort = e.target.value;
      state.currentPage = 1;
      fetchHosts(1);
    });

    // View Grid / Table Toggle
    el.btnViewGrid.addEventListener('click', () => {
      state.viewMode = 'grid';
      el.btnViewGrid.classList.add('active');
      el.btnViewTable.classList.remove('active');
      renderHosts();
    });

    el.btnViewTable.addEventListener('click', () => {
      state.viewMode = 'table';
      el.btnViewTable.classList.add('active');
      el.btnViewGrid.classList.remove('active');
      renderHosts();
    });

    // Discovery Button
    el.btnRunDiscovery.addEventListener('click', () => triggerDiscovery());

    // Alerts Drawer
    el.btnOpenAlerts.addEventListener('click', openAlertsDrawer);
    el.btnCloseAlertsDrawer.addEventListener('click', closeAlertsDrawer);
    if (el.btnBannerViewAlerts) el.btnBannerViewAlerts.addEventListener('click', openAlertsDrawer);

    el.drawerTabs.forEach(tab => {
      tab.addEventListener('click', () => {
        el.drawerTabs.forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
        const tabName = tab.dataset.tab;
        if (tabName === 'activeAlerts') {
          if (el.tabActiveAlerts) el.tabActiveAlerts.style.display = 'block';
          if (el.tabAlertHistory) el.tabAlertHistory.style.display = 'none';
          fetchAlerts();
        } else {
          if (el.tabActiveAlerts) el.tabActiveAlerts.style.display = 'none';
          if (el.tabAlertHistory) el.tabAlertHistory.style.display = 'block';
          fetchAlertHistory();
        }
      });
    });

    // Quick Acknowledge All
    if (el.btnQuickAckAll) el.btnQuickAckAll.addEventListener('click', handleAcknowledgeAll);
    if (el.btnBannerAckAll) el.btnBannerAckAll.addEventListener('click', handleAcknowledgeAll);
    if (el.btnDrawerAckAll) el.btnDrawerAckAll.addEventListener('click', handleAcknowledgeAll);

    // CIDR Modal
    el.btnOpenCIDR.addEventListener('click', openCIDRModal);
    el.btnCloseCidrModal.addEventListener('click', closeCIDRModal);
    el.formAddCIDR.addEventListener('submit', handleAddCIDR);
    el.btnCidrModalDiscoverAll.addEventListener('click', () => {
      closeCIDRModal();
      triggerDiscovery();
    });

    // Exclusions Modal
    el.btnOpenExclusions.addEventListener('click', openExclusionsModal);
    el.btnCloseExclusionsModal.addEventListener('click', closeExclusionsModal);
    el.formAddExclusion.addEventListener('submit', handleAddExclusion);

    // Settings Modal
    el.btnOpenSettings.addEventListener('click', openSettingsModal);
    el.btnCloseSettingsModal.addEventListener('click', closeSettingsModal);
    el.btnCancelSettings.addEventListener('click', closeSettingsModal);
    el.formSettings.addEventListener('submit', handleSaveSettings);

    // Host Detail Modal
    el.btnCloseHostDetailModal.addEventListener('click', closeHostDetailModal);
    el.formHostMeta.addEventListener('submit', handleSaveHostMeta);
    el.btnDetailPingNow.addEventListener('click', handleDetailManualPing);
    el.btnDetailToggleExclude.addEventListener('click', handleDetailToggleExclude);
    el.btnDetailUnenroll.addEventListener('click', handleDetailUnenroll);
    el.btnDetailAck.addEventListener('click', () => {
      if (state.selectedHostIP) {
        const alt = state.activeAlerts.find(a => a.ip === state.selectedHostIP);
        openAcknowledgeModal(state.selectedHostIP, alt ? alt.id : '');
      }
    });

    // Historical Time Window Selector Tabs
    if (el.detailWindowTabs) {
      el.detailWindowTabs.querySelectorAll('.chart-tab').forEach(tab => {
        tab.addEventListener('click', () => {
          el.detailWindowTabs.querySelectorAll('.chart-tab').forEach(t => t.classList.remove('active'));
          tab.classList.add('active');
          state.chartWindow = tab.dataset.window;
          loadHostHistory(state.selectedHostIP, state.chartWindow);
        });
      });
    }

    // Acknowledge Modal
    el.btnCloseAckModal.addEventListener('click', closeAckModal);
    el.btnCancelAck.addEventListener('click', closeAckModal);
    el.formAcknowledgeAlert.addEventListener('submit', handleConfirmAcknowledge);
  }

  // SSE Stream Connection
  function connectSSE() {
    const sse = new EventSource('/api/stream');

    sse.onopen = () => {
      state.sseConnected = true;
      el.liveStatusBadge.classList.remove('disconnected');
      el.liveStatusText.textContent = 'LIVE PROBING';
    };

    sse.onerror = () => {
      state.sseConnected = false;
      el.liveStatusBadge.classList.add('disconnected');
      el.liveStatusText.textContent = 'RECONNECTING...';
    };

    sse.addEventListener('summary_update', (e) => {
      try {
        state.summary = JSON.parse(e.data);
        renderKPIs();
      } catch (err) {
        console.error('Failed to parse summary_update:', err);
      }
    });

    sse.addEventListener('host_state_change', (e) => {
      try {
        const payload = JSON.parse(e.data);
        const host = payload.host;

        // Update local map if present
        if (state.hosts.has(host.ip)) {
          state.hosts.set(host.ip, host);
        }

        scheduleRender();
        fetchAlerts();
        fetchSummary();

        if (state.currentView === 'matrix') {
          fetchSubnetsMatrix();
        } else if (state.currentView === 'outliers') {
          fetchOutliers();
        }

        if (payload.newStatus === 'DOWN') {
          showToast(`Host ${host.ip} (${host.alias || host.cidr}) is DOWN!`, 'error');
        } else if (payload.oldStatus === 'DOWN' && payload.newStatus === 'UP') {
          showToast(`Host ${host.ip} has recovered (UP).`, 'success');
        }
      } catch (err) {
        console.error('Failed to parse host_state_change:', err);
      }
    });

    sse.addEventListener('discovery_started', () => {
      state.discoveryStatus.isScanning = true;
      updateDiscoveryUI();
      showToast('Subnet discovery scan started...', 'info');
    });

    sse.addEventListener('discovery_completed', (e) => {
      try {
        const payload = JSON.parse(e.data);
        state.discoveryStatus = payload.status;
        updateDiscoveryUI();
        showToast(`Discovery finished: ${payload.discoveredOnline} active hosts found.`, 'success');
        fetchSubnetsMatrix();
        fetchHosts(state.currentPage);
        fetchCIDRs();
      } catch (err) {
        console.error('Failed to parse discovery_completed:', err);
      }
    });

    sse.addEventListener('alert_fired', () => {
      fetchAlerts();
      fetchOutliers();
      fetchSummary();
    });

    sse.addEventListener('alert_acknowledged', () => {
      fetchAlerts();
      fetchOutliers();
      fetchSummary();
    });

    sse.addEventListener('alert_resolved', () => {
      fetchAlerts();
      fetchAlertHistory();
      fetchOutliers();
      fetchSummary();
    });
  }

  // Throttled RAF render trigger
  function scheduleRender() {
    if (state.renderPending) return;
    state.renderPending = true;
    requestAnimationFrame(() => {
      state.renderPending = false;
      renderKPIs();
      if (state.currentView === 'explorer') {
        renderHosts();
      }
    });
  }

  // REST API Calls
  async function fetchSummary() {
    try {
      const res = await fetch('/api/summary');
      if (res.ok) {
        state.summary = await res.json();
        renderKPIs();
      }
    } catch (e) {
      console.error('Failed to fetch summary:', e);
    }
  }

  async function fetchSettings() {
    try {
      const res = await fetch('/api/settings');
      if (res.ok) {
        state.settings = await res.json();
      }
    } catch (e) {
      console.error('Failed to fetch settings:', e);
    }
  }

  async function fetchDiscoveryStatus() {
    try {
      const res = await fetch('/api/discovery/status');
      if (res.ok) {
        state.discoveryStatus = await res.json();
        updateDiscoveryUI();
      }
    } catch (e) {
      console.error('Failed to fetch discovery status:', e);
    }
  }

  async function fetchCIDRs() {
    try {
      const res = await fetch('/api/cidrs');
      if (res.ok) {
        state.cidrs = await res.json();
        renderCIDRTable();
      }
    } catch (e) {
      console.error('Failed to fetch CIDRs:', e);
    }
  }

  async function fetchExclusions() {
    try {
      const res = await fetch('/api/exclusions');
      if (res.ok) {
        state.exclusions = await res.json();
        renderExclusionTable();
      }
    } catch (e) {
      console.error('Failed to fetch exclusions:', e);
    }
  }

  // Fetch Subnet Matrix Heatmap
  async function fetchSubnetsMatrix() {
    try {
      const res = await fetch('/api/subnets/matrix');
      if (res.ok) {
        state.subnetsMatrix = await res.json();
        renderSubnetMatrix();
      }
    } catch (e) {
      console.error('Failed to fetch subnets matrix:', e);
    }
  }

  // Fetch Outliers
  async function fetchOutliers() {
    try {
      const res = await fetch('/api/outliers?limit=50');
      if (res.ok) {
        state.outliers = await res.json();
        renderOutliers();
      }
    } catch (e) {
      console.error('Failed to fetch outliers:', e);
    }
  }

  // Fetch Paginated Hosts (Explorer)
  async function fetchHosts(page = 1) {
    try {
      const params = new URLSearchParams({
        page: page,
        limit: state.pageSize,
        status: state.filter,
        search: state.search,
        sort: state.sort,
        lightweight: 'true'
      });

      const res = await fetch(`/api/hosts?${params.toString()}`);
      if (res.ok) {
        const data = await res.json();
        if (data.hosts !== undefined) {
          state.currentPage = data.page;
          state.totalPages = data.totalPages || 1;
          state.totalHostsCount = data.total || 0;

          state.hosts.clear();
          for (const h of data.hosts) {
            state.hosts.set(h.ip, h);
          }
        } else if (Array.isArray(data)) {
          // Fallback if legacy array
          state.hosts.clear();
          for (const h of data) {
            state.hosts.set(h.ip, h);
          }
          state.totalHostsCount = data.length;
          state.totalPages = 1;
          state.currentPage = 1;
        }

        renderHosts();
        updatePaginationUI();
      }
    } catch (e) {
      console.error('Failed to fetch hosts:', e);
    }
  }

  async function fetchAlerts() {
    try {
      const res = await fetch('/api/alerts');
      if (res.ok) {
        state.activeAlerts = await res.json();
        renderAlertsDrawer();
        renderKPIs();
      }
    } catch (e) {
      console.error('Failed to fetch active alerts:', e);
    }
  }

  async function fetchAlertHistory() {
    try {
      const res = await fetch('/api/alerts/history');
      if (res.ok) {
        state.alertHistory = await res.json();
        renderAlertHistory();
      }
    } catch (e) {
      console.error('Failed to fetch alert history:', e);
    }
  }

  // -------------------------------------------------------------
  // RENDERING FUNCTIONS
  // -------------------------------------------------------------

  function renderAll() {
    renderKPIs();
    renderSubnetMatrix();
    renderOutliers();
    renderHosts();
    renderCIDRTable();
    renderExclusionTable();
  }

  function renderKPIs() {
    const s = state.summary;
    if (!s) return;

    el.kpiTotal.textContent = s.totalTargets.toLocaleString();
    el.kpiCidrCount.textContent = `${state.cidrs.length} Subnets (${s.subnetCapacity.toLocaleString()} Capacity)`;

    el.kpiUp.textContent = s.upCount.toLocaleString();
    const activeTotal = s.upCount + s.downCount;
    const healthRate = activeTotal > 0 ? ((s.upCount / activeTotal) * 100).toFixed(1) : '100.0';
    el.kpiHealthRate.textContent = `${healthRate}% Reachability`;

    el.kpiDown.textContent = s.downCount.toLocaleString();
    el.kpiUnackCount.textContent = `${s.alertsUnack} Unacknowledged`;

    el.kpiAlerts.textContent = s.alertsActive.toLocaleString();
    el.kpiAvgLatency.textContent = s.avgLatencyMs > 0 ? `${s.avgLatencyMs.toFixed(2)} ms` : '--';

    if (s.packetsPerSec > 0) {
      el.kpiPacingRate.textContent = `${s.packetsPerSec} pkts/sec (${s.pacedDelayMs}ms pace)`;
    } else {
      el.kpiPacingRate.textContent = 'Paced across interval';
    }

    el.kpiExcluded.textContent = s.excludedCount.toLocaleString();
    el.kpiExclRuleCount.textContent = `${state.exclusions.length} Rules Applied`;

    // Filter Chip Badge Counts
    if (el.countAll) el.countAll.textContent = (s.totalTargets || 0).toLocaleString();
    if (el.countDown) el.countDown.textContent = (s.downCount || 0).toLocaleString();
    if (el.countAck) el.countAck.textContent = (s.ackCount ?? (s.alertsActive - s.alertsUnack) ?? 0).toLocaleString();
    if (el.countUp) el.countUp.textContent = (s.upCount || 0).toLocaleString();
    if (el.countExcluded) el.countExcluded.textContent = (s.excludedCount || 0).toLocaleString();

    // Alert Banner & Badges
    if (s.alertsUnack > 0) {
      el.unackAlertBanner.style.display = 'flex';
      el.bannerAlertTitle.textContent = `${s.alertsUnack} Unacknowledged Host Outage${s.alertsUnack === 1 ? '' : 's'} Detected!`;
      el.btnQuickAckAll.style.display = 'inline-block';
      el.navAlertBadge.style.display = 'inline-block';
      el.navAlertBadge.textContent = s.alertsUnack;
    } else {
      el.unackAlertBanner.style.display = 'none';
      el.btnQuickAckAll.style.display = 'none';
      el.navAlertBadge.style.display = 'none';
    }

    // Outlier Tab Badge
    if (state.outliers.length > 0) {
      el.outliersCountBadge.style.display = 'inline-block';
      el.outliersCountBadge.textContent = state.outliers.length;
    } else {
      el.outliersCountBadge.style.display = 'none';
    }
  }

  // -------------------------------------------------------------
  // VIEW 1: Subnet Heatmap Matrix (/24 256-Grid)
  // -------------------------------------------------------------

  function renderSubnetMatrix() {
    el.matrixGridContainer.innerHTML = '';
    const blocks = state.subnetsMatrix || [];

    el.matrixSubnetCount.textContent = `${blocks.length} Subnet${blocks.length === 1 ? '' : 's'} Monitored`;

    if (blocks.length === 0) {
      el.matrixGridContainer.innerHTML = `
        <div class="empty-state" style="grid-column: 1 / -1;">
          <div class="empty-icon">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path stroke-linecap="round" stroke-linejoin="round" d="M12 4v16m8-8H4" />
            </svg>
          </div>
          <h3>No subnets configured yet</h3>
          <p>Add a CIDR block to begin high-density subnet matrix monitoring.</p>
        </div>
      `;
      return;
    }

    blocks.forEach(block => {
      const card = createSubnetMatrixCard(block);
      el.matrixGridContainer.appendChild(card);
    });
  }

  function getCellClass(cellData) {
    const status = cellData.status ?? cellData.Status ?? 'PENDING';
    const latencyMs = cellData.latencyMs ?? cellData.LatencyMs ?? 0;
    const alertAck = cellData.alertAck ?? cellData.AlertAck ?? false;

    if (status === 'DOWN') {
      return alertAck ? 'cell-ack' : 'cell-down';
    } else if (status === 'EXCLUDED') {
      return 'cell-excl';
    } else if (status === 'UP') {
      if (latencyMs < 5.0) return 'cell-fast';
      else if (latencyMs <= 25.0) return 'cell-normal';
      else if (latencyMs <= 100.0) return 'cell-slow';
      return 'cell-degraded';
    }
    return 'cell-normal';
  }

  function createSubnetMatrixCard(block) {
    const card = document.createElement('div');
    const offlineCount = block.offlineCount ?? block.OfflineCount ?? 0;
    const onlineCount = block.onlineCount ?? block.OnlineCount ?? 0;
    const healthPct = block.healthPct ?? block.HealthPct ?? 100.0;
    const avgLatencyMs = block.avgLatencyMs ?? block.AvgLatencyMs ?? 0;
    const cidr = block.cidr ?? block.CIDR ?? 'Subnet';
    const cells = block.cells ?? block.Cells ?? [];

    const hasOutage = offlineCount > 0;
    card.className = `subnet-matrix-card ${hasOutage ? 'has-outage' : ''}`;

    let healthClass = 'good';
    if (healthPct < 90) healthClass = 'bad';
    else if (healthPct < 98) healthClass = 'warn';

    card.innerHTML = `
      <div class="matrix-card-header">
        <div class="matrix-card-title">
          <span class="matrix-cidr-label">${escapeHtml(cidr)}</span>
          <span class="matrix-cidr-stats">${onlineCount} UP · ${offlineCount} DOWN · ${avgLatencyMs ? avgLatencyMs.toFixed(1) + 'ms' : '--'} avg (${cells.length} discovered)</span>
        </div>
        <span class="matrix-health-pill ${healthClass}">${healthPct.toFixed(1)}% Health</span>
      </div>
      <div class="matrix-cells-grid"></div>
    `;

    const grid = card.querySelector('.matrix-cells-grid');

    if (cells.length === 1) {
      const cellData = cells[0];
      const status = cellData.status ?? cellData.Status ?? 'PENDING';
      const latencyMs = cellData.latencyMs ?? cellData.LatencyMs ?? 0;
      const rttText = status === 'UP' ? `${latencyMs.toFixed(2)} ms` : status;
      const aliasText = cellData.alias ? `· ${escapeHtml(cellData.alias)}` : '';
      const ip = cellData.ip ?? cellData.IP ?? '';

      grid.className = 'matrix-single-host-view';
      grid.innerHTML = `
        <div class="matrix-cell ${getCellClass(cellData)}" style="width: 20px; height: 20px; flex-shrink: 0;"></div>
        <div class="d-flex align-center justify-between flex-1 font-mono text-xs">
          <strong>${escapeHtml(ip)} <span class="text-muted font-normal">${aliasText}</span></strong>
          <span>${rttText}</span>
        </div>
      `;
      grid.addEventListener('mouseenter', (e) => showMatrixTooltip(e, cellData));
      grid.addEventListener('mouseleave', hideMatrixTooltip);
      grid.addEventListener('click', () => openHostDetailModal(ip));
      return card;
    }

    // Render all discovered cells
    cells.forEach(cellData => {
      const ip = cellData.ip ?? cellData.IP ?? '';
      const cellEl = document.createElement('div');
      cellEl.className = `matrix-cell ${getCellClass(cellData)}`;

      cellEl.addEventListener('mouseenter', (e) => showMatrixTooltip(e, cellData));
      cellEl.addEventListener('mouseleave', hideMatrixTooltip);
      cellEl.addEventListener('click', () => openHostDetailModal(ip));

      grid.appendChild(cellEl);
    });

    return card;
  }

  function showMatrixTooltip(e, cell) {
    const rect = e.target.getBoundingClientRect();
    const ip = cell.ip ?? cell.IP ?? '';
    const alias = cell.alias ?? cell.Alias ?? '';
    const status = cell.status ?? cell.Status ?? 'PENDING';
    const latencyMs = cell.latencyMs ?? cell.LatencyMs ?? 0;
    const packetLossPct = cell.packetLossPct ?? cell.PacketLossPct ?? 0;

    const rtt = status === 'UP' ? `${latencyMs.toFixed(2)} ms` : status;
    const loss = packetLossPct > 0 ? `Loss: ${packetLossPct.toFixed(1)}%` : '0% Loss';

    matrixTooltip.innerHTML = `
      <strong class="font-mono">${escapeHtml(ip)}</strong>
      ${alias ? `<span>${escapeHtml(alias)}</span>` : ''}
      <span class="text-xs text-muted">RTT: <strong>${rtt}</strong> · ${loss}</span>
    `;

    matrixTooltip.style.left = `${rect.left + rect.width / 2}px`;
    matrixTooltip.style.top = `${rect.top - 8}px`;
    matrixTooltip.style.display = 'flex';
  }

  function hideMatrixTooltip() {
    matrixTooltip.style.display = 'none';
  }

  // -------------------------------------------------------------
  // VIEW 2: Outliers & Exception Board
  // -------------------------------------------------------------

  function renderOutliers() {
    el.outliersTableBody.innerHTML = '';
    const list = state.outliers || [];

    if (list.length === 0) {
      el.outliersTableBody.innerHTML = `
        <tr>
          <td colspan="8" style="text-align: center; padding: 3rem 1rem; color: var(--text-muted);">
            🎉 Zero outliers or degraded hosts detected! All systems operate at peak reachability and latency.
          </td>
        </tr>
      `;
      return;
    }

    list.forEach(o => {
      const tr = document.createElement('tr');

      const severity = o.severity ?? o.Severity ?? 'DEGRADED';
      const ip = o.ip ?? o.IP ?? '';
      const subnet = o.subnet ?? o.Subnet ?? '';
      const packetLossPct = o.packetLossPct ?? o.PacketLossPct ?? 0;
      const avgLatencyMs = o.avgLatencyMs ?? o.AvgLatencyMs ?? 0;
      const p95LatencyMs = o.p95LatencyMs ?? o.P95LatencyMs ?? 0;
      const sampleCount = o.sampleCount ?? o.SampleCount ?? 0;

      let sevClass = 'severity-degraded';
      if (severity === 'CRITICAL') sevClass = 'severity-critical';
      else if (severity === 'WARNING') sevClass = 'severity-warning';

      tr.innerHTML = `
        <td><span class="severity-pill ${sevClass}">${escapeHtml(severity)}</span></td>
        <td><strong class="font-mono">${escapeHtml(ip)}</strong></td>
        <td class="text-muted font-mono text-xs">${escapeHtml(subnet || '--')}</td>
        <td class="font-mono ${packetLossPct > 0 ? 'text-down' : ''}"><strong>${packetLossPct.toFixed(1)}%</strong></td>
        <td class="font-mono">${avgLatencyMs ? avgLatencyMs.toFixed(2) + ' ms' : '--'}</td>
        <td class="font-mono"><strong>${p95LatencyMs ? p95LatencyMs.toFixed(2) + ' ms' : '--'}</strong></td>
        <td class="text-xs text-muted">${sampleCount} probes</td>
        <td>
          <button class="btn btn-sm btn-outline btn-outlier-inspect" data-ip="${ip}">Inspect Host</button>
        </td>
      `;

      tr.querySelector('.btn-outlier-inspect').addEventListener('click', (e) => {
        e.stopPropagation();
        openHostDetailModal(ip);
      });
      tr.addEventListener('click', () => openHostDetailModal(ip));

      el.outliersTableBody.appendChild(tr);
    });
  }

  // -------------------------------------------------------------
  // VIEW 3: Host Explorer (List & Cards)
  // -------------------------------------------------------------

  function renderHosts() {
    const list = Array.from(state.hosts.values());

    if (list.length === 0) {
      el.emptyState.style.display = 'flex';
      el.hostsGrid.style.display = 'none';
      el.hostsTableWrapper.style.display = 'none';
      return;
    }

    el.emptyState.style.display = 'none';

    if (state.viewMode === 'grid') {
      el.hostsGrid.style.display = 'grid';
      el.hostsTableWrapper.style.display = 'none';
      renderGridView(list);
    } else {
      el.hostsGrid.style.display = 'none';
      el.hostsTableWrapper.style.display = 'block';
      renderTableView(list);
    }
  }

  function renderGridView(hosts) {
    el.hostsGrid.innerHTML = '';
    hosts.forEach(h => {
      const card = createHostCard(h);
      el.hostsGrid.appendChild(card);
      const canvas = card.querySelector('.sparkline-canvas');
      if (canvas) {
        drawSparkline(canvas, h.latencyHistory || [], h.status);
      }
    });
  }

  function createHostCard(h) {
    const card = document.createElement('div');
    let statusClass = 'status-pending';
    let dotClass = 'dot-pending';
    let pillClass = 'pill-pending';
    let statusText = 'PENDING';

    if (h.isExcluded || h.status === 'EXCLUDED') {
      statusClass = 'status-excluded';
      dotClass = 'dot-excl';
      pillClass = 'pill-excl';
      statusText = 'EXCLUDED';
    } else if (h.status === 'DOWN') {
      if (h.alertAcknowledged) {
        statusClass = 'status-ack';
        dotClass = 'dot-ack';
        pillClass = 'pill-ack';
        statusText = 'ACKNOWLEDGED';
      } else {
        statusClass = 'status-down';
        dotClass = 'dot-down';
        pillClass = 'pill-down';
        statusText = 'DOWN';
      }
    } else if (h.status === 'UP') {
      statusClass = 'status-up';
      dotClass = 'dot-up';
      pillClass = 'pill-up';
      statusText = 'UP';
    }

    card.className = `host-card ${statusClass}`;
    card.dataset.ip = h.ip;

    const latencyDisplay = (h.status === 'UP' && h.latencyMs > 0) 
      ? `<span class="latency-value">${h.latencyMs.toFixed(2)} ms</span>`
      : `<span class="latency-value down-val">${h.isExcluded ? 'Excluded' : (h.lastError || 'Unreachable')}</span>`;

    const ackNoteHtml = (h.alertAcknowledged && h.alertAckBy)
      ? `<div class="ack-box" style="margin-top: 4px;">Acked by ${escapeHtml(h.alertAckBy)}</div>`
      : '';

    card.innerHTML = `
      <div class="host-card-header">
        <div class="host-title-group">
          <div class="status-dot ${dotClass}"></div>
          <div class="host-ip-info">
            <span class="host-ip">${escapeHtml(h.ip)}</span>
            <span class="host-alias">${escapeHtml(h.alias || h.cidr || '')}</span>
          </div>
        </div>
        <span class="status-pill ${pillClass}">${statusText}</span>
      </div>

      <div class="host-card-body">
        <div class="latency-row">
          <span class="text-xs text-muted">Latency RTT</span>
          ${latencyDisplay}
        </div>
        <canvas class="sparkline-canvas" width="260" height="36"></canvas>
        ${ackNoteHtml}
      </div>

      <div class="host-card-stats">
        <span>Loss: <strong class="${h.packetLoss > 0 ? 'text-down' : ''}">${h.packetLoss}%</strong></span>
        <span>Avg: <strong>${h.avgLatencyMs ? h.avgLatencyMs.toFixed(1) + 'ms' : '--'}</strong></span>
        <span>Pkts: <strong>${h.sentPackets || 0}</strong></span>
      </div>
    `;

    card.addEventListener('click', () => openHostDetailModal(h.ip));
    return card;
  }

  function renderTableView(hosts) {
    el.hostsTableBody.innerHTML = '';
    hosts.forEach(h => {
      const tr = createHostTableRow(h);
      el.hostsTableBody.appendChild(tr);

      const canvas = tr.querySelector('.table-sparkline');
      if (canvas) {
        drawSparkline(canvas, h.latencyHistory || [], h.status);
      }
    });
  }

  function createHostTableRow(h) {
    const tr = document.createElement('tr');
    tr.dataset.ip = h.ip;
    if (h.status === 'DOWN') tr.className = 'row-down';

    let pillClass = 'pill-pending';
    let statusText = 'PENDING';
    if (h.isExcluded) {
      pillClass = 'pill-excl';
      statusText = 'EXCLUDED';
    } else if (h.status === 'DOWN') {
      pillClass = h.alertAcknowledged ? 'pill-ack' : 'pill-down';
      statusText = h.alertAcknowledged ? 'ACKNOWLEDGED' : 'DOWN';
    } else if (h.status === 'UP') {
      pillClass = 'pill-up';
      statusText = 'UP';
    }

    const rttText = (h.status === 'UP' && h.latencyMs > 0) ? `${h.latencyMs.toFixed(2)} ms` : '--';
    const lastSeenText = h.lastSeen ? formatTimeAgo(new Date(h.lastSeen)) : 'Never';

    tr.innerHTML = `
      <td><span class="status-pill ${pillClass}">${statusText}</span></td>
      <td>
        <div class="d-flex flex-column">
          <strong class="font-mono">${escapeHtml(h.ip)}</strong>
          <small class="text-muted">${escapeHtml(h.alias || '')}</small>
        </div>
      </td>
      <td class="text-muted font-mono text-xs">${escapeHtml(h.cidr || '--')}</td>
      <td class="font-mono"><strong>${rttText}</strong></td>
      <td><canvas class="table-sparkline" width="100" height="24"></canvas></td>
      <td class="font-mono text-xs">Loss: ${h.packetLoss}% (${h.recvPackets}/${h.sentPackets})</td>
      <td class="text-xs text-muted">${lastSeenText}</td>
      <td>
        <button class="btn btn-sm btn-outline btn-tbl-inspect" data-ip="${h.ip}">Inspect</button>
      </td>
    `;

    tr.querySelector('.btn-tbl-inspect').addEventListener('click', (e) => {
      e.stopPropagation();
      openHostDetailModal(h.ip);
    });
    tr.addEventListener('click', () => openHostDetailModal(h.ip));

    return tr;
  }

  function updatePaginationUI() {
    if (state.totalHostsCount <= state.pageSize && state.currentPage === 1) {
      el.paginationBar.style.display = 'none';
      return;
    }

    el.paginationBar.style.display = 'flex';
    const start = (state.currentPage - 1) * state.pageSize + 1;
    const end = Math.min(state.currentPage * state.pageSize, state.totalHostsCount);

    el.pageRangeStart.textContent = start.toLocaleString();
    el.pageRangeEnd.textContent = end.toLocaleString();
    el.pageTotalCount.textContent = state.totalHostsCount.toLocaleString();

    el.currentPageNum.textContent = state.currentPage;
    el.totalPageCount.textContent = state.totalPages;

    el.btnPrevPage.disabled = (state.currentPage <= 1);
    el.btnNextPage.disabled = (state.currentPage >= state.totalPages);
  }

  // -------------------------------------------------------------
  // TIME-SERIES & SPARKLINE CHART RENDERER
  // -------------------------------------------------------------

  function drawSparkline(canvas, data, status) {
    const ctx = canvas.getContext('2d');
    const width = canvas.width;
    const height = canvas.height;

    ctx.clearRect(0, 0, width, height);

    if (!data || data.length < 2) {
      ctx.fillStyle = 'rgba(255,255,255,0.05)';
      ctx.fillRect(0, 0, width, height);
      return;
    }

    const validPoints = data.filter(v => v > 0);
    const min = validPoints.length > 0 ? Math.min(...validPoints) * 0.8 : 0;
    const max = validPoints.length > 0 ? Math.max(...validPoints) * 1.2 : 10;
    const range = max - min || 1;
    const step = width / (data.length - 1);

    ctx.beginPath();
    let started = false;

    for (let i = 0; i < data.length; i++) {
      const val = data[i];
      const x = i * step;
      if (val <= 0) {
        const y = height - 2;
        if (!started) { ctx.moveTo(x, y); started = true; }
        else { ctx.lineTo(x, y); }
      } else {
        const normalized = (val - min) / range;
        const y = height - (normalized * (height - 8)) - 4;
        if (!started) { ctx.moveTo(x, y); started = true; }
        else { ctx.lineTo(x, y); }
      }
    }

    let strokeColor = '#06b6d4';
    if (status === 'DOWN') strokeColor = '#ef4444';
    else if (status === 'EXCLUDED') strokeColor = '#64748b';

    ctx.strokeStyle = strokeColor;
    ctx.lineWidth = 1.8;
    ctx.lineJoin = 'round';
    ctx.stroke();
  }

  // High-Precision Historical Time Series Chart for Host Modal
  function drawHistoricalChart(canvas, points) {
    const ctx = canvas.getContext('2d');
    const width = canvas.width;
    const height = canvas.height;

    ctx.clearRect(0, 0, width, height);

    if (!points || points.length === 0) {
      ctx.fillStyle = 'rgba(255,255,255,0.05)';
      ctx.fillRect(0, 0, width, height);
      ctx.fillStyle = 'rgba(255,255,255,0.4)';
      ctx.font = '12px Inter, sans-serif';
      ctx.textAlign = 'center';
      ctx.fillText('Accumulating historical rollup metrics...', width / 2, height / 2);
      return;
    }

    const avgs = points.map(p => p.avgLatencyMs || 0).filter(v => v > 0);
    const minVal = avgs.length > 0 ? Math.min(...avgs) * 0.8 : 0;
    const maxVal = avgs.length > 0 ? Math.max(...avgs) * 1.2 : 10;
    const range = maxVal - minVal || 1;
    const step = width / Math.max(points.length - 1, 1);

    // Draw background grid lines
    ctx.strokeStyle = 'rgba(255,255,255,0.06)';
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(0, height / 2);
    ctx.lineTo(width, height / 2);
    ctx.stroke();

    // Draw filled area gradient
    const gradient = ctx.createLinearGradient(0, 0, 0, height);
    gradient.addColorStop(0, 'rgba(6, 182, 212, 0.35)');
    gradient.addColorStop(1, 'rgba(6, 182, 212, 0.0)');

    ctx.beginPath();
    ctx.moveTo(0, height);

    for (let i = 0; i < points.length; i++) {
      const p = points[i];
      const x = i * step;
      const val = p.avgLatencyMs || 0;
      const norm = (val - minVal) / range;
      const y = height - (norm * (height - 20)) - 10;
      ctx.lineTo(x, y);
    }

    ctx.lineTo((points.length - 1) * step, height);
    ctx.closePath();
    ctx.fillStyle = gradient;
    ctx.fill();

    // Draw main line
    ctx.beginPath();
    for (let i = 0; i < points.length; i++) {
      const p = points[i];
      const x = i * step;
      const val = p.avgLatencyMs || 0;
      const norm = (val - minVal) / range;
      const y = height - (norm * (height - 20)) - 10;
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.strokeStyle = '#06b6d4';
    ctx.lineWidth = 2;
    ctx.stroke();

    // Draw Loss bars if any packet loss occurred
    points.forEach((p, idx) => {
      if (p.packetLossPct > 0) {
        const x = idx * step;
        ctx.fillStyle = 'rgba(239, 68, 68, 0.7)';
        const barHeight = (p.packetLossPct / 100.0) * height;
        ctx.fillRect(x - 2, height - barHeight, 4, barHeight);
      }
    });
  }

  // -------------------------------------------------------------
  // HOST DETAIL MODAL
  // -------------------------------------------------------------

  async function openHostDetailModal(ip) {
    state.selectedHostIP = ip;
    el.hostDetailModal.style.display = 'flex';

    try {
      const res = await fetch(`/api/hosts/${ip}`);
      if (res.ok) {
        const host = await res.json();
        state.selectedHostData = host;
        populateHostDetailModal(host);
      }
    } catch (e) {
      console.error('Failed to load host detail:', e);
    }
  }

  function closeHostDetailModal() {
    el.hostDetailModal.style.display = 'none';
    state.selectedHostIP = null;
    state.selectedHostData = null;
  }

  async function loadHostHistory(ip, window) {
    if (window === 'realtime') {
      if (state.selectedHostData) {
        drawSparkline(el.detailSparklineCanvas, state.selectedHostData.latencyHistory || [], state.selectedHostData.status);
      }
      return;
    }

    try {
      const res = await fetch(`/api/hosts/${ip}/history?window=${window}`);
      if (res.ok) {
        state.historyPoints = await res.json();
        drawHistoricalChart(el.detailSparklineCanvas, state.historyPoints);

        // Update P95 and Jitter from historical data if present
        if (state.historyPoints.length > 0) {
          const latest = state.historyPoints[state.historyPoints.length - 1];
          if (latest.p95LatencyMs) el.detailP95RTT.textContent = `${latest.p95LatencyMs.toFixed(2)} ms`;
          if (latest.jitterMs) el.detailJitter.textContent = `${latest.jitterMs.toFixed(2)} ms`;
        }
      }
    } catch (e) {
      console.error('Failed to load host history:', e);
    }
  }

  function populateHostDetailModal(h) {
    el.detailHostIP.textContent = h.ip;
    el.detailHostAlias.textContent = h.alias ? `${h.alias} (${h.cidr || 'Scope'})` : (h.cidr || 'Single Monitored Target');

    // Status dot
    el.detailStatusDot.className = 'status-indicator-lg';
    if (h.isExcluded) el.detailStatusDot.classList.add('dot-excl');
    else if (h.status === 'DOWN') el.detailStatusDot.classList.add(h.alertAcknowledged ? 'dot-ack' : 'dot-down');
    else if (h.status === 'UP') el.detailStatusDot.classList.add('dot-up');

    // Metrics
    el.detailCurrentRTT.textContent = (h.status === 'UP' && h.latencyMs > 0) ? `${h.latencyMs.toFixed(2)} ms` : '--';
    el.detailMinRTT.textContent = h.minLatencyMs > 0 ? `${h.minLatencyMs.toFixed(2)} ms` : '--';
    el.detailAvgRTT.textContent = h.avgLatencyMs > 0 ? `${h.avgLatencyMs.toFixed(2)} ms` : '--';
    el.detailMaxRTT.textContent = h.maxLatencyMs > 0 ? `${h.maxLatencyMs.toFixed(2)} ms` : '--';
    el.detailLoss.textContent = `${h.packetLoss}%`;
    el.detailPackets.textContent = `${h.recvPackets || 0} / ${h.sentPackets || 0}`;
    el.detailLastSeen.textContent = h.lastSeen ? formatTimeAgo(new Date(h.lastSeen)) : 'Never';

    // Latency chart
    if (state.chartWindow === 'realtime') {
      drawSparkline(el.detailSparklineCanvas, h.latencyHistory || [], h.status);
    } else {
      loadHostHistory(h.ip, state.chartWindow);
    }

    // Form inputs
    el.inputHostAlias.value = h.alias || '';
    el.inputHostNotes.value = h.notes || '';

    // Alert Section
    if (h.alertActive && h.status === 'DOWN') {
      el.detailAlertSection.style.display = 'flex';
      el.detailAlertBadge.textContent = h.alertAcknowledged ? 'ACKNOWLEDGED' : 'FIRING';
      el.detailAlertBadge.className = `badge ${h.alertAcknowledged ? 'badge-ack' : 'badge-alert'}`;
      el.detailAlertMsg.textContent = h.lastError ? `Outage reason: ${h.lastError}` : 'Host unreachable via ICMP echo requests.';

      if (h.alertAcknowledged && h.alertAckBy) {
        el.detailAckInfo.style.display = 'block';
        el.detailAckInfo.innerHTML = `Acknowledged by <strong>${escapeHtml(h.alertAckBy)}</strong>: ${escapeHtml(h.alertAckNote || 'No notes')}`;
        el.btnDetailAck.textContent = 'Update Acknowledgement';
      } else {
        el.detailAckInfo.style.display = 'none';
        el.btnDetailAck.textContent = 'Acknowledge Alert';
      }
    } else {
      el.detailAlertSection.style.display = 'none';
    }

    // Exclusion Button
    if (h.isExcluded) {
      el.detailExcludeText.textContent = 'Remove Exclusion';
    } else {
      el.detailExcludeText.textContent = 'Exclude Host';
    }
  }

  async function handleDetailManualPing() {
    if (!state.selectedHostIP) return;
    try {
      el.btnDetailPingNow.disabled = true;
      el.btnDetailPingNow.textContent = 'Probing...';

      const res = await fetch(`/api/hosts/${state.selectedHostIP}/ping`, { method: 'POST' });
      if (res.ok) {
        const result = await res.json();
        if (result.Success) {
          showToast(`Probe successful: ${result.LatencyMs.toFixed(2)} ms`, 'success');
        } else {
          showToast(`Probe failed: ${result.Error || 'Request timeout'}`, 'error');
        }
        await openHostDetailModal(state.selectedHostIP);
      }
    } catch (e) {
      showToast(`Ping error: ${e.message}`, 'error');
    } finally {
      el.btnDetailPingNow.disabled = false;
      el.btnDetailPingNow.innerHTML = `
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="btn-icon">
          <path stroke-linecap="round" stroke-linejoin="round" d="M13 10V3L4 14h7v7l9-11h-7z" />
        </svg>
        <span>Probe Now (Manual Ping)</span>
      `;
    }
  }

  async function handleDetailToggleExclude() {
    if (!state.selectedHostIP || !state.selectedHostData) return;
    const isExcl = state.selectedHostData.isExcluded;

    try {
      if (isExcl) {
        await fetch(`/api/exclusions?rule=${encodeURIComponent(state.selectedHostIP)}`, { method: 'DELETE' });
        showToast(`Exclusion rule removed for ${state.selectedHostIP}`, 'success');
      } else {
        await fetch('/api/exclusions', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            rule: state.selectedHostIP,
            reason: 'Manual exclusion from host inspector',
            enabled: true
          })
        });
        showToast(`Excluded ${state.selectedHostIP} from active monitoring`, 'info');
      }

      await Promise.all([
        fetchExclusions(),
        fetchSubnetsMatrix(),
        fetchOutliers(),
        fetchHosts(state.currentPage),
        fetchAlerts(),
        fetchSummary(),
        openHostDetailModal(state.selectedHostIP)
      ]);
    } catch (e) {
      showToast(`Failed to update exclusion: ${e.message}`, 'error');
    }
  }

  async function handleDetailUnenroll() {
    if (!state.selectedHostIP) return;
    if (!confirm(`Un-enroll host ${state.selectedHostIP} from active monitoring?`)) return;

    try {
      const res = await fetch(`/api/hosts/${state.selectedHostIP}/enrollment`, { method: 'DELETE' });
      if (res.ok) {
        showToast(`Host ${state.selectedHostIP} un-enrolled`, 'info');
        closeHostDetailModal();
        await Promise.all([
          fetchSubnetsMatrix(),
          fetchOutliers(),
          fetchHosts(state.currentPage),
          fetchAlerts(),
          fetchSummary()
        ]);
      }
    } catch (e) {
      showToast(`Un-enroll error: ${e.message}`, 'error');
    }
  }

  async function handleSaveHostMeta(e) {
    e.preventDefault();
    if (!state.selectedHostIP) return;

    try {
      const res = await fetch(`/api/hosts/${state.selectedHostIP}/meta`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          alias: el.inputHostAlias.value.trim(),
          notes: el.inputHostNotes.value.trim()
        })
      });

      if (res.ok) {
        showToast('Host metadata saved', 'success');
        await Promise.all([fetchHosts(state.currentPage), openHostDetailModal(state.selectedHostIP)]);
      }
    } catch (err) {
      showToast(`Failed to save metadata: ${err.message}`, 'error');
    }
  }

  // -------------------------------------------------------------
  // ALERTS & INCIDENT DRAWER
  // -------------------------------------------------------------

  function openAlertsDrawer() {
    el.alertsDrawer.style.display = 'flex';
    const activeTab = document.querySelector('.drawer-tab.active');
    const tabName = activeTab ? activeTab.dataset.tab : 'activeAlerts';
    if (tabName === 'alertHistory') {
      if (el.tabActiveAlerts) el.tabActiveAlerts.style.display = 'none';
      if (el.tabAlertHistory) el.tabAlertHistory.style.display = 'block';
      fetchAlertHistory();
    } else {
      if (el.tabActiveAlerts) el.tabActiveAlerts.style.display = 'block';
      if (el.tabAlertHistory) el.tabAlertHistory.style.display = 'none';
      renderAlertsDrawer();
    }
  }

  function closeAlertsDrawer() {
    el.alertsDrawer.style.display = 'none';
  }

  function renderAlertsDrawer() {
    el.drawerAlertCount.textContent = state.activeAlerts.length;
    el.alertsSummaryCount.textContent = `${state.activeAlerts.length} active alert${state.activeAlerts.length === 1 ? '' : 's'}`;

    el.activeAlertsList.innerHTML = '';
    if (state.activeAlerts.length === 0) {
      el.activeAlertsList.innerHTML = `
        <div class="empty-state" style="padding: 2rem 0;">
          <p>🎉 All monitored hosts are online and reachable.</p>
        </div>
      `;
      return;
    }

    state.activeAlerts.forEach(alt => {
      const item = document.createElement('div');
      item.className = `alert-item ${alt.acknowledged ? 'acknowledged' : 'firing'}`;

      const statusBadge = alt.acknowledged 
        ? `<span class="status-pill pill-ack">ACKNOWLEDGED</span>`
        : `<span class="status-pill pill-down">FIRING</span>`;

      const ackInfo = alt.acknowledged
        ? `<div class="ack-box">Acknowledged by <strong>${escapeHtml(alt.acknowledgedBy || 'Operator')}</strong>: ${escapeHtml(alt.ackNote || 'No notes provided')}</div>`
        : `<button class="btn btn-sm btn-warning btn-ack-single" data-ip="${alt.ip}">Acknowledge Outage</button>`;

      item.innerHTML = `
        <div class="alert-item-header">
          <div class="alert-item-ip">${escapeHtml(alt.ip)}</div>
          ${statusBadge}
        </div>
        <div class="alert-meta">
          <span>Target: <strong>${escapeHtml(alt.alias || alt.cidr || 'Single IP')}</strong></span>
          <span>Outage Started: <strong>${new Date(alt.startedAt).toLocaleTimeString()}</strong> (${formatDuration(alt.durationSec)})</span>
          <span>Reason: <span class="text-down">${escapeHtml(alt.lastError || 'ICMP Request timeout')}</span></span>
        </div>
        ${ackInfo}
      `;

      const btnAck = item.querySelector('.btn-ack-single');
      if (btnAck) {
        btnAck.addEventListener('click', () => {
          closeAlertsDrawer();
          openAcknowledgeModal(alt.ip, alt.id);
        });
      }

      el.activeAlertsList.appendChild(item);
    });
  }

  function renderAlertHistory() {
    el.alertHistoryList.innerHTML = '';
    if (!state.alertHistory || state.alertHistory.length === 0) {
      el.alertHistoryList.innerHTML = `
        <div class="empty-state" style="padding: 2rem 0;">
          <p>No past incident history recorded yet.</p>
        </div>
      `;
      return;
    }

    state.alertHistory.forEach(alt => {
      const item = document.createElement('div');
      item.className = 'alert-item resolved';

      item.innerHTML = `
        <div class="alert-item-header">
          <div class="alert-item-ip">${escapeHtml(alt.ip)}</div>
          <span class="status-pill pill-up">RESOLVED</span>
        </div>
        <div class="alert-meta">
          <span>Target: <strong>${escapeHtml(alt.alias || alt.cidr || 'Single IP')}</strong></span>
          <span>Outage Duration: <strong>${formatDuration(alt.durationSec)}</strong></span>
          <span>Resolved At: <strong>${alt.resolvedAt ? new Date(alt.resolvedAt).toLocaleTimeString() : 'Recently'}</strong></span>
          <span>Reason: <span class="text-down">${escapeHtml(alt.lastError || 'ICMP Request timeout')}</span></span>
        </div>
      `;
      el.alertHistoryList.appendChild(item);
    });
  }

  // -------------------------------------------------------------
  // ACKNOWLEDGEMENT MODAL
  // -------------------------------------------------------------

  function openAcknowledgeModal(ip, alertId) {
    el.ackTargetIP.value = ip;
    el.ackTargetID.value = alertId;
    el.ackModalTargetInfo.textContent = `Acknowledge outage incident for host ${ip}`;
    el.inputAckBy.value = localStorage.getItem('dinis_operator_name') || '';
    el.inputAckNote.value = '';
    el.ackModal.style.display = 'flex';
    el.inputAckNote.focus();
  }

  function closeAckModal() {
    el.ackModal.style.display = 'none';
  }

  async function handleConfirmAcknowledge(e) {
    e.preventDefault();
    const ip = el.ackTargetIP.value;
    const alertId = el.ackTargetID.value;
    const operator = el.inputAckBy.value.trim() || 'NOC Operator';
    const note = el.inputAckNote.value.trim() || 'Acknowledged via Dashboard';

    localStorage.setItem('dinis_operator_name', operator);

    try {
      const res = await fetch('/api/alerts/acknowledge', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ip: ip,
          alertId: alertId,
          operator: operator,
          note: note
        })
      });

      if (res.ok) {
        showToast(`Outage acknowledged for ${ip}`, 'success');
        closeAckModal();
        await Promise.all([fetchAlerts(), fetchHosts(state.currentPage)]);
        if (state.selectedHostIP === ip) {
          await openHostDetailModal(ip);
        }
      }
    } catch (err) {
      showToast(`Failed to acknowledge alert: ${err.message}`, 'error');
    }
  }

  async function handleAcknowledgeAll() {
    if (state.activeAlerts.length === 0) {
      showToast('No active alerts to acknowledge', 'info');
      return;
    }

    const operator = prompt('Enter Operator Name to acknowledge ALL active outages:', localStorage.getItem('dinis_operator_name') || 'NOC Operator');
    if (!operator) return;

    localStorage.setItem('dinis_operator_name', operator);

    try {
      const res = await fetch('/api/alerts/acknowledge-all', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          operator: operator,
          note: 'Bulk acknowledged via dashboard'
        })
      });

      if (res.ok) {
        showToast('All active outages acknowledged', 'success');
        await Promise.all([fetchAlerts(), fetchHosts(state.currentPage)]);
      }
    } catch (e) {
      showToast(`Failed to bulk acknowledge: ${e.message}`, 'error');
    }
  }

  // -------------------------------------------------------------
  // CIDR MANAGEMENT MODAL
  // -------------------------------------------------------------

  function openCIDRModal() {
    el.cidrModal.style.display = 'flex';
    renderCIDRTable();
  }

  function closeCIDRModal() {
    el.cidrModal.style.display = 'none';
    el.cidrFeedback.textContent = '';
  }

  function renderCIDRTable() {
    el.cidrTableBody.innerHTML = '';
    if (state.cidrs.length === 0) {
      el.cidrTableBody.innerHTML = `
        <tr>
          <td colspan="5" style="text-align: center; color: var(--text-muted);">No CIDRs configured. Add a subnet above.</td>
        </tr>
      `;
      return;
    }

    state.cidrs.forEach(c => {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td><strong class="font-mono">${escapeHtml(c.cidr)}</strong></td>
        <td>${escapeHtml(c.description || '--')}</td>
        <td><span class="status-pill ${c.enabled ? 'pill-up' : 'pill-excl'}">${c.enabled ? 'Active' : 'Disabled'}</span></td>
        <td class="text-xs text-muted">${c.includeNetAndBcast ? 'Yes' : 'No'}</td>
        <td>
          <button class="btn btn-sm btn-outline btn-scan-cidr" data-cidr="${c.cidr}" title="Trigger discovery sweep on this subnet">Scan</button>
          <button class="btn btn-sm btn-outline text-down btn-del-cidr" data-cidr="${c.cidr}">Delete</button>
        </td>
      `;

      tr.querySelector('.btn-scan-cidr').addEventListener('click', () => {
        closeCIDRModal();
        triggerDiscovery(c.cidr);
      });

      tr.querySelector('.btn-del-cidr').addEventListener('click', async () => {
        if (confirm(`Remove CIDR ${c.cidr}? Discovered hosts under this subnet will be un-enrolled.`)) {
          await fetch(`/api/cidrs?cidr=${encodeURIComponent(c.cidr)}`, { method: 'DELETE' });
          showToast(`Removed CIDR ${c.cidr}`, 'info');
          await Promise.all([
            fetchCIDRs(),
            fetchSubnetsMatrix(),
            fetchOutliers(),
            fetchHosts(state.currentPage),
            fetchAlerts(),
            fetchSummary()
          ]);
        }
      });

      el.cidrTableBody.appendChild(tr);
    });
  }

  async function handleAddCIDR(e) {
    e.preventDefault();
    const cidr = el.inputCIDR.value.trim();
    const desc = el.inputCIDRDesc.value.trim();
    const incNetBcast = el.checkIncludeNetBcast.checked;

    if (!cidr) return;

    try {
      const res = await fetch('/api/cidrs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          cidr: cidr,
          description: desc,
          includeNetAndBcast: incNetBcast
        })
      });

      const data = await res.json();
      if (res.ok) {
        showToast(`Added CIDR ${cidr} (${data.totalHosts} total capacity)`, 'success');
        el.inputCIDR.value = '';
        el.inputCIDRDesc.value = '';
        el.cidrFeedback.textContent = '';
        await Promise.all([
          fetchCIDRs(),
          fetchSubnetsMatrix(),
          fetchOutliers(),
          fetchHosts(state.currentPage),
          fetchSummary()
        ]);
      } else {
        el.cidrFeedback.textContent = data.error || 'Invalid CIDR subnet';
      }
    } catch (err) {
      el.cidrFeedback.textContent = err.message;
    }
  }

  // -------------------------------------------------------------
  // EXCLUSIONS MODAL
  // -------------------------------------------------------------

  function openExclusionsModal() {
    el.exclusionsModal.style.display = 'flex';
    renderExclusionTable();
  }

  function closeExclusionsModal() {
    el.exclusionsModal.style.display = 'none';
  }

  function renderExclusionTable() {
    el.exclusionTableBody.innerHTML = '';
    if (state.exclusions.length === 0) {
      el.exclusionTableBody.innerHTML = `
        <tr>
          <td colspan="4" style="text-align: center; color: var(--text-muted);">No IP exclusion rules defined.</td>
        </tr>
      `;
      return;
    }

    state.exclusions.forEach(ex => {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td><strong class="font-mono">${escapeHtml(ex.rule)}</strong></td>
        <td>${escapeHtml(ex.reason || 'None provided')}</td>
        <td><span class="status-pill ${ex.enabled ? 'pill-excl' : 'pill-pending'}">${ex.enabled ? 'Active' : 'Disabled'}</span></td>
        <td>
          <button class="btn btn-sm btn-outline text-down btn-del-excl" data-rule="${ex.rule}">Delete</button>
        </td>
      `;

      tr.querySelector('.btn-del-excl').addEventListener('click', async () => {
        await fetch(`/api/exclusions?rule=${encodeURIComponent(ex.rule)}`, { method: 'DELETE' });
        showToast(`Removed exclusion ${ex.rule}`, 'info');
        await Promise.all([
          fetchExclusions(),
          fetchSubnetsMatrix(),
          fetchOutliers(),
          fetchHosts(state.currentPage),
          fetchAlerts(),
          fetchSummary()
        ]);
      });

      el.exclusionTableBody.appendChild(tr);
    });
  }

  async function handleAddExclusion(e) {
    e.preventDefault();
    const rule = el.inputExclRule.value.trim();
    const reason = el.inputExclReason.value.trim();

    if (!rule) return;

    try {
      const res = await fetch('/api/exclusions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          rule: rule,
          reason: reason,
          enabled: true
        })
      });

      if (res.ok) {
        showToast(`Exclusion rule added for ${rule}`, 'success');
        el.inputExclRule.value = '';
        el.inputExclReason.value = '';
        await Promise.all([
          fetchExclusions(),
          fetchSubnetsMatrix(),
          fetchOutliers(),
          fetchHosts(state.currentPage),
          fetchAlerts(),
          fetchSummary()
        ]);
      }
    } catch (err) {
      showToast(`Failed to add exclusion: ${err.message}`, 'error');
    }
  }

  // -------------------------------------------------------------
  // SETTINGS MODAL
  // -------------------------------------------------------------

  function openSettingsModal() {
    const s = state.settings;
    el.inputDiscoveryInterval.value = s.discoveryIntervalMin !== undefined ? s.discoveryIntervalMin : 240;
    el.inputInterval.value = s.intervalSec || 60;
    el.inputTimeout.value = s.timeoutMs || 1000;
    el.inputFailThreshold.value = s.failThreshold || 2;
    el.inputConcurrency.value = s.concurrency || 100;
    el.settingsModal.style.display = 'flex';
  }

  function closeSettingsModal() {
    el.settingsModal.style.display = 'none';
  }

  async function handleSaveSettings(e) {
    e.preventDefault();
    const payload = {
      discoveryIntervalMin: parseInt(el.inputDiscoveryInterval.value, 10),
      intervalSec: parseFloat(el.inputInterval.value),
      timeoutMs: parseInt(el.inputTimeout.value, 10),
      failThreshold: parseInt(el.inputFailThreshold.value, 10),
      concurrency: parseInt(el.inputConcurrency.value, 10),
      autoDiscovery: parseInt(el.inputDiscoveryInterval.value, 10) > 0
    };

    try {
      const res = await fetch('/api/settings', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      if (res.ok) {
        state.settings = payload;
        showToast('Settings saved and engine re-configured', 'success');
        closeSettingsModal();
        fetchSummary();
      }
    } catch (err) {
      showToast(`Failed to save settings: ${err.message}`, 'error');
    }
  }

  // -------------------------------------------------------------
  // DISCOVERY HELPERS
  // -------------------------------------------------------------

  async function triggerDiscovery(cidr = '') {
    if (state.discoveryStatus.isScanning) {
      showToast('Discovery scan is already in progress', 'info');
      return;
    }

    try {
      el.btnRunDiscovery.disabled = true;
      el.discRadarIcon.classList.add('scanning');
      el.btnDiscoveryText.textContent = 'Scanning Subnets...';

      const res = await fetch('/api/discovery/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ cidr })
      });

      if (!res.ok) {
        const err = await res.json();
        showToast(`Discovery error: ${err.error}`, 'error');
      }
    } catch (e) {
      showToast(`Failed to trigger discovery: ${e.message}`, 'error');
    }
  }

  function updateDiscoveryUI() {
    if (state.discoveryStatus.isScanning) {
      el.btnRunDiscovery.disabled = true;
      el.discRadarIcon.classList.add('scanning');
      el.btnDiscoveryText.textContent = 'Scanning Subnets...';
    } else {
      el.btnRunDiscovery.disabled = false;
      el.discRadarIcon.classList.remove('scanning');
      el.btnDiscoveryText.textContent = 'Run Discovery';
    }
  }

  // -------------------------------------------------------------
  // UTILITY HELPERS
  // -------------------------------------------------------------

  function showToast(msg, type = 'info') {
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.textContent = msg;
    el.toastContainer.appendChild(toast);
    setTimeout(() => {
      toast.style.opacity = '0';
      setTimeout(() => toast.remove(), 200);
    }, 4000);
  }

  function escapeHtml(str) {
    if (!str) return '';
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  function formatTimeAgo(date) {
    const sec = Math.floor((new Date() - date) / 1000);
    if (sec < 5) return 'Just now';
    if (sec < 60) return `${sec}s ago`;
    if (sec < 3600) return `${Math.floor(sec / 60)}m ago`;
    if (sec < 86400) return `${Math.floor(sec / 3600)}h ago`;
    return `${Math.floor(sec / 86400)}d ago`;
  }

  function formatDuration(sec) {
    if (!sec || sec < 0) return '0s';
    if (sec < 60) return `${Math.floor(sec)}s`;
    if (sec < 3600) return `${Math.floor(sec / 60)}m ${Math.floor(sec % 60)}s`;
    return `${Math.floor(sec / 3600)}h ${Math.floor((sec % 3600) / 60)}m`;
  }

  // Start app on DOMContentLoaded
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
