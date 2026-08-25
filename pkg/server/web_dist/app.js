/**
 * DINIS — High-Performance ICMP Network Monitor Web Application
 */

(function() {
  'use strict';

  // State
  const state = {
    hosts: new Map(), // IP -> Host object
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
      soundAlerts: true,
      discoveryIntervalMin: 240,
      autoDiscovery: true
    },
    filter: 'all',
    search: '',
    sort: 'status',
    viewMode: 'grid',
    selectedHostIP: null,
    sseConnected: false,
    soundEnabled: true,
    audioCtx: null
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

    // Toolbar
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

    // Displays
    hostsGrid: document.getElementById('hostsGrid'),
    hostsTableWrapper: document.getElementById('hostsTableWrapper'),
    hostsTableBody: document.getElementById('hostsTableBody'),
    emptyState: document.getElementById('emptyState'),

    // Buttons
    btnOpenAlerts: document.getElementById('btnOpenAlerts'),
    btnOpenCIDR: document.getElementById('btnOpenCIDR'),
    btnOpenExclusions: document.getElementById('btnOpenExclusions'),
    btnOpenSettings: document.getElementById('btnOpenSettings'),
    btnToggleSound: document.getElementById('btnToggleSound'),
    soundIconOn: document.getElementById('soundIconOn'),
    soundIconOff: document.getElementById('soundIconOff'),

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
    detailCurrentRTT: document.getElementById('detailCurrentRTT'),
    detailSparklineCanvas: document.getElementById('detailSparklineCanvas'),
    detailMinRTT: document.getElementById('detailMinRTT'),
    detailAvgRTT: document.getElementById('detailAvgRTT'),
    detailMaxRTT: document.getElementById('detailMaxRTT'),
    detailLoss: document.getElementById('detailLoss'),
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

  // Initialize
  async function init() {
    setupEventListeners();
    await Promise.all([
      fetchSettings(),
      fetchDiscoveryStatus(),
      fetchCIDRs(),
      fetchExclusions(),
      fetchHosts(),
      fetchAlerts()
    ]);
    connectSSE();
    renderAll();

    // Auto-refresh timer: ensures metrics, summary, pacing and reachability
    // stay fresh and in sync continuously in real time.
    setInterval(async () => {
      try {
        const res = await fetch('/api/summary');
        if (res.ok) {
          state.summary = await res.json();
          renderKPIs();
        }
        if (!state.sseConnected) {
          // If SSE disconnected, fallback to active polling
          await Promise.all([fetchHosts(), fetchAlerts(), fetchDiscoveryStatus()]);
        }
      } catch (e) {
        // silent fail on network blip
      }
    }, 3000);
  }

  // Event Listeners
  function setupEventListeners() {
    // Search
    el.hostSearchInput.addEventListener('input', (e) => {
      state.search = e.target.value.toLowerCase().trim();
      el.btnClearSearch.style.display = state.search ? 'block' : 'none';
      renderHosts();
    });

    el.btnClearSearch.addEventListener('click', () => {
      el.hostSearchInput.value = '';
      state.search = '';
      el.btnClearSearch.style.display = 'none';
      renderHosts();
    });

    // Filter chips
    el.filterChips.forEach(chip => {
      chip.addEventListener('click', () => {
        el.filterChips.forEach(c => c.classList.remove('active'));
        chip.classList.add('active');
        state.filter = chip.dataset.filter;
        renderHosts();
      });
    });

    // Sort
    el.sortSelect.addEventListener('change', (e) => {
      state.sort = e.target.value;
      renderHosts();
    });

    // View toggle
    el.btnViewGrid.addEventListener('click', () => {
      state.viewMode = 'grid';
      el.btnViewGrid.classList.add('active');
      el.btnViewTable.classList.remove('active');
      el.hostsGrid.style.display = 'grid';
      el.hostsTableWrapper.style.display = 'none';
      renderHosts();
    });

    el.btnViewTable.addEventListener('click', () => {
      state.viewMode = 'table';
      el.btnViewTable.classList.add('active');
      el.btnViewGrid.classList.remove('active');
      el.hostsGrid.style.display = 'none';
      el.hostsTableWrapper.style.display = 'block';
      renderHosts();
    });

    // Modals open/close
    el.btnOpenAlerts.addEventListener('click', openAlertsDrawer);
    el.btnCloseAlertsDrawer.addEventListener('click', closeAlertsDrawer);
    el.btnBannerViewAlerts.addEventListener('click', openAlertsDrawer);

    el.btnOpenCIDR.addEventListener('click', () => openModal(el.cidrModal));
    el.btnCloseCidrModal.addEventListener('click', () => closeModal(el.cidrModal));

    el.btnOpenExclusions.addEventListener('click', () => openModal(el.exclusionsModal));
    el.btnCloseExclusionsModal.addEventListener('click', () => closeModal(el.exclusionsModal));

    el.btnOpenSettings.addEventListener('click', openSettingsModal);
    el.btnCloseSettingsModal.addEventListener('click', () => closeModal(el.settingsModal));
    el.btnCancelSettings.addEventListener('click', () => closeModal(el.settingsModal));

    el.btnCloseAckModal.addEventListener('click', () => closeModal(el.ackModal));
    el.btnCancelAck.addEventListener('click', () => closeModal(el.ackModal));

    el.btnCloseHostDetailModal.addEventListener('click', () => closeModal(el.hostDetailModal));

    // Drawer Tabs
    el.drawerTabs.forEach(tab => {
      tab.addEventListener('click', () => {
        el.drawerTabs.forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
        const target = tab.dataset.tab;
        if (target === 'activeAlerts') {
          el.tabActiveAlerts.style.display = 'block';
          el.tabAlertHistory.style.display = 'none';
        } else {
          el.tabActiveAlerts.style.display = 'none';
          el.tabAlertHistory.style.display = 'block';
          fetchAlertHistory();
        }
      });
    });

    // Sound toggle
    el.btnToggleSound.addEventListener('click', toggleSound);

    // Forms
    el.formAddCIDR.addEventListener('submit', handleAddCIDR);
    el.inputCIDR.addEventListener('input', validateCIDRInput);
    el.formAddExclusion.addEventListener('submit', handleAddExclusion);
    el.formAcknowledgeAlert.addEventListener('submit', handleAcknowledgeSubmit);
    el.formSettings.addEventListener('submit', handleSaveSettings);
    el.formHostMeta.addEventListener('submit', handleSaveHostMeta);

    // Global Acknowledge All Buttons
    el.btnQuickAckAll.addEventListener('click', promptAcknowledgeAll);
    el.btnBannerAckAll.addEventListener('click', promptAcknowledgeAll);
    el.btnDrawerAckAll.addEventListener('click', promptAcknowledgeAll);

    // Discovery actions
    el.btnRunDiscovery.addEventListener('click', () => triggerDiscovery());
    if (el.btnCidrModalDiscoverAll) {
      el.btnCidrModalDiscoverAll.addEventListener('click', () => triggerDiscovery());
    }

    // Host Detail Action Buttons
    el.btnDetailPingNow.addEventListener('click', handleManualPing);
    el.btnDetailToggleExclude.addEventListener('click', handleToggleExclude);
    el.btnDetailUnenroll.addEventListener('click', handleUnenrollHost);
    el.btnDetailAck.addEventListener('click', () => {
      if (state.selectedHostIP) {
        closeModal(el.hostDetailModal);
        openAcknowledgeModal(state.selectedHostIP);
      }
    });

    // Close modals on overlay backdrop click
    document.querySelectorAll('.modal-overlay, .drawer-overlay').forEach(overlay => {
      overlay.addEventListener('click', (e) => {
        if (e.target === overlay) {
          overlay.style.display = 'none';
        }
      });
    });
  }

  // Web Audio Alert Sound Generator
  function playAlertChime() {
    if (!state.soundEnabled) return;
    try {
      if (!state.audioCtx) {
        state.audioCtx = new (window.AudioContext || window.webkitAudioContext)();
      }
      if (state.audioCtx.state === 'suspended') {
        state.audioCtx.resume();
      }
      const now = state.audioCtx.currentTime;
      const osc = state.audioCtx.createOscillator();
      const gain = state.audioCtx.createGain();

      osc.type = 'sine';
      osc.frequency.setValueAtTime(587.33, now); // D5
      osc.frequency.exponentialRampToValueAtTime(880, now + 0.1); // A5
      osc.frequency.exponentialRampToValueAtTime(440, now + 0.3); // A4

      gain.gain.setValueAtTime(0.2, now);
      gain.gain.exponentialRampToValueAtTime(0.001, now + 0.5);

      osc.connect(gain);
      gain.connect(state.audioCtx.destination);

      osc.start(now);
      osc.stop(now + 0.5);
    } catch (e) {
      console.warn('Audio alert not allowed by browser autoplay policy yet:', e);
    }
  }

  function toggleSound() {
    state.soundEnabled = !state.soundEnabled;
    el.soundIconOn.style.display = state.soundEnabled ? 'block' : 'none';
    el.soundIconOff.style.display = state.soundEnabled ? 'none' : 'block';
    showToast(state.soundEnabled ? 'Audio alerts enabled' : 'Audio alerts muted', 'info');
    if (state.soundEnabled) {
      playAlertChime();
    }
  }

  // SSE Real-time Streaming Connection
  function connectSSE() {
    const sse = new EventSource('/api/stream');

    sse.onopen = () => {
      state.sseConnected = true;
      el.liveStatusBadge.classList.remove('disconnected');
      el.liveStatusText.textContent = 'STREAMING LIVE';
    };

    sse.onerror = () => {
      state.sseConnected = false;
      el.liveStatusBadge.classList.add('disconnected');
      el.liveStatusText.textContent = 'RECONNECTING...';
    };

    sse.addEventListener('summary_update', (e) => {
      try {
        const data = JSON.parse(e.data);
        state.summary = data;
        renderKPIs();
      } catch (err) {
        console.error('Failed to parse summary_update:', err);
      }
    });

    sse.addEventListener('host_update', (e) => {
      try {
        const host = JSON.parse(e.data);
        state.hosts.set(host.ip, host);
        updateHostView(host);
        updateFilterCounts();
        renderKPIs();
        if (state.selectedHostIP === host.ip && el.hostDetailModal.style.display !== 'none') {
          populateHostDetailModal(host);
        }
      } catch (err) {
        console.error('Failed to parse host_update:', err);
      }
    });

    sse.addEventListener('host_state_change', (e) => {
      try {
        const payload = JSON.parse(e.data);
        const host = payload.host;
        state.hosts.set(host.ip, host);
        renderHosts();
        updateFilterCounts();
        fetchAlerts();
        renderKPIs();

        if (payload.newStatus === 'DOWN') {
          playAlertChime();
          showToast(`Host ${host.ip} (${host.alias || host.cidr}) is DOWN!`, 'error');
        } else if (payload.oldStatus === 'DOWN' && payload.newStatus === 'UP') {
          showToast(`Host ${host.ip} has recovered (UP).`, 'success');
        }
      } catch (err) {
        console.error('Failed to parse host_state_change:', err);
      }
    });

    sse.addEventListener('discovery_started', (e) => {
      try {
        state.discoveryStatus.isScanning = true;
        updateDiscoveryUI();
        showToast('Subnet discovery scan started...', 'info');
      } catch (err) {
        console.error('Failed to parse discovery_started:', err);
      }
    });

    sse.addEventListener('discovery_completed', (e) => {
      try {
        const payload = JSON.parse(e.data);
        state.discoveryStatus = payload.status;
        updateDiscoveryUI();
        showToast(`Discovery finished: ${payload.discoveredOnline} active hosts found (${payload.newDiscovered} new).`, 'success');
        fetchHosts();
        fetchCIDRs();
      } catch (err) {
        console.error('Failed to parse discovery_completed:', err);
      }
    });

    sse.addEventListener('alert_fired', (e) => {
      try {
        fetchAlerts();
        fetchHosts();
      } catch (err) {
        console.error('Failed to parse alert_fired:', err);
      }
    });

    sse.addEventListener('alert_acknowledged', (e) => {
      try {
        fetchAlerts();
        fetchHosts();
      } catch (err) {
        console.error('Failed to parse alert_acknowledged:', err);
      }
    });

    sse.addEventListener('alert_resolved', (e) => {
      try {
        fetchAlerts();
        fetchHosts();
      } catch (err) {
        console.error('Failed to parse alert_resolved:', err);
      }
    });
  }

  // REST API Calls
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

  async function fetchHosts() {
    try {
      const res = await fetch('/api/hosts');
      if (res.ok) {
        const list = await res.json();
        state.hosts.clear();
        for (const h of list) {
          state.hosts.set(h.ip, h);
        }
        renderHosts();
        updateFilterCounts();
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

  // Render Functions
  function renderAll() {
    renderKPIs();
    renderHosts();
    renderCIDRTable();
    renderExclusionTable();
    renderAlertsDrawer();
  }

  function renderKPIs() {
    const total = state.hosts.size;
    let up = 0;
    let down = 0;
    let excluded = 0;
    let sumLatency = 0;
    let upLatencyCount = 0;

    for (const h of state.hosts.values()) {
      if (h.status === 'UP') {
        up++;
        if (h.latencyMs > 0) {
          sumLatency += h.latencyMs;
          upLatencyCount++;
        }
      } else if (h.status === 'DOWN') {
        down++;
      } else if (h.status === 'EXCLUDED' || h.isExcluded) {
        excluded++;
      }
    }

    // Authoritative unack count from activeAlerts
    const unackAlerts = state.activeAlerts.filter(a => !a.acknowledged && a.state !== 'RESOLVED');
    const unackCount = unackAlerts.length;

    const avgLat = upLatencyCount > 0 ? (sumLatency / upLatencyCount).toFixed(2) : '0.00';
    const monitoredActive = total - excluded;
    const healthRate = monitoredActive > 0 ? Math.round((up / monitoredActive) * 100) : 100;

    el.kpiTotal.textContent = total;
    const totalCap = state.discoveryStatus.subnetCapacity || total;
    el.kpiCidrCount.textContent = `${state.cidrs.length} Subnet${state.cidrs.length === 1 ? '' : 's'} (${totalCap} Capacity)`;

    el.kpiUp.textContent = up;
    el.kpiHealthRate.textContent = `${healthRate}% Reachability`;

    el.kpiDown.textContent = down;
    el.kpiUnackCount.textContent = `${unackCount} Unacknowledged`;

    el.kpiAlerts.textContent = state.activeAlerts.length;
    el.kpiAvgLatency.textContent = `${avgLat} ms`;

    // Render Pacing Chips
    if (el.kpiPacingContainer) {
      if (state.summary && state.summary.packetsPerSec > 0) {
        const ppsText = formatPPS(state.summary.packetsPerSec);
        const delayText = formatPacedDelay(state.summary.pacedDelayMs);
        el.kpiPacingContainer.innerHTML = `
          <span class="pacing-chip" title="Packet dispatch rate"><strong>${ppsText}</strong></span>
          <span class="pacing-dot">•</span>
          <span class="pacing-chip" title="Inter-probe spacing delay"><strong>${delayText}</strong></span>
        `;
      } else {
        el.kpiPacingContainer.innerHTML = `<span class="pacing-chip">Paced across <strong>${state.settings.intervalSec || 60}s</strong></span>`;
      }
    }

    el.kpiExcluded.textContent = excluded;
    el.kpiExclRuleCount.textContent = `${state.exclusions.length} Exclusion Rules`;

    // Visual Alert Badges & Banners
    if (down > 0) {
      el.cardKpiDown.classList.add('has-down');
    } else {
      el.cardKpiDown.classList.remove('has-down');
    }

    if (unackCount > 0) {
      el.navAlertBadge.style.display = 'inline-flex';
      el.navAlertBadge.textContent = unackCount;
      el.unackAlertBanner.style.display = 'flex';
      el.bannerAlertTitle.textContent = `${unackCount} Unacknowledged Host Outage${unackCount === 1 ? '' : 's'} Detected!`;
      el.btnQuickAckAll.style.display = 'inline-block';
    } else {
      el.navAlertBadge.style.display = 'none';
      el.unackAlertBanner.style.display = 'none';
      el.btnQuickAckAll.style.display = 'none';
    }
  }

  function updateFilterCounts() {
    let all = 0, down = 0, ack = 0, up = 0, excl = 0;
    for (const h of state.hosts.values()) {
      all++;
      if (h.isExcluded || h.status === 'EXCLUDED') {
        excl++;
      } else if (h.status === 'DOWN') {
        if (h.alertAcknowledged) {
          ack++;
        } else {
          down++;
        }
      } else if (h.status === 'UP') {
        up++;
      }
    }

    el.countAll.textContent = all;
    el.countDown.textContent = down;
    el.countAck.textContent = ack;
    el.countUp.textContent = up;
    el.countExcluded.textContent = excl;
  }

  function getFilteredAndSortedHosts() {
    let list = Array.from(state.hosts.values());

    // Search filter
    if (state.search) {
      list = list.filter(h => {
        return h.ip.toLowerCase().includes(state.search) ||
               (h.alias && h.alias.toLowerCase().includes(state.search)) ||
               (h.cidr && h.cidr.toLowerCase().includes(state.search)) ||
               (h.exclusionReason && h.exclusionReason.toLowerCase().includes(state.search));
      });
    }

    // Status filter chip
    if (state.filter === 'down') {
      list = list.filter(h => h.status === 'DOWN' && !h.alertAcknowledged && !h.isExcluded);
    } else if (state.filter === 'ack') {
      list = list.filter(h => h.status === 'DOWN' && h.alertAcknowledged && !h.isExcluded);
    } else if (state.filter === 'up') {
      list = list.filter(h => h.status === 'UP' && !h.isExcluded);
    } else if (state.filter === 'excluded') {
      list = list.filter(h => h.isExcluded || h.status === 'EXCLUDED');
    }

    // Sort
    list.sort((a, b) => {
      switch (state.sort) {
        case 'ip-asc':
          return ipToNumber(a.ip) - ipToNumber(b.ip);
        case 'ip-desc':
          return ipToNumber(b.ip) - ipToNumber(a.ip);
        case 'status': {
          // Order: Down unacknowledged (1), Down acknowledged (2), Up (3), Excluded (4), Pending (5)
          const weightA = getStatusWeight(a);
          const weightB = getStatusWeight(b);
          if (weightA !== weightB) return weightA - weightB;
          return ipToNumber(a.ip) - ipToNumber(b.ip);
        }
        case 'latency-desc':
          return b.latencyMs - a.latencyMs;
        case 'latency-asc':
          return (a.latencyMs || 9999) - (b.latencyMs || 9999);
        case 'loss':
          return b.packetLoss - a.packetLoss;
        default:
          return 0;
      }
    });

    return list;
  }

  function getStatusWeight(h) {
    if (h.isExcluded || h.status === 'EXCLUDED') return 4;
    if (h.status === 'DOWN') {
      return h.alertAcknowledged ? 2 : 1;
    }
    if (h.status === 'UP') return 3;
    return 5;
  }

  function ipToNumber(ip) {
    const parts = ip.split('.').map(Number);
    if (parts.length !== 4) return 0;
    return (parts[0] << 24) + (parts[1] << 16) + (parts[2] << 8) + parts[3];
  }

  function renderHosts() {
    const list = getFilteredAndSortedHosts();

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
      // Draw sparkline canvas
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

  function updateHostView(host) {
    if (state.viewMode === 'grid') {
      const card = el.hostsGrid.querySelector(`.host-card[data-ip="${host.ip}"]`);
      if (card) {
        const newCard = createHostCard(host);
        el.hostsGrid.replaceChild(newCard, card);
        const canvas = newCard.querySelector('.sparkline-canvas');
        if (canvas) {
          drawSparkline(canvas, host.latencyHistory || [], host.status);
        }
      } else {
        renderHosts();
      }
    } else if (state.viewMode === 'table') {
      const tr = el.hostsTableBody.querySelector(`tr[data-ip="${host.ip}"]`);
      if (tr) {
        const newTr = createHostTableRow(host);
        el.hostsTableBody.replaceChild(newTr, tr);
        const canvas = newTr.querySelector('.table-sparkline');
        if (canvas) {
          drawSparkline(canvas, host.latencyHistory || [], host.status);
        }
      } else {
        renderHosts();
      }
    }
  }

  // Sparkline Chart Renderer
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

    // Filter valid positive latencies to determine min/max scale
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
        // Drop / loss point
        const y = height - 2;
        if (!started) {
          ctx.moveTo(x, y);
          started = true;
        } else {
          ctx.lineTo(x, y);
        }
      } else {
        const normalized = (val - min) / range;
        const y = height - (normalized * (height - 8)) - 4;
        if (!started) {
          ctx.moveTo(x, y);
          started = true;
        } else {
          ctx.lineTo(x, y);
        }
      }
    }

    // Colors based on status
    let strokeColor = '#06b6d4';
    if (status === 'DOWN') strokeColor = '#ef4444';
    else if (status === 'EXCLUDED') strokeColor = '#64748b';

    ctx.strokeStyle = strokeColor;
    ctx.lineWidth = 1.8;
    ctx.lineJoin = 'round';
    ctx.stroke();

    // Draw last point dot
    const lastVal = data[data.length - 1];
    const lastX = (data.length - 1) * step;
    let lastY = height - 2;
    if (lastVal > 0) {
      const norm = (lastVal - min) / range;
      lastY = height - (norm * (height - 8)) - 4;
    }

    ctx.beginPath();
    ctx.arc(lastX, lastY, 3, 0, Math.PI * 2);
    ctx.fillStyle = strokeColor;
    ctx.fill();
  }

  // Alerts Drawer & Incident Center
  function openAlertsDrawer() {
    el.alertsDrawer.style.display = 'flex';
    renderAlertsDrawer();
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
    if (state.alertHistory.length === 0) {
      el.alertHistoryList.innerHTML = `
        <div class="empty-state" style="padding: 2rem 0;">
          <p>No historical resolved incidents.</p>
        </div>
      `;
      return;
    }

    state.alertHistory.forEach(alt => {
      const item = document.createElement('div');
      item.className = 'alert-item';
      item.innerHTML = `
        <div class="alert-item-header">
          <div class="alert-item-ip">${escapeHtml(alt.ip)}</div>
          <span class="status-pill pill-up">RESOLVED</span>
        </div>
        <div class="alert-meta">
          <span>Duration: <strong>${formatDuration(alt.durationSec)}</strong></span>
          <span>Started: ${new Date(alt.startedAt).toLocaleString()}</span>
          <span>Recovered: ${alt.resolvedAt ? new Date(alt.resolvedAt).toLocaleString() : '--'}</span>
          ${alt.acknowledgedBy ? `<span>Acked by: ${escapeHtml(alt.acknowledgedBy)} (${escapeHtml(alt.ackNote || '')})</span>` : ''}
        </div>
      `;
      el.alertHistoryList.appendChild(item);
    });
  }

  // Acknowledge Modal
  function openAcknowledgeModal(ip, id = '') {
    el.ackTargetIP.value = ip;
    el.ackTargetID.value = id;
    const h = state.hosts.get(ip);
    const label = h ? (h.alias ? `${ip} (${h.alias})` : ip) : ip;
    el.ackModalTargetInfo.textContent = `Target: ${label}`;
    openModal(el.ackModal);
  }

  async function handleAcknowledgeSubmit(e) {
    e.preventDefault();
    const ip = el.ackTargetIP.value;
    const id = el.ackTargetID.value;
    const ackBy = el.inputAckBy.value.trim() || 'NOC Admin';
    const note = el.inputAckNote.value.trim();

    try {
      const res = await fetch('/api/alerts/acknowledge', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ip, id, ackBy, note })
      });

      if (!res.ok) {
        const err = await res.json();
        showToast(`Failed to acknowledge: ${err.error}`, 'error');
        return;
      }

      closeModal(el.ackModal);
      showToast(`Outage on ${ip} acknowledged`, 'success');
      await Promise.all([fetchHosts(), fetchAlerts()]);
    } catch (err) {
      showToast(`Network error: ${err.message}`, 'error');
    }
  }

  async function promptAcknowledgeAll() {
    const unackAlerts = state.activeAlerts.filter(a => !a.acknowledged);
    if (unackAlerts.length === 0) {
      showToast('No unacknowledged alerts to acknowledge.', 'info');
      return;
    }

    const confirmed = confirm(`Acknowledge all ${unackAlerts.length} active outages?`);
    if (!confirmed) return;

    try {
      const res = await fetch('/api/alerts/acknowledge-all', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ackBy: 'NOC Admin', note: 'Bulk acknowledged by operator' })
      });

      if (res.ok) {
        showToast(`All ${unackAlerts.length} alerts acknowledged`, 'success');
        await Promise.all([fetchHosts(), fetchAlerts()]);
      }
    } catch (e) {
      showToast(`Failed to acknowledge all: ${e.message}`, 'error');
    }
  }

  // CIDR Subnet Modal
  function validateCIDRInput() {
    const val = el.inputCIDR.value.trim();
    if (!val) {
      el.cidrFeedback.textContent = '';
      el.cidrFeedback.className = 'field-feedback';
      return;
    }

    if (val.includes('/')) {
      const parts = val.split('/');
      const mask = parseInt(parts[1], 10);
      if (!isNaN(mask) && mask >= 0 && mask <= 32) {
        const total = Math.pow(2, 32 - mask);
        const usable = mask <= 30 ? (total - 2) : total;
        el.cidrFeedback.textContent = `Valid CIDR: ~${usable} usable host IPs (Total: ${total})`;
        el.cidrFeedback.className = 'field-feedback valid';
        return;
      }
    }
    el.cidrFeedback.textContent = 'Enter valid IP or CIDR (e.g. 192.168.1.0/24 or 8.8.8.8)';
    el.cidrFeedback.className = 'field-feedback invalid';
  }

  async function handleAddCIDR(e) {
    e.preventDefault();
    const cidr = el.inputCIDR.value.trim();
    const description = el.inputCIDRDesc.value.trim();
    const includeNetAndBcast = el.checkIncludeNetBcast.checked;

    try {
      const res = await fetch('/api/cidrs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ cidr, description, includeNetAndBcast })
      });

      if (!res.ok) {
        const err = await res.json();
        showToast(err.error || 'Failed to add CIDR', 'error');
        return;
      }

      const result = await res.json();
      showToast(`Added CIDR ${cidr} (${result.totalHosts} hosts)`, 'success');
      el.inputCIDR.value = '';
      el.inputCIDRDesc.value = '';
      el.cidrFeedback.textContent = '';
      await Promise.all([fetchCIDRs(), fetchHosts()]);
    } catch (err) {
      showToast(`Network error: ${err.message}`, 'error');
    }
  }

  function renderCIDRTable() {
    el.cidrTableBody.innerHTML = '';
    if (state.cidrs.length === 0) {
      el.cidrTableBody.innerHTML = '<tr><td colspan="5" class="text-center text-muted">No subnets configured</td></tr>';
      return;
    }

    state.cidrs.forEach(c => {
      const tr = document.createElement('tr');
      // Count hosts from this CIDR
      let count = 0;
      for (const h of state.hosts.values()) {
        if (h.cidr === c.cidr) count++;
      }

      tr.innerHTML = `
        <td class="font-mono font-bold">${escapeHtml(c.cidr)}</td>
        <td>${escapeHtml(c.description || '--')}</td>
        <td><span class="badge" style="background: rgba(56, 189, 248, 0.15); color: #38bdf8;">${count} active</span></td>
        <td class="text-muted text-xs">${new Date(c.createdAt).toLocaleDateString()}</td>
        <td>
          <div class="d-flex" style="gap: 6px;">
            <button class="btn btn-sm btn-outline btn-scan-cidr" data-cidr="${c.cidr}" title="Scan this subnet for online devices">Scan</button>
            <button class="btn btn-sm btn-danger btn-del-cidr" data-cidr="${c.cidr}">Delete</button>
          </div>
        </td>
      `;

      tr.querySelector('.btn-scan-cidr').addEventListener('click', () => {
        closeModal(el.cidrModal);
        triggerDiscovery(c.cidr);
      });
      tr.querySelector('.btn-del-cidr').addEventListener('click', () => handleDeleteCIDR(c.cidr));
      el.cidrTableBody.appendChild(tr);
    });
  }

  async function handleDeleteCIDR(cidr) {
    if (!confirm(`Delete CIDR subnet ${cidr}? Its hosts will stop being monitored.`)) return;

    try {
      const res = await fetch(`/api/cidrs?cidr=${encodeURIComponent(cidr)}`, { method: 'DELETE' });
      if (res.ok) {
        showToast(`Removed CIDR ${cidr}`, 'success');
        await Promise.all([fetchCIDRs(), fetchHosts()]);
      }
    } catch (e) {
      showToast(`Failed to delete CIDR: ${e.message}`, 'error');
    }
  }

  // Exclusion Modal
  async function handleAddExclusion(e) {
    e.preventDefault();
    const rule = el.inputExclRule.value.trim();
    const reason = el.inputExclReason.value.trim();

    try {
      const res = await fetch('/api/exclusions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ rule, reason })
      });

      if (!res.ok) {
        const err = await res.json();
        showToast(err.error || 'Failed to add exclusion', 'error');
        return;
      }

      showToast(`Exclusion rule added for ${rule}`, 'success');
      el.inputExclRule.value = '';
      el.inputExclReason.value = '';
      await Promise.all([fetchExclusions(), fetchHosts()]);
    } catch (err) {
      showToast(`Network error: ${err.message}`, 'error');
    }
  }

  function renderExclusionTable() {
    el.exclusionTableBody.innerHTML = '';
    if (state.exclusions.length === 0) {
      el.exclusionTableBody.innerHTML = '<tr><td colspan="4" class="text-center text-muted">No exclusion rules configured</td></tr>';
      return;
    }

    state.exclusions.forEach(ex => {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td class="font-mono font-bold">${escapeHtml(ex.rule)}</td>
        <td>${escapeHtml(ex.reason || 'No reason specified')}</td>
        <td class="text-muted text-xs">${new Date(ex.createdAt).toLocaleDateString()}</td>
        <td>
          <button class="btn btn-sm btn-outline btn-del-excl" data-rule="${ex.rule}">Remove</button>
        </td>
      `;

      tr.querySelector('.btn-del-excl').addEventListener('click', () => handleDeleteExclusion(ex.rule));
      el.exclusionTableBody.appendChild(tr);
    });
  }

  async function handleDeleteExclusion(rule) {
    if (!confirm(`Remove exclusion rule for ${rule}?`)) return;

    try {
      const res = await fetch(`/api/exclusions?rule=${encodeURIComponent(rule)}`, { method: 'DELETE' });
      if (res.ok) {
        showToast(`Exclusion removed for ${rule}`, 'success');
        await Promise.all([fetchExclusions(), fetchHosts()]);
      }
    } catch (e) {
      showToast(`Failed to remove exclusion: ${e.message}`, 'error');
    }
  }

  // Host Detail Modal
  function openHostDetailModal(ip) {
    state.selectedHostIP = ip;
    const h = state.hosts.get(ip);
    if (!h) return;

    populateHostDetailModal(h);
    openModal(el.hostDetailModal);
  }

  function populateHostDetailModal(h) {
    el.detailHostIP.textContent = h.ip;
    el.detailHostAlias.textContent = h.alias ? `${h.alias} (${h.cidr || 'Static'})` : (h.cidr || 'Single Host');
    
    // Status color
    let dotColor = 'var(--color-up)';
    if (h.isExcluded) dotColor = 'var(--color-excluded)';
    else if (h.status === 'DOWN') dotColor = h.alertAcknowledged ? 'var(--color-ack)' : 'var(--color-down)';
    el.detailStatusDot.style.backgroundColor = dotColor;
    el.detailStatusDot.style.color = dotColor;

    el.detailCurrentRTT.textContent = (h.status === 'UP' && h.latencyMs > 0) ? `${h.latencyMs.toFixed(2)} ms` : (h.isExcluded ? 'Excluded' : 'Unreachable');
    el.detailMinRTT.textContent = h.minLatencyMs > 0 ? `${h.minLatencyMs.toFixed(2)} ms` : '--';
    el.detailAvgRTT.textContent = h.avgLatencyMs > 0 ? `${h.avgLatencyMs.toFixed(2)} ms` : '--';
    el.detailMaxRTT.textContent = h.maxLatencyMs > 0 ? `${h.maxLatencyMs.toFixed(2)} ms` : '--';
    el.detailLoss.textContent = `${h.packetLoss}%`;
    el.detailPackets.textContent = `${h.recvPackets} / ${h.sentPackets}`;
    el.detailLastSeen.textContent = h.lastSeen ? formatTimeAgo(new Date(h.lastSeen)) : 'Never';

    // Alert Section
    if (h.alertActive) {
      el.detailAlertSection.style.display = 'flex';
      el.detailAlertTitle.textContent = h.alertAcknowledged ? 'Acknowledged Outage' : 'Active Outage Incident';
      el.detailAlertBadge.textContent = h.alertAcknowledged ? 'ACKNOWLEDGED' : 'FIRING';
      el.detailAlertBadge.className = `badge ${h.alertAcknowledged ? 'pill-ack' : 'pill-down'}`;
      el.detailAlertMsg.textContent = `Host failed ${h.consecutiveFails} consecutive ICMP probes (${h.lastError || 'Timeout'}).`;

      if (h.alertAcknowledged) {
        el.detailAckInfo.style.display = 'block';
        el.detailAckInfo.innerHTML = `Acknowledged by <strong>${escapeHtml(h.alertAckBy || 'Operator')}</strong> (${formatTimeAgo(new Date(h.alertAckAt))}): ${escapeHtml(h.alertAckNote || 'No notes provided')}`;
        el.btnDetailAck.style.display = 'none';
      } else {
        el.detailAckInfo.style.display = 'none';
        el.btnDetailAck.style.display = 'inline-block';
      }
    } else {
      el.detailAlertSection.style.display = 'none';
    }

    // Exclude button
    el.detailExcludeText.textContent = h.isExcluded ? 'Include (Remove Exclusion)' : 'Exclude this Host';

    // Metadata form values
    el.inputHostAlias.value = h.alias || '';
    el.inputHostNotes.value = '';

    // Draw detail chart
    drawSparkline(el.detailSparklineCanvas, h.latencyHistory || [], h.status);
  }

  async function handleManualPing() {
    if (!state.selectedHostIP) return;
    el.btnDetailPingNow.disabled = true;
    el.btnDetailPingNow.textContent = 'Probing...';

    try {
      const res = await fetch(`/api/hosts/${state.selectedHostIP}/ping`, { method: 'POST' });
      const data = await res.json();
      if (data.Success) {
        showToast(`Probe successful: ${data.LatencyMs.toFixed(2)} ms`, 'success');
      } else {
        showToast(`Probe failed: ${data.Error}`, 'error');
      }
      await fetchHosts();
      const updated = state.hosts.get(state.selectedHostIP);
      if (updated) populateHostDetailModal(updated);
    } catch (e) {
      showToast(`Manual probe failed: ${e.message}`, 'error');
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

  async function handleToggleExclude() {
    if (!state.selectedHostIP) return;
    const h = state.hosts.get(state.selectedHostIP);
    if (!h) return;

    if (h.isExcluded) {
      await handleDeleteExclusion(h.ip);
    } else {
      try {
        const res = await fetch('/api/exclusions', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ rule: h.ip, reason: 'Manually excluded from host inspector' })
        });
        if (res.ok) {
          showToast(`Host ${h.ip} excluded`, 'info');
          await Promise.all([fetchExclusions(), fetchHosts()]);
        }
      } catch (e) {
        showToast(`Failed to exclude: ${e.message}`, 'error');
      }
    }

    const updated = state.hosts.get(state.selectedHostIP);
    if (updated) populateHostDetailModal(updated);
  }

  async function handleSaveHostMeta(e) {
    e.preventDefault();
    if (!state.selectedHostIP) return;
    const alias = el.inputHostAlias.value.trim();
    const notes = el.inputHostNotes.value.trim();

    try {
      const res = await fetch(`/api/hosts/${state.selectedHostIP}/meta`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ alias, notes })
      });
      if (res.ok) {
        showToast('Host metadata saved', 'success');
        await fetchHosts();
        const updated = state.hosts.get(state.selectedHostIP);
        if (updated) populateHostDetailModal(updated);
      }
    } catch (err) {
      showToast(`Failed to save alias: ${err.message}`, 'error');
    }
  }

  async function handleUnenrollHost() {
    if (!state.selectedHostIP) return;
    if (!confirm(`Un-enroll host ${state.selectedHostIP} from continuous monitoring? (It will be re-discovered if online during discovery scans).`)) return;

    try {
      const res = await fetch(`/api/hosts/${state.selectedHostIP}/enrollment`, { method: 'DELETE' });
      if (res.ok) {
        closeModal(el.hostDetailModal);
        showToast(`Host ${state.selectedHostIP} un-enrolled from monitoring`, 'info');
        await fetchHosts();
      }
    } catch (e) {
      showToast(`Failed to un-enroll host: ${e.message}`, 'error');
    }
  }

  // Settings Modal
  function openSettingsModal() {
    if (el.inputDiscoveryInterval) {
      el.inputDiscoveryInterval.value = state.settings.discoveryIntervalMin != null ? state.settings.discoveryIntervalMin : 240;
    }
    el.inputInterval.value = state.settings.intervalSec || 60.0;
    el.inputTimeout.value = state.settings.timeoutMs || 1000;
    el.inputFailThreshold.value = state.settings.failThreshold || 2;
    el.inputConcurrency.value = state.settings.concurrency || 100;
    openModal(el.settingsModal);
  }

  async function handleSaveSettings(e) {
    e.preventDefault();
    const payload = {
      discoveryIntervalMin: el.inputDiscoveryInterval ? parseInt(el.inputDiscoveryInterval.value, 10) : 15,
      autoDiscovery: true,
      intervalSec: parseFloat(el.inputInterval.value),
      timeoutMs: parseInt(el.inputTimeout.value, 10),
      failThreshold: parseInt(el.inputFailThreshold.value, 10),
      concurrency: parseInt(el.inputConcurrency.value, 10),
      soundAlerts: state.soundEnabled
    };

    try {
      const res = await fetch('/api/settings', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      if (res.ok) {
        state.settings = await res.json();
        closeModal(el.settingsModal);
        showToast('Settings saved and applied live', 'success');
      }
    } catch (err) {
      showToast(`Failed to update settings: ${err.message}`, 'error');
    }
  }

  // Helpers
  function openModal(modalEl) {
    modalEl.style.display = 'flex';
  }

  function closeModal(modalEl) {
    modalEl.style.display = 'none';
  }

  function showToast(message, type = 'info') {
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.textContent = message;
    el.toastContainer.appendChild(toast);
    setTimeout(() => {
      toast.style.opacity = '0';
      toast.style.transform = 'translateY(12px)';
      toast.style.transition = 'all 0.3s ease';
      setTimeout(() => toast.remove(), 300);
    }, 4000);
  }

  function formatTimeAgo(date) {
    const diff = Math.floor((new Date() - date) / 1000);
    if (diff < 5) return 'just now';
    if (diff < 60) return `${diff}s ago`;
    if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
    if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
    return `${Math.floor(diff / 86400)}d ago`;
  }

  function formatDuration(sec) {
    if (sec < 60) return `${sec}s`;
    if (sec < 3600) return `${Math.floor(sec / 60)}m ${sec % 60}s`;
    return `${Math.floor(sec / 3600)}h ${Math.floor((sec % 3600) / 60)}m`;
  }

  function formatPPS(pps) {
    if (!pps || pps <= 0) return '0 pkts/s';
    if (pps >= 100) return `${Math.round(pps)} pkts/s`;
    if (pps >= 10) return `${pps.toFixed(1)} pkts/s`;
    return `${pps.toFixed(1)} pkts/s`;
  }

  function formatPacedDelay(delayMs) {
    if (delayMs === undefined || delayMs === null || delayMs < 0) return 'Immediate';
    if (delayMs === 0) return 'Single target';
    if (delayMs >= 1000) {
      return `${(delayMs / 1000).toFixed(1)}s delay`;
    }
    if (delayMs >= 10) {
      return `${Math.round(delayMs)}ms delay`;
    }
    if (delayMs >= 1) {
      return `${delayMs.toFixed(1)}ms delay`;
    }
    return `${Math.round(delayMs * 1000)}µs delay`;
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

  // Run on DOM ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

})();
