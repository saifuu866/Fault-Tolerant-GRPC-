/* ==========================================================================
   FAULT-TOLERANT RPC DASHBOARD CLIENT LOGIC
   ========================================================================== */

const REFRESH_MS = 1500;

// Internal cache for events & filtering
let currentEvents = [];
let activeLogFilter = 'ALL';
let logSearchQuery = '';

/* -------------------------------- HTTP Helpers -------------------------------- */
async function api(url, options) {
  const res = await fetch(url, options);
  return res.json();
}

function post(url, body) {
  return api(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body || {})
  });
}

/* -------------------------------- Status Rendering -------------------------------- */
function renderStatus(d) {
  // Update metric counters
  document.getElementById('m-total').textContent     = Number(d.totals.requests).toLocaleString();
  document.getElementById('m-success').textContent   = Number(d.totals.success).toLocaleString();
  document.getElementById('m-failed').textContent    = Number(d.totals.failed).toLocaleString();
  document.getElementById('m-retries').textContent   = Number(d.totals.retries).toLocaleString();
  document.getElementById('m-failovers').textContent = Number(d.totals.failovers).toLocaleString();
  
  const leaderEl = document.getElementById('m-leader');
  if (d.leader) {
    leaderEl.innerHTML = `<span>${d.leader}</span>`;
  } else {
    leaderEl.textContent = 'NONE';
  }

  // Update header indicators
  document.getElementById('online-count').textContent =
    `(${d.online_count}/${d.server_count} online)`;
  
  const uptimeSec = Math.round(d.uptime_seconds);
  const mins = Math.floor(uptimeSec / 60);
  const secs = uptimeSec % 60;
  const timeStr = mins > 0 ? `${mins}m ${secs}s` : `${secs}s`;
  document.getElementById('clock').textContent = `uptime ${timeStr}`;

  /* --- Server Table --- */
  const tbody = document.querySelector('#server-table tbody');
  tbody.innerHTML = '';
  d.servers.forEach(s => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>
        <div class="server-id-cell">
          <strong>${s.server_id}</strong>
          ${s.is_leader ? '<span class="crown-badge">&#9733; LEADER</span>' : ''}
        </div>
      </td>
      <td><span class="address-code">${s.address}</span></td>
      <td>
        <span class="badge ${s.status}">
          <span class="pulse-dot" style="background-color: currentColor; width: 6px; height: 6px;"></span>
          ${s.status}
        </span>
      </td>
      <td><strong>${s.requests}</strong></td>
      <td style="color: var(--green-ok); font-weight: 600;">${s.successes}</td>
      <td style="color: ${s.failures > 0 ? 'var(--red-bad)' : 'inherit'}; font-weight: 600;">${s.failures}</td>
      <td><span class="latency-cell">${s.latency_ms} ms</span></td>
      <td>
        <div class="btn-group">
          <button class="btn btn-sm btn-danger" onclick="serverAction('${s.server_id}','kill')" title="Simulate process crash">
            Kill
          </button>
          <button class="btn btn-sm btn-success" onclick="serverAction('${s.server_id}','revive')" title="Revive crashed server">
            Revive
          </button>
          <select class="btn btn-xs btn-outline" onchange="setServerFailRate('${s.server_id}', this.value)" title="Inject transient failure rate probability">
            <option value="0">Glitch: 0%</option>
            <option value="0.3">Glitch: 30%</option>
            <option value="0.7">Glitch: 70%</option>
            <option value="1.0">Glitch: 100%</option>
          </select>
        </div>
      </td>`;
    if (s.last_error) {
      tr.title = `Last error: ${s.last_error}`;
    }
    tbody.appendChild(tr);
  });

  /* --- Distribution Bars --- */
  const dist = document.getElementById('dist');
  const total = Math.max(1, d.servers.reduce((acc, s) => acc + s.requests, 0));
  dist.innerHTML = d.servers.map(s => {
    const pct = Math.round((s.requests / total) * 100);
    return `
      <div class="bar-row">
        <div class="bar-labels">
          <span>${s.server_id} ${s.is_leader ? '(Leader)' : ''} &middot; ${s.requests} requests</span>
          <span>${pct}%</span>
        </div>
        <div class="bar-bg">
          <div class="bar-fill" style="width:${pct}%"></div>
        </div>
      </div>`;
  }).join('');

  /* --- Event Log --- */
  currentEvents = d.events || [];
  renderEventLog();
}

/* -------------------------------- Event Log Filtering -------------------------------- */
function setLogFilter(level) {
  activeLogFilter = level;
  document.querySelectorAll('.filter-pill').forEach(btn => {
    btn.classList.toggle('active', btn.textContent.toUpperCase() === level);
  });
  renderEventLog();
}

function filterEvents() {
  const searchInput = document.getElementById('event-search');
  logSearchQuery = (searchInput.value || '').toLowerCase().trim();
  renderEventLog();
}

function renderEventLog() {
  const container = document.getElementById('events');
  let filtered = currentEvents;

  if (activeLogFilter !== 'ALL') {
    filtered = filtered.filter(e => (e.level || '').toUpperCase() === activeLogFilter);
  }

  if (logSearchQuery) {
    filtered = filtered.filter(e =>
      (e.message || '').toLowerCase().includes(logSearchQuery) ||
      (e.time || '').toLowerCase().includes(logSearchQuery)
    );
  }

  if (filtered.length === 0) {
    container.innerHTML = '<p style="color: var(--text-muted); padding: 12px 0;">No matching log events recorded.</p>';
    return;
  }

  container.innerHTML = filtered.map(e => `
    <div class="log-entry">
      <span class="log-time">${e.time}</span>
      <span class="log-badge ${e.level}">${e.level}</span>
      <span class="log-msg">${escapeHtml(e.message)}</span>
    </div>
  `).join('');
}

function escapeHtml(str) {
  if (!str) return '';
  return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

/* -------------------------------- Polling Loop -------------------------------- */
async function refresh() {
  try {
    const data = await api('/api/status');
    renderStatus(data);
  } catch (err) {
    console.error('Status refresh failed:', err);
  }
}

/* -------------------------------- Actions & RPC -------------------------------- */
async function sendRequest() {
  const btn = document.getElementById('send-btn');
  btn.disabled = true;
  btn.innerHTML = `
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="spin">
      <circle cx="12" cy="12" r="10" stroke-opacity="0.25"></circle>
      <path d="M12 2a10 10 0 0 1 10 10"></path>
    </svg>
    Dispatching RPCs...`;

  try {
    const data = await post('/api/request', {
      operation: document.getElementById('operation').value,
      payload:   document.getElementById('payload').value,
      count:     parseInt(document.getElementById('count').value, 10) || 1
    });
    renderResults(data.results);
  } catch (err) {
    alert('Request failed: ' + err);
  } finally {
    btn.disabled = false;
    btn.innerHTML = `
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <line x1="22" y1="2" x2="11" y2="13"></line>
        <polygon points="22 2 15 22 11 13 2 9 22 2"></polygon>
      </svg>
      Send RPC Request(s)`;
    refresh();
  }
}

function renderResults(results) {
  const box = document.getElementById('results');
  if (!results || results.length === 0) {
    box.innerHTML = '<div class="empty-state"><p>No results returned.</p></div>';
    return;
  }

  box.innerHTML = results.map(r => `
    <div class="result-card ${r.success ? '' : 'fail'}">
      <div class="result-header">
        <div class="result-title">
          #${r.request_id} &middot; <code>${r.operation}</code> &rarr;
          <strong style="color: ${r.success ? 'var(--green-ok)' : 'var(--red-bad)'};">
            ${r.success ? 'SUCCESS on ' + r.server_id : 'FAILED (' + (r.error_type || 'ERROR') + ')'}
          </strong>
        </div>
        <span class="result-time-badge">${r.total_ms} ms</span>
      </div>

      <div class="result-body">
        <strong>Return:</strong> ${escapeHtml(r.success ? String(r.result) : String(r.message))}
      </div>

      <div class="attempt-timeline">
        ${r.attempts.map(a => `
          <div class="attempt-row">
            <div>
              <strong>Attempt #${a.attempt}:</strong>
              Target: <code>${a.server_id || 'None (All down)'}</code>
              &mdash; <span class="attempt-status ${a.outcome}">[${a.outcome}]</span>
              <span style="color: var(--text-muted); font-size: 13px;">${escapeHtml(a.message || a.code || '')}</span>
            </div>
            <span style="color: var(--text-muted); font-family: var(--font-mono); font-size: 12px;">${a.duration_ms} ms</span>
          </div>
        `).join('')}
      </div>
    </div>
  `).join('');
}

function clearResults() {
  document.getElementById('results').innerHTML = `
    <div class="empty-state">
      <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="#94a3b8" stroke-width="1.5">
        <circle cx="12" cy="12" r="10"></circle>
        <line x1="12" y1="8" x2="12" y2="12"></line>
        <line x1="12" y1="16" x2="12.01" y2="16"></line>
      </svg>
      <p>Execution trace cleared.</p>
      <span>Submit a new RPC request to view live attempt timelines.</span>
    </div>`;
}

/* -------------------------------- Server & Leader Controls -------------------------------- */
async function serverAction(id, action) {
  const res = await post(`/api/servers/${id}/${action}`);
  if (!res.ok) alert(`${action} ${id}: ${res.message}`);
  refresh();
}

async function setServerFailRate(id, rate) {
  const res = await post(`/api/servers/${id}/fail_rate`, { rate: parseFloat(rate) });
  if (!res.ok) alert(`Set fail rate error: ${res.message}`);
  refresh();
}

async function killLeader() {
  const res = await post('/api/leader/kill');
  if (!res.ok) alert(res.message);
  refresh();
}

async function forceElection() {
  await post('/api/leader/elect');
  refresh();
}

async function resetMetrics() {
  await post('/api/reset');
  clearResults();
  refresh();
}

/* -------------------------------- Presets & Scenarios -------------------------------- */
function applyPreset(op, payload, count) {
  document.getElementById('operation').value = op;
  document.getElementById('payload').value = payload;
  document.getElementById('count').value = count;
  updatePayloadHint(op);
}

function updatePayloadHint(op) {
  const hints = {
    echo: 'Any text string (e.g. "hello")',
    sum: 'Comma-separated integers (e.g. "10,20,30")',
    square: 'Single integer (e.g. "7")',
    slow: 'Seconds to sleep (e.g. "1")',
    flaky: 'Number of transient glitches before success (e.g. "1")',
    boom: 'No payload needed (triggers instant permanent error)'
  };
  const el = document.getElementById('payload-hint');
  if (el) el.textContent = hints[op] || '';
}

function switchTab(tabId) {
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.classList.remove('active');
  });
  document.querySelectorAll('.tab-pane').forEach(pane => {
    pane.classList.remove('active');
  });

  const activeBtn = document.querySelector(`.tab-btn[onclick*="${tabId}"]`);
  if (activeBtn) activeBtn.classList.add('active');

  const targetPane = document.getElementById(tabId);
  if (targetPane) targetPane.classList.add('active');
}

async function runScenario(scenarioNum) {
  switchTab('tab-live');
  window.scrollTo({ top: 0, behavior: 'smooth' });

  switch (scenarioNum) {
    case 1:
      applyPreset('echo', 'hello round-robin', 6);
      setTimeout(sendRequest, 300);
      break;
    case 2:
      await serverAction('server-1', 'kill');
      applyPreset('echo', 'failover test', 4);
      setTimeout(sendRequest, 400);
      break;
    case 3:
      applyPreset('flaky', '1', 1);
      setTimeout(sendRequest, 300);
      break;
    case 4:
      applyPreset('boom', '', 1);
      setTimeout(sendRequest, 300);
      break;
    case 5:
      await serverAction('server-1', 'revive');
      await serverAction('server-2', 'revive');
      await serverAction('server-3', 'revive');
      break;
    case 6:
      await killLeader();
      break;
  }
}

/* Auto-fill a sensible payload when the operation changes */
document.getElementById('operation').addEventListener('change', e => {
  const defaults = {
    echo: 'hello',
    sum: '1,2,3',
    square: '7',
    slow: '1',
    flaky: '1',
    boom: ''
  };
  document.getElementById('payload').value = defaults[e.target.value] ?? '';
  updatePayloadHint(e.target.value);
});

// Initial boot
updatePayloadHint(document.getElementById('operation').value);
refresh();
setInterval(refresh, REFRESH_MS);
