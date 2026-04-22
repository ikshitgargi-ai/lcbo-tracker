/* ===== Anu Spirits LCBO Tracker Pro — Frontend Engine ===== */

let currentPage = 1;
let activeRepId = null;
let currentModalStoreId = null;
let searchTimeout = null;
let selectedProducers = [];
let selectedActivities = [];
let selectedVenue = '';

// === INIT ===
let autoRefreshInterval = null;

document.addEventListener('DOMContentLoaded', async () => {
  await loadReps();
  showView('dashboard');
  startLiveClock();
  // Auto-refresh dashboard every 30 seconds for live data sync
  autoRefreshInterval = setInterval(() => {
    const activeView = document.querySelector('.view.active');
    if (activeView && activeView.id === 'view-dashboard') loadDashboard();
  }, 30000);
});

// === LIVE CLOCK ===
function startLiveClock() {
  const el = document.getElementById('liveClock');
  if (!el) return;
  function tick() {
    const now = new Date();
    el.textContent = now.toLocaleDateString('en-CA', { month: 'short', day: 'numeric' }) +
      ' ' + now.toLocaleTimeString('en-CA', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  }
  tick();
  setInterval(tick, 1000);
}

// === NAVIGATION ===
function showView(view) {
  document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
  document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
  const viewEl = document.getElementById('view-' + view);
  const navEl = document.querySelector(`[data-view="${view}"]`);
  if (viewEl) viewEl.classList.add('active');
  if (navEl) navEl.classList.add('active');

  if (view === 'dashboard') loadDashboard();
  if (view === 'stores') { loadCities(); loadStores(); }
  if (view === 'log') {}
  if (view === 'inventory') loadInventory();
  if (view === 'routes') loadRoutes();
  if (view === 'opportunities') { loadOpportunities(); loadDailyPlan(); }
  if (view === 'followups') loadFollowups();
  if (view === 'gap') { loadTrackedProductOptions(); loadListingStatus(); loadGapReport(); }
  if (view === 'reorder') { loadTrackedProductOptions(); loadReorderReport(); }
  if (view === 'sod') { loadSodStatus(); loadSodProducts(); loadSodListingChanges(); }
  if (view === 'reports') { loadReport('daily'); }
  if (view === 'crm') { loadCrmDashboard(); }
  if (view === 'map') { initMap(); }
  if (view === 'oosrisk') { loadCrmFilterOptions(); loadOos(); }
  if (view === 'opps') { loadCrmFilterOptions(); loadOpportunitiesFull(); }
  if (view === 'goals') { loadGoals(); }
  if (view === 'horeca') { loadCrmFilterOptions(); loadHoreca(); }
  if (view === 'territories') { loadTerritories(); }
}

// === LIVE INVENTORY / GAP / REORDER ===
let _gapDebounce = null, _reorderDebounce = null;
function debounceGapReload() { clearTimeout(_gapDebounce); _gapDebounce = setTimeout(loadGapReport, 350); }
function debounceReorderReload() { clearTimeout(_reorderDebounce); _reorderDebounce = setTimeout(loadReorderReport, 350); }

async function loadTrackedProductOptions() {
  try {
    const res = await fetch('/api/products');
    const products = await res.json();
    const opts = ['<option value="">All Products</option>'].concat(
      products.filter(p => p.lcbo_sku).map(p => `<option value="${esc(p.lcbo_sku)}">${esc(p.brand||'')} — ${esc(p.name)} (${esc(p.lcbo_sku)})</option>`)
    ).join('');
    ['gapSkuFilter','reorderSkuFilter'].forEach(id => { const el = document.getElementById(id); if (el) el.innerHTML = opts; });
  } catch(e) { console.warn('loadTrackedProductOptions', e); }
}

async function refreshLiveInventoryAll() {
  const btn = event && event.target;
  if (btn) { btn.disabled = true; btn.textContent = '⏳ Refreshing from LCBO.com...'; }
  try {
    const res = await fetch('/api/inventory/refresh-all', {method:'POST'});
    const data = await res.json();
    alert(`Refreshed ${data.refreshed}/${data.total_tracked} products. Live data from ${data.source||'lcbo.com'}.`);
    loadListingStatus();
    loadGapReport();
  } catch(e) {
    alert('Refresh failed: ' + e.message);
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = '🔄 Refresh Live Inventory (All Products)'; }
  }
}

async function loadListingStatus() {
  try {
    const res = await fetch('/api/inventory/listing-status');
    const data = await res.json();
    const bar = document.getElementById('listingStatusBar');
    if (!bar) return;
    const statusColor = code => ({1:'#00b894',2:'#00b894',3:'#fdcb6e',4:'#e17055',5:'#d63031'})[code] || '#636e72';
    bar.innerHTML = (data.products||[]).map(p => `
      <div class="ls-card" style="border-left:4px solid ${statusColor(p.listing_status_code)}">
        <div class="ls-name">${esc(p.name)}</div>
        <div class="ls-meta"><span class="ls-sku">SKU ${esc(p.sku||'')}</span> · <span>${esc(p.price||'')}</span></div>
        <div class="ls-status" style="color:${statusColor(p.listing_status_code)}">${esc(p.listing_status_label)}</div>
        <div class="ls-stats"><b>${p.store_count}</b> stores · <b>${p.total_units}</b> units</div>
      </div>
    `).join('');
  } catch(e) { console.warn('loadListingStatus', e); }
}

async function loadGapReport() {
  const sku = document.getElementById('gapSkuFilter')?.value || '';
  const city = document.getElementById('gapCityFilter')?.value || '';
  const container = document.getElementById('gapReportContent');
  if (!container) return;
  container.innerHTML = '<p class="muted">Loading gap report...</p>';
  try {
    const qs = new URLSearchParams();
    if (sku) qs.set('sku', sku);
    if (city) qs.set('city', city);
    const res = await fetch('/api/inventory/gap-report?' + qs.toString());
    const data = await res.json();
    if (!data.products || !data.products.length) {
      container.innerHTML = '<p class="muted">No products with live inventory. Click Refresh above.</p>';
      return;
    }
    container.innerHTML = data.products.map(p => `
      <div class="card gap-card">
        <div class="gap-header">
          <div>
            <h3>${esc(p.product.brand||'')} — ${esc(p.product.name)}</h3>
            <div class="muted">SKU ${esc(p.product.sku)} · ${esc(p.product.price||'')}</div>
          </div>
          <div class="gap-metrics">
            <div class="gap-metric"><b>${p.carrying_count}</b><span>carrying</span></div>
            <div class="gap-metric gap-bad"><b>${p.gap_count}</b><span>GAP (no stock)</span></div>
            <div class="gap-metric"><b>${p.gap_rate_pct}%</b><span>gap rate</span></div>
          </div>
        </div>
        <details>
          <summary>Show top ${Math.min(p.gap_stores.length, 50)} gap stores</summary>
          <table class="data-table">
            <thead><tr><th>Store #</th><th>Account</th><th>City</th><th>Address</th><th>Manager</th><th>Phone</th></tr></thead>
            <tbody>
              ${p.gap_stores.slice(0, 50).map(s => `<tr>
                <td>${esc(s.store_number||'')}</td>
                <td>${esc(s.account||'')}</td>
                <td>${esc(s.city||'')}</td>
                <td>${esc(s.address||'')}</td>
                <td>${esc(s.manager_name||'')}</td>
                <td>${esc(s.phone||'')}</td>
              </tr>`).join('')}
            </tbody>
          </table>
        </details>
      </div>
    `).join('');
  } catch(e) {
    container.innerHTML = `<p class="muted">Error loading gap report: ${esc(e.message||'')}</p>`;
  }
}

async function loadReorderReport() {
  const threshold = document.getElementById('reorderThreshold')?.value || 5;
  const sku = document.getElementById('reorderSkuFilter')?.value || '';
  const city = document.getElementById('reorderCityFilter')?.value || '';
  try {
    const qs = new URLSearchParams({threshold});
    if (sku) qs.set('sku', sku);
    if (city) qs.set('city', city);
    const res = await fetch('/api/inventory/reorder-needed?' + qs.toString());
    const data = await res.json();
    const stats = document.getElementById('reorderStats');
    if (stats) stats.innerHTML = `
      <div class="stat-card red"><div class="label">Critical (0 units)</div><div class="value">${data.critical_count||0}</div></div>
      <div class="stat-card orange"><div class="label">High (≤ 2 units)</div><div class="value">${data.high_count||0}</div></div>
      <div class="stat-card" style="border-left:3px solid #fdcb6e"><div class="label">Medium (< ${threshold})</div><div class="value" style="color:#fdcb6e">${data.medium_count||0}</div></div>
      <div class="stat-card accent"><div class="label">Total Alerts</div><div class="value">${data.total_reorder_alerts||0}</div></div>
    `;
    const tbody = document.querySelector('#reorderTable tbody');
    const cnt = document.getElementById('reorderCount');
    if (cnt) cnt.textContent = data.total_reorder_alerts || 0;
    if (!tbody) return;
    const urgencyBadge = u => `<span class="urgency urgency-${u}">${u.toUpperCase()}</span>`;
    tbody.innerHTML = (data.alerts||[]).map(a => `
      <tr>
        <td>${urgencyBadge(a.urgency)}</td>
        <td>${esc(a.store_number||'')}</td>
        <td>${esc(a.store_name||'')}</td>
        <td>${esc(a.city||'')}</td>
        <td>${esc((a.product&&a.product.name)||'')}<br><span class="muted">SKU ${esc((a.product&&a.product.sku)||'')}</span></td>
        <td><b>${a.quantity}</b></td>
        <td>${esc(a.manager||'')}</td>
        <td>${esc(a.phone||'')}</td>
      </tr>
    `).join('') || '<tr><td colspan="8" class="muted">No low-stock alerts. Click "Refresh Live Inventory" in Gap Report first.</td></tr>';
  } catch(e) {
    console.warn('loadReorderReport', e);
  }
}

// === REPS ===
async function loadReps() {
  const res = await fetch('/api/reps');
  const reps = await res.json();
  const sel = document.getElementById('activeRep');
  sel.innerHTML = reps.map(r => `<option value="${r.id}">${r.name}</option>`).join('');
  activeRepId = reps.length ? reps[0].id : null;
}

function setActiveRep() {
  activeRepId = parseInt(document.getElementById('activeRep').value);
}

// === DASHBOARD ===
async function loadDashboard() {
  const res = await fetch('/api/dashboard');
  const d = await res.json();
  const types = d.by_type || {};
  const prods = d.by_producer || {};
  const reps = d.by_rep || {};

  document.getElementById('statsGrid').innerHTML = `
    <div class="stat-card accent"><div class="label">Total Stores</div><div class="value">${d.total_stores}</div></div>
    <div class="stat-card green"><div class="label">Activities Logged</div><div class="value">${d.total_activities}</div></div>
    <div class="stat-card blue"><div class="label">Active Stores</div><div class="value">${d.active_stores || 0}</div></div>
    <div class="stat-card orange"><div class="label">This Week</div><div class="value">${d.week_activities || 0}</div></div>
    <div class="stat-card" style="border-left:3px solid #e17055"><div class="label">NB Distillers</div><div class="value" style="color:#e17055">${prods['NB Distillers'] || 0}</div></div>
    <div class="stat-card" style="border-left:3px solid #00cec9"><div class="label">Anu Portfolio</div><div class="value" style="color:#00cec9">${prods['Anu Portfolio'] || 0}</div></div>
    <div class="stat-card red"><div class="label">Overdue Follow-Ups</div><div class="value">${d.overdue_followups || 0}</div></div>
    <div class="stat-card" style="border-left:3px solid var(--accent)"><div class="label">Untouched Stores</div><div class="value" style="color:var(--accent-light)">${d.total_stores - (d.active_stores || 0)}</div></div>
  `;

  // Rep stats
  const repDiv = document.getElementById('repStats');
  let repHtml = '';
  for (const [name, count] of Object.entries(reps)) {
    const pct = d.total_activities ? Math.round((count / d.total_activities) * 100) : 0;
    repHtml += `<div class="rep-bar"><div class="rep-name">${esc(name)}</div><div class="progress-bar"><div class="progress-fill" style="width:${pct}%"></div></div><div class="rep-count">${count} (${pct}%)</div></div>`;
  }
  repDiv.innerHTML = repHtml || '<p class="muted">No data yet</p>';

  // Recent count badge
  const countEl = document.getElementById('recentCount');
  if (countEl) countEl.textContent = d.total_activities;

  // Recent table
  const tbody = document.querySelector('#recentTable tbody');
  if (!d.recent.length) {
    tbody.innerHTML = '<tr><td colspan="6" class="muted">No activity yet. Start logging!</td></tr>';
    return;
  }
  tbody.innerHTML = d.recent.map(a => `
    <tr onclick="openStoreModal(${a.store_id})">
      <td class="nowrap">${formatDate(a.created_at)}</td>
      <td>${esc(a.rep_name)}</td>
      <td><strong>#${a.store_number}</strong> ${esc(truncate(a.account, 20))}</td>
      <td>${producerTags(a.producer)}</td>
      <td><span class="type-badge ${a.activity_type}">${formatType(a.activity_type)}</span></td>
      <td class="notes-cell">${esc(truncate(a.notes, 40))}</td>
    </tr>
  `).join('');

  // Update sync indicator
  const liveEl = document.getElementById('dashLive');
  if (liveEl) liveEl.title = 'Last synced: ' + new Date().toLocaleTimeString();
}

// === STORES ===
async function loadCities() {
  const res = await fetch('/api/cities');
  const cities = await res.json();
  const sel = document.getElementById('cityFilter');
  sel.innerHTML = '<option value="">All Cities</option>' + cities.map(c => `<option value="${c}">${c}</option>`).join('');
}

function debounceSearch() {
  clearTimeout(searchTimeout);
  searchTimeout = setTimeout(() => { currentPage = 1; loadStores(); }, 300);
}

async function loadStores() {
  const search = document.getElementById('storeSearch').value;
  const city = document.getElementById('cityFilter').value;
  const producer = document.getElementById('producerFilter') ? document.getElementById('producerFilter').value : '';
  const res = await fetch(`/api/stores?search=${encodeURIComponent(search)}&city=${encodeURIComponent(city)}&producer=${encodeURIComponent(producer)}&page=${currentPage}&per_page=50`);
  const data = await res.json();

  const countEl = document.getElementById('storeCount');
  if (countEl) countEl.textContent = `(${data.total})`;

  const tbody = document.querySelector('#storesTable tbody');
  tbody.innerHTML = data.stores.map(s => `
    <tr onclick="openStoreModal(${s.id})">
      <td><strong>${s.store_number}</strong></td>
      <td>${esc(truncate(s.account, 25))}</td>
      <td>${esc(s.city)}</td>
      <td>${esc(s.manager_name || '—')}</td>
      <td>${esc(s.manager_phone || s.phone || '—')}</td>
      <td>${statusBadge(s.status)}</td>
      <td>${producerTags(s.producer)}</td>
      <td><button class="btn-sm" onclick="event.stopPropagation();openStoreModal(${s.id})">View</button></td>
    </tr>
  `).join('');
  renderPagination(data.page, data.pages);
}

function renderPagination(page, pages) {
  const div = document.getElementById('pagination');
  if (pages <= 1) { div.innerHTML = ''; return; }
  let btns = [];
  if (page > 1) btns.push(`<button onclick="goPage(${page-1})">&laquo;</button>`);
  let start = Math.max(1, page - 3), end = Math.min(pages, page + 3);
  for (let i = start; i <= end; i++) {
    btns.push(`<button class="${i === page ? 'active' : ''}" onclick="goPage(${i})">${i}</button>`);
  }
  if (page < pages) btns.push(`<button onclick="goPage(${page+1})">&raquo;</button>`);
  div.innerHTML = btns.join('');
}

function goPage(p) { currentPage = p; loadStores(); }

// === STORE MODAL ===
async function openStoreModal(storeId) {
  currentModalStoreId = storeId;
  const res = await fetch(`/api/stores/${storeId}`);
  const s = await res.json();

  document.getElementById('modalTitle').textContent = `${s.account} — Store #${s.store_number}`;
  document.getElementById('editStoreId').value = s.id;
  document.getElementById('editStoreNum').value = s.store_number;
  document.getElementById('editAccount').value = s.account || '';
  document.getElementById('editAddress').value = s.address || '';
  document.getElementById('editCity').value = s.city || '';
  document.getElementById('editPostal').value = s.postal || '';
  document.getElementById('editPhone').value = s.phone || '';
  document.getElementById('editEmail').value = s.email || '';
  document.getElementById('editContacts').value = s.contacts || '';
  document.getElementById('editPriority').value = s.priority || 'Standard';
  document.getElementById('editStatus').value = s.status || '';
  document.getElementById('editRep').value = s.rep || '';
  document.getElementById('editProducer').value = s.producer || '';
  document.getElementById('editManagerName').value = s.manager_name || '';
  document.getElementById('editAsstManager').value = s.asst_manager_name || '';
  document.getElementById('editManagerPhone').value = s.manager_phone || '';
  document.getElementById('editStoreEmail').value = s.store_email || '';

  switchTab('snapshot', document.querySelector('.tab-btn'));
  document.getElementById('storeModal').style.display = 'flex';
  loadSnapshot(storeId);
}

async function loadSnapshot(storeId) {
  const div = document.getElementById('snapshotContent');
  div.innerHTML = '<p class="muted">Loading...</p>';

  const res = await fetch(`/api/stores/${storeId}/snapshot`);
  const snap = await res.json();
  const s = snap.store;
  const summary = snap.summary || {};

  let html = '';

  // Quick stats
  html += '<div class="snapshot-stats">';
  const types = ['tasting', 'site_visit', 'listing', 'email', 'call'];
  for (const t of types) {
    const data = summary[t];
    html += `<div class="ss-stat"><div class="ss-val">${data ? data.count : 0}</div><div class="ss-label">${formatType(t)}</div>${data ? `<div class="ss-date">${formatDateShort(data.last_date)}</div>` : ''}</div>`;
  }
  html += '</div>';

  // Last conversation
  if (snap.last_note) {
    html += `<div class="snapshot-section">
      <h4>Last Conversation</h4>
      <div class="last-note-card">
        <div class="ln-meta">${esc(snap.last_note.rep_name)} · ${formatType(snap.last_note.activity_type)} · ${formatDate(snap.last_note.created_at)}</div>
        <div class="ln-text">${esc(snap.last_note.notes)}</div>
      </div>
    </div>`;
  }

  // Store info summary
  html += `<div class="snapshot-section">
    <h4>Account Details</h4>
    <div class="snap-grid">
      <div><span class="snap-label">Manager:</span> ${esc(s.manager_name || 'Not set')}</div>
      <div><span class="snap-label">Phone:</span> ${esc(s.manager_phone || s.phone || 'Not set')}</div>
      <div><span class="snap-label">Email:</span> ${esc(s.store_email || s.email || 'Not set')}</div>
      <div><span class="snap-label">Producer:</span> ${producerTags(s.producer) || 'Not assigned'}</div>
      <div><span class="snap-label">Status:</span> ${statusBadge(s.status) || 'Not set'}</div>
      <div><span class="snap-label">Priority:</span> ${esc(s.priority)}</div>
      <div><span class="snap-label">Address:</span> ${esc(s.address)}, ${esc(s.city)} ${esc(s.postal)}</div>
      <div><span class="snap-label">Total Activities:</span> <strong>${snap.total_activities}</strong></div>
    </div>
  </div>`;

  // First contact
  if (snap.first_contact) {
    html += `<div class="snapshot-section">
      <h4>First Contact</h4>
      <div class="muted">${formatDate(snap.first_contact.created_at)} by ${esc(snap.first_contact.rep_name)} — ${formatType(snap.first_contact.activity_type)}</div>
    </div>`;
  }

  // Upcoming follow-ups
  if (snap.followups.length) {
    const today = new Date().toISOString().split('T')[0];
    html += `<div class="snapshot-section"><h4>Follow-Ups</h4>`;
    for (const f of snap.followups.slice(0, 5)) {
      const overdue = f.follow_up_date < today;
      html += `<div class="mini-followup ${overdue ? 'overdue' : ''}">
        <span>${f.follow_up_date} ${overdue ? '(OVERDUE)' : ''}</span>
        <span class="muted">${formatType(f.activity_type)} ${f.producer ? '· ' + esc(f.producer) : ''}</span>
      </div>`;
    }
    html += '</div>';
  }

  // Google Maps link
  if (s.lat && s.lng && s.lat !== 0) {
    html += `<div class="snapshot-section">
      <a href="https://www.google.com/maps/dir/43.6558,-79.3628/${s.lat},${s.lng}" target="_blank" class="btn-primary" style="display:inline-block;text-decoration:none;font-size:13px;padding:8px 16px">&#128506; Navigate from Home Base</a>
    </div>`;
  }

  div.innerHTML = html;
}

function closeModal() {
  document.getElementById('storeModal').style.display = 'none';
  currentModalStoreId = null;
}

document.addEventListener('keydown', e => { if (e.key === 'Escape') closeModal(); });

async function saveStore() {
  const id = document.getElementById('editStoreId').value;
  const data = {
    account: document.getElementById('editAccount').value,
    address: document.getElementById('editAddress').value,
    city: document.getElementById('editCity').value,
    postal: document.getElementById('editPostal').value,
    phone: document.getElementById('editPhone').value,
    email: document.getElementById('editEmail').value,
    contacts: document.getElementById('editContacts').value,
    priority: document.getElementById('editPriority').value,
    status: document.getElementById('editStatus').value,
    rep: document.getElementById('editRep').value,
    producer: document.getElementById('editProducer').value,
    manager_name: document.getElementById('editManagerName').value,
    asst_manager_name: document.getElementById('editAsstManager').value,
    manager_phone: document.getElementById('editManagerPhone').value,
    store_email: document.getElementById('editStoreEmail').value,
  };
  await fetch(`/api/stores/${id}`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) });
  document.querySelectorAll('.success-msg').forEach(m => { m.style.display = 'inline'; setTimeout(() => m.style.display = 'none', 2000); });
  loadStores();
}

function switchTab(tab, el) {
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('tab-' + tab).classList.add('active');
  el.classList.add('active');
  if (tab === 'activities' && currentModalStoreId) loadModalActivities(currentModalStoreId);
  if (tab === 'snapshot' && currentModalStoreId) loadSnapshot(currentModalStoreId);
}

async function loadModalActivities(storeId, type = '') {
  const url = `/api/activities/${storeId}` + (type ? `?type=${type}` : '');
  const res = await fetch(url);
  const acts = await res.json();
  const div = document.getElementById('modalActivities');
  if (!acts.length) { div.innerHTML = '<p class="muted">No activities recorded yet.</p>'; return; }
  div.innerHTML = acts.map(a => `
    <div class="activity-card">
      <div class="ac-header">
        <span class="type-badge ${a.activity_type}">${formatType(a.activity_type)}</span>
        <span class="ac-date">${formatDate(a.created_at)}</span>
      </div>
      <div class="ac-rep">by ${esc(a.rep_name)} ${producerTags(a.producer)} ${a.venue_type ? `<span class="venue-tag">${esc(a.venue_type)}</span>` : ''}</div>
      ${a.notes ? `<div class="ac-notes">${esc(a.notes)}</div>` : ''}
      ${a.follow_up_date ? `<div class="ac-followup">Follow-up: ${a.follow_up_date}</div>` : ''}
    </div>
  `).join('');
}

function filterActivities(type, el) {
  document.querySelectorAll('.filter-pill').forEach(p => p.classList.remove('active'));
  el.classList.add('active');
  if (currentModalStoreId) loadModalActivities(currentModalStoreId, type);
}

// === LOG ACTIVITY ===
function toggleChip(el, group) {
  el.classList.toggle('active');
  if (group === 'producer') {
    selectedProducers = [...document.querySelectorAll('#producerChips .chip.active')].map(c => c.dataset.val);
  } else if (group === 'activity') {
    selectedActivities = [...document.querySelectorAll('#activityChips .chip.active')].map(c => c.dataset.val);
  } else if (group === 'venue') {
    // Single select for venue
    document.querySelectorAll('#venueChips .chip').forEach(c => { if (c !== el) c.classList.remove('active'); });
    selectedVenue = el.classList.contains('active') ? el.dataset.val : '';
  }
}

async function searchLogStore() {
  const q = document.getElementById('logStoreSearch').value.trim();
  const dropdown = document.getElementById('logStoreResults');
  if (q.length < 1) { dropdown.classList.remove('show'); return; }
  const res = await fetch(`/api/stores?search=${encodeURIComponent(q)}&per_page=10`);
  const data = await res.json();
  if (!data.stores.length) {
    dropdown.innerHTML = '<div class="dropdown-item muted">No stores found</div>';
    dropdown.classList.add('show');
    return;
  }
  dropdown.innerHTML = data.stores.map(s => `
    <div class="dropdown-item" onclick="selectLogStore(${s.id}, ${s.store_number}, '${escAttr(s.account)}', '${escAttr(s.address)}', '${escAttr(s.city)}')">
      <span class="store-num">#${s.store_number}</span> ${esc(s.account)}
      <div class="store-addr">${esc(s.address)}, ${esc(s.city)}</div>
    </div>
  `).join('');
  dropdown.classList.add('show');
}

function selectLogStore(id, num, account, address, city) {
  document.getElementById('logStoreId').value = id;
  document.getElementById('logStoreSearch').value = `#${num} — ${account}`;
  document.getElementById('logStoreResults').classList.remove('show');
  const info = document.getElementById('selectedStoreInfo');
  info.innerHTML = `<strong>Store #${num}</strong> — ${account}<br>${address}, ${city}`;
  info.classList.add('show');
  loadStoreHistoryPanel(id);
}

async function loadStoreHistoryPanel(storeId) {
  const res = await fetch(`/api/stores/${storeId}/snapshot`);
  const snap = await res.json();
  const div = document.getElementById('storeHistory');
  const summary = snap.summary || {};

  let html = '<div class="snapshot-stats">';
  for (const t of ['tasting', 'site_visit', 'listing', 'email', 'call']) {
    const s = summary[t];
    html += `<div class="ss-stat"><div class="ss-val">${s ? s.count : 0}</div><div class="ss-label">${formatType(t)}</div></div>`;
  }
  html += '</div>';

  if (snap.last_note) {
    html += `<div class="last-note-card">
      <div class="ln-meta">${esc(snap.last_note.rep_name)} · ${formatDate(snap.last_note.created_at)}</div>
      <div class="ln-text">${esc(snap.last_note.notes)}</div>
    </div>`;
  }

  const acts = snap.activities || [];
  if (acts.length) {
    html += '<h4 style="margin:16px 0 8px;font-size:13px">Recent Activity</h4>';
    html += acts.slice(0, 8).map(a => `
      <div class="activity-card compact">
        <div class="ac-header"><span class="type-badge ${a.activity_type}">${formatType(a.activity_type)}</span><span class="ac-date">${formatDate(a.created_at)}</span></div>
        ${a.notes ? `<div class="ac-notes">${esc(truncate(a.notes, 80))}</div>` : ''}
      </div>
    `).join('');
  }

  div.innerHTML = html;
}

async function submitActivity() {
  const storeId = document.getElementById('logStoreId').value;
  const notes = document.getElementById('logNotes').value.trim();
  if (!storeId) { alert('Please select a store'); return; }
  if (!selectedActivities.length) { alert('Please select at least one activity type'); return; }
  if (!activeRepId) { alert('Please select a rep'); return; }

  const followUpDate = document.getElementById('logFollowUpDate').value || '';
  const producerStr = selectedProducers.join(', ');

  // Log one entry per activity type (supports multi-select)
  for (const actType of selectedActivities) {
    await fetch('/api/activities', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        store_id: parseInt(storeId),
        rep_id: activeRepId,
        activity_type: actType,
        producer: producerStr,
        venue_type: selectedVenue,
        notes: notes,
        follow_up_date: followUpDate
      })
    });
  }

  const msg = document.getElementById('logSuccess');
  msg.style.display = 'inline';
  setTimeout(() => msg.style.display = 'none', 3000);

  // Reset form
  document.getElementById('logNotes').value = '';
  document.getElementById('logFollowUpDate').value = '';
  document.querySelectorAll('#activityChips .chip').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('#venueChips .chip').forEach(b => b.classList.remove('active'));
  selectedActivities = [];
  selectedVenue = '';
  loadStoreHistoryPanel(storeId);
}

document.addEventListener('click', e => {
  if (!e.target.closest('.form-group')) {
    const dd = document.getElementById('logStoreResults');
    if (dd) dd.classList.remove('show');
  }
});

// === INVENTORY ===
async function loadInventory() {
  const res = await fetch('/api/products');
  const products = await res.json();
  const grid = document.getElementById('inventoryGrid');

  if (!products.length) {
    grid.innerHTML = '<p class="muted">No products being tracked.</p>';
    return;
  }

  grid.innerHTML = products.map(p => `
    <div class="inventory-card ${p.brand === 'NB Distillers' ? 'inv-nb' : 'inv-anu'}">
      <div class="inv-header">
        <div>
          <div class="inv-brand">${esc(p.brand)}</div>
          <div class="inv-name">${esc(p.name)}</div>
        </div>
        <div class="inv-cat">${esc(p.category)}</div>
      </div>
      <div class="inv-details">
        <div class="inv-stat"><span class="inv-val">${p.lcbo_sku || '—'}</span><span class="inv-label">SKU</span></div>
        <div class="inv-stat"><span class="inv-val">${p.price || '—'}</span><span class="inv-label">Price</span></div>
        <div class="inv-stat"><span class="inv-val">${p.stores_stocked}</span><span class="inv-label">Stores</span></div>
        <div class="inv-stat"><span class="inv-val">${p.total_inventory}</span><span class="inv-label">Units</span></div>
      </div>
      <div class="inv-actions">
        ${p.lcbo_sku ? `<button class="btn-sm" onclick="checkInventory('${p.lcbo_sku}', this)">Check Stock</button>` : '<span class="muted">No SKU — Not yet listed</span>'}
        ${p.lcbo_url ? `<a href="${p.lcbo_url}" target="_blank" class="btn-sm">View on LCBO.com</a>` : ''}
      </div>
      <div class="inv-stores" id="inv-stores-${p.lcbo_sku || p.id}" style="display:none"></div>
      ${p.last_checked ? `<div class="inv-checked">Last checked: ${formatDate(p.last_checked)}</div>` : ''}
    </div>
  `).join('');
}

async function checkInventory(sku, btn) {
  btn.textContent = 'Checking...';
  btn.disabled = true;
  try {
    const res = await fetch(`/api/inventory/check/${sku}`);
    const data = await res.json();
    const div = document.getElementById('inv-stores-' + sku);

    if (data.stores && data.stores.length) {
      div.innerHTML = `<h4 style="margin:8px 0">Available at ${data.stores.length} stores:</h4>` +
        data.stores.slice(0, 20).map(s => `
          <div class="inv-store-row">
            <span>${esc(s.store_name || 'Store #' + s.store_number)}</span>
            <span>${esc(s.city || '')}</span>
            <span class="inv-qty">${s.quantity} units</span>
          </div>
        `).join('') +
        (data.stores.length > 20 ? `<div class="muted">+${data.stores.length - 20} more stores</div>` : '');
      div.style.display = 'block';
    } else {
      div.innerHTML = '<p class="muted">No inventory data found. Check LCBO.com directly.</p>';
      div.style.display = 'block';
    }
    if (data.source === 'cache') {
      div.innerHTML += '<div class="muted" style="font-size:11px;margin-top:4px">Showing cached data (live check failed)</div>';
    }
  } catch (e) {
    console.error('Inventory check failed:', e);
  }
  btn.textContent = 'Check Stock';
  btn.disabled = false;
  loadInventory(); // Refresh stats
}

async function addProduct() {
  const brand = document.getElementById('newProdBrand').value;
  const name = document.getElementById('newProdName').value.trim();
  if (!name) { alert('Enter product name'); return; }
  await fetch('/api/products', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      brand: brand,
      name: name,
      lcbo_sku: document.getElementById('newProdSku').value.trim(),
      category: document.getElementById('newProdCat').value,
    })
  });
  document.getElementById('newProdName').value = '';
  document.getElementById('newProdSku').value = '';
  loadInventory();
}

// === ROUTES ===
async function loadRoutes() {
  // Load city filter
  const cityRes = await fetch('/api/routes/cities');
  const cities = await cityRes.json();
  const citySel = document.getElementById('routeCity');
  if (citySel.options.length <= 1) {
    citySel.innerHTML = '<option value="">All Cities</option>' + cities.map(c => `<option value="${c.city}">${c.city} (${c.store_count}, ${c.distance_km}km, ${c.coverage}%)</option>`).join('');
  }

  const city = document.getElementById('routeCity').value;
  const maxKm = document.getElementById('routeMaxKm').value;
  const limit = document.getElementById('routeLimit').value;
  const sort = document.getElementById('routeSort').value;
  const district = document.getElementById('routeDistrict').value;

  const res = await fetch(`/api/routes?city=${encodeURIComponent(city)}&max_km=${maxKm}&limit=${limit}&sort=${sort}&district=${encodeURIComponent(district)}`);
  const data = await res.json();

  // Populate district dropdown once
  const distSel = document.getElementById('routeDistrict');
  if (distSel.options.length <= 1 && data.districts) {
    distSel.innerHTML = '<option value="">All Districts</option>' + data.districts.map(d => `<option value="${d}">${d}</option>`).join('');
  }

  const countEl = document.getElementById('routeCount');
  if (countEl) countEl.textContent = data.total;

  // Route map link
  const linkDiv = document.getElementById('routeMapLink');
  if (data.route_url && data.stores.length) {
    linkDiv.innerHTML = `<a href="${data.route_url}" target="_blank" class="btn-primary" style="display:inline-block;text-decoration:none;margin-right:8px">&#128506; Open Route in Google Maps (Top ${Math.min(9, data.stores.length)} stores)</a>`;
  }
  checkGeocode(data.stores);

  // District summary cards
  const distDiv = document.getElementById('districtSummary');
  if (data.district_summary && data.district_summary.length > 1) {
    distDiv.innerHTML = '<div class="city-route-grid">' + data.district_summary.map(ds =>
      `<div class="city-route-card district-card" onclick="document.getElementById('routeCity').value='${escAttr(ds.city)}';loadRoutes()">
        <div class="crc-name">${esc(ds.city)}</div>
        <div class="crc-dist">${ds.avg_dist} km avg</div>
        <div class="crc-count">${ds.count} target stores</div>
      </div>`
    ).join('') + '</div>';
  } else {
    distDiv.innerHTML = '';
  }

  // Store table with address and days since visit
  const tbody = document.querySelector('#routesTable tbody');
  tbody.innerHTML = data.stores.map(s => {
    const daysBadge = s.days_since_visit !== null
      ? (s.days_since_visit > 30 ? '<span class="days-badge overdue">' + s.days_since_visit + 'd ago</span>'
        : s.days_since_visit > 14 ? '<span class="days-badge warning">' + s.days_since_visit + 'd ago</span>'
        : '<span class="days-badge recent">' + s.days_since_visit + 'd ago</span>')
      : '<span class="days-badge never">Never</span>';
    const addr = s.address ? truncate(s.address, 30) : '';
    return `<tr onclick="openStoreModal(${s.id})" style="cursor:pointer" class="${!s.last_activity ? 'row-unvisited' : ''}">
      <td><strong>${s.distance_km} km</strong></td>
      <td>#${s.store_number}</td>
      <td>${esc(truncate(s.account, 22))}</td>
      <td class="addr-cell">${esc(addr)}</td>
      <td>${esc(s.city)}</td>
      <td>${daysBadge}</td>
      <td>${s.activity_count}</td>
      <td><a href="https://www.google.com/maps/dir/43.6558,-79.3628/${encodeURIComponent(s.full_address || s.lat+','+s.lng)}" target="_blank" class="btn-sm" onclick="event.stopPropagation()">&#128506; Go</a></td>
    </tr>`;
  }).join('');

  // Cities distance list with coverage bars
  const citiesDiv = document.getElementById('citiesRoute');
  citiesDiv.innerHTML = '<div class="city-route-grid">' + cities.slice(0, 40).map(c =>
    `<div class="city-route-card" onclick="document.getElementById('routeCity').value='${escAttr(c.city)}';loadRoutes()">
      <div class="crc-name">${esc(c.city)}</div>
      <div class="crc-dist">${c.distance_km} km</div>
      <div class="crc-count">${c.store_count} stores</div>
      <div class="coverage-bar"><div class="coverage-fill" style="width:${c.coverage}%;background:${c.coverage > 50 ? '#16a34a' : c.coverage > 20 ? '#f59e0b' : '#ef4444'}"></div></div>
      <div class="crc-cov">${c.coverage}% covered</div>
    </div>`
  ).join('') + '</div>';
}

function onDistrictChange() {
  document.getElementById('routeCity').value = '';
  loadRoutes();
}

// Check if stores need geocoding (many share same coords = city-center defaults)
function checkGeocode(stores) {
  const coordMap = {};
  for (const s of stores) {
    const key = `${(s.lat||0).toFixed(3)},${(s.lng||0).toFixed(3)}`;
    coordMap[key] = (coordMap[key] || 0) + 1;
  }
  const duplicates = Object.values(coordMap).filter(v => v > 1).reduce((a, b) => a + b, 0);
  const bar = document.getElementById('geocodeBar');
  if (bar) bar.style.display = duplicates > 5 ? '' : 'none';
}

async function geocodeStores() {
  const btn = document.getElementById('geocodeBtn');
  const status = document.getElementById('geocodeStatus');
  btn.disabled = true;
  btn.textContent = 'Geocoding...';
  status.textContent = 'This takes ~1 sec per store (rate limit). Please wait...';
  try {
    const res = await fetch('/api/geocode', { method: 'POST' });
    const data = await res.json();
    status.textContent = data.message;
    if (data.remaining > 0) {
      btn.textContent = `Continue (${data.remaining} left)`;
      btn.disabled = false;
    } else {
      btn.textContent = 'Done!';
      loadRoutes();
    }
  } catch (e) {
    status.textContent = 'Error: ' + e.message;
    btn.disabled = false;
    btn.textContent = 'Retry';
  }
}

// === OPPORTUNITIES ===
async function loadOpportunities() {
  const res = await fetch('/api/opportunities/nb-distillers');
  const data = await res.json();
  const s = data.summary;

  document.getElementById('oppStats').innerHTML = `
    <div class="opp-stat-grid">
      <div class="stat-card red"><div class="label">No NB Products</div><div class="value">${s.zero_nb}</div></div>
      <div class="stat-card orange"><div class="label">Only 1 Product</div><div class="value">${s.one_nb}</div></div>
      <div class="stat-card green"><div class="label">Both Stocked</div><div class="value">${s.both_nb}</div></div>
      <div class="stat-card"><div class="label">Total Stores</div><div class="value">${s.total_stores}</div></div>
    </div>`;

  function oppTable(stores, emptyMsg) {
    if (!stores.length) return `<p class="muted">${emptyMsg}</p>`;
    return `<table class="data-table"><thead><tr><th>Store #</th><th>Account</th><th>City</th><th>NB Products</th><th>Inventory</th><th>Visits</th><th>Navigate</th></tr></thead><tbody>` +
      stores.slice(0, 200).map(s => `<tr onclick="openStoreModal(${s.id})" style="cursor:pointer" class="${s.nb_products_stocked === 0 ? 'row-unvisited' : ''}">
        <td>#${s.store_number}</td>
        <td>${esc(truncate(s.account, 22))}</td>
        <td>${esc(s.city)}</td>
        <td><strong>${s.nb_products_stocked}</strong> / 2</td>
        <td>${s.total_nb_inventory} units</td>
        <td>${s.activity_count}</td>
        <td><a href="https://www.google.com/maps/dir/43.6558,-79.3628/${encodeURIComponent(s.full_address)}" target="_blank" class="btn-sm" onclick="event.stopPropagation()">&#128506;</a></td>
      </tr>`).join('') + '</tbody></table>';
  }

  document.getElementById('oppZero').innerHTML = oppTable(data.zero_stock, 'All stores have NB products!');
  document.getElementById('oppOne').innerHTML = oppTable(data.one_product, 'No stores with only 1 product');
  document.getElementById('oppFull').innerHTML = oppTable(data.fully_stocked, 'No fully stocked stores yet');
}

function showOppTab(tab, btn) {
  document.querySelectorAll('.opp-tab-content').forEach(t => t.style.display = 'none');
  document.querySelectorAll('.opp-tabs .tab-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  const map = { zero: 'oppZero', one: 'oppOne', full: 'oppFull' };
  document.getElementById(map[tab]).style.display = '';
}

async function loadDailyPlan() {
  const district = document.getElementById('planDistrict').value;
  const storesPerDay = document.getElementById('planStoresPerDay').value;
  const grid = document.getElementById('dailyPlanGrid');
  grid.innerHTML = '<p class="muted">Building optimized route plan...</p>';

  const res = await fetch(`/api/routes/daily-plan?district=${encodeURIComponent(district)}&stores_per_day=${storesPerDay}`);
  const data = await res.json();

  if (!data.plans || !data.plans.length) {
    grid.innerHTML = '<p class="muted">No stores found in this district.</p>';
    return;
  }

  grid.innerHTML = data.plans.map(plan => `
    <div class="daily-plan-card">
      <div class="dp-header">
        <div>
          <strong>${plan.day}</strong>
          <span class="muted" style="margin-left:8px">${plan.date}</span>
        </div>
        <div>
          <span class="card-badge">${plan.store_count} stores</span>
          <span class="muted" style="margin-left:6px">${plan.cities.join(', ')}</span>
        </div>
      </div>
      <div class="dp-stores">
        ${plan.stores.map((s, i) => `<div class="dp-store" onclick="openStoreModal(${s.id})" style="cursor:pointer">
          <span class="dp-num">${i + 1}</span>
          <div class="dp-info">
            <strong>LCBO #${s.store_number}</strong> — ${esc(truncate(s.account, 20))}
            <div class="muted" style="font-size:11px">${esc(s.address || '')}, ${esc(s.city || '')}</div>
          </div>
          <div class="dp-meta">
            ${s.days_since_visit !== null ? `<span class="days-badge ${s.days_since_visit > 30 ? 'overdue' : s.days_since_visit > 14 ? 'warning' : 'recent'}">${s.days_since_visit}d</span>` : '<span class="days-badge never">New</span>'}
          </div>
        </div>`).join('')}
      </div>
      <a href="${plan.route_url}" target="_blank" class="btn-primary dp-route-btn">&#128506; Open ${plan.day} Route in Google Maps</a>
    </div>
  `).join('');

  grid.innerHTML += `<div class="muted" style="margin-top:12px;font-size:12px">Planned ${data.total_stores_planned} of ${data.total_stores_in_district} stores in ${data.district}</div>`;
}

// === FOLLOW-UPS ===
async function loadFollowups(status, btn) {
  if (!status) status = 'pending';
  if (btn) {
    document.querySelectorAll('.fu-filter-bar .tab-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
  }
  const res = await fetch(`/api/followups?status=${status}`);
  const followups = await res.json();
  const div = document.getElementById('followupsList');
  const today = new Date().toISOString().split('T')[0];

  if (!followups.length) {
    div.innerHTML = '<p class="muted">No follow-ups scheduled. Set a follow-up date when logging an activity.</p>';
    document.getElementById('followupStats').innerHTML = '';
    return;
  }

  const overdue = followups.filter(f => f.follow_up_date < today);
  const todayItems = followups.filter(f => f.follow_up_date === today);
  const upcoming = followups.filter(f => f.follow_up_date > today);

  // Stats bar
  document.getElementById('followupStats').innerHTML = `
    <div class="fu-stat red-bg"><span class="fu-stat-val">${overdue.length}</span><span>Overdue</span></div>
    <div class="fu-stat orange-bg"><span class="fu-stat-val">${todayItems.length}</span><span>Today</span></div>
    <div class="fu-stat green-bg"><span class="fu-stat-val">${upcoming.length}</span><span>Upcoming</span></div>
    <div class="fu-stat blue-bg"><span class="fu-stat-val">${followups.length}</span><span>Total</span></div>
  `;

  let html = '';
  if (overdue.length) {
    html += '<h4 class="fu-heading fu-overdue-heading">OVERDUE</h4>';
    html += overdue.map(f => followupCardHTML(f, true)).join('');
  }
  if (todayItems.length) {
    html += '<h4 class="fu-heading fu-today-heading">TODAY</h4>';
    html += todayItems.map(f => followupCardHTML(f, false)).join('');
  }
  if (upcoming.length) {
    html += '<h4 class="fu-heading fu-upcoming-heading">UPCOMING</h4>';
    html += upcoming.map(f => followupCardHTML(f, false)).join('');
  }
  div.innerHTML = html;
}

function followupCardHTML(f, overdue) {
  const isCompleted = f.status === 'completed';
  const fuId = f.id;
  return `<div class="followup-card ${overdue ? 'followup-overdue' : ''} ${isCompleted ? 'followup-completed' : ''}">
    <div class="fu-top">
      <span class="fu-store" onclick="openStoreModal(${f.store_id})" style="cursor:pointer">LCBO #${f.store_number} — ${esc(f.account)}</span>
      <span class="fu-date">${f.follow_up_date}${overdue ? ' (OVERDUE)' : ''}${isCompleted ? ' ✓ DONE' : ''}</span>
    </div>
    <div class="fu-meta">${esc(f.rep_name || '')} · <span class="type-badge ${f.activity_type || f.followup_type || ''}">${formatType(f.activity_type || f.followup_type || '')}</span> ${producerTags(f.producer)} ${f.venue_type ? `<span class="venue-tag">${esc(f.venue_type)}</span>` : ''}</div>
    ${f.notes ? `<div class="fu-notes">${esc(truncate(f.notes, 120))}</div>` : ''}
    ${f.city ? `<div class="fu-city">${esc(f.city)}${f.address ? ' — ' + esc(f.address) : ''}</div>` : ''}
    ${!isCompleted ? `<div class="fu-actions">
      <button class="btn-sm btn-complete" onclick="event.stopPropagation();completeFollowup(${fuId})">✓ Complete</button>
      <button class="btn-sm btn-reschedule" onclick="event.stopPropagation();rescheduleFollowup(${fuId})">&#128197; Reschedule</button>
      <a href="https://www.google.com/maps/dir/43.6558,-79.3628/${encodeURIComponent((f.address||'')+', '+(f.city||'')+', ON')}" target="_blank" class="btn-sm" onclick="event.stopPropagation()">&#128506; Navigate</a>
    </div>` : `<div class="fu-completed-at muted" style="font-size:11px">Completed: ${f.completed_at || ''}</div>`}
  </div>`;
}

async function completeFollowup(id) {
  await fetch(`/api/followups/${id}/complete`, { method: 'POST' });
  loadFollowups('pending');
}

async function rescheduleFollowup(id) {
  const newDate = prompt('Enter new follow-up date (YYYY-MM-DD):');
  if (!newDate || !/^\d{4}-\d{2}-\d{2}$/.test(newDate)) return;
  await fetch(`/api/followups/${id}/reschedule`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ due_date: newDate })
  });
  loadFollowups('pending');
}

// === HELPERS ===
function formatType(t) {
  const map = { tasting: 'Tasting', site_visit: 'Site Visit', listing: 'Listing', email: 'Email', call: 'Call', follow_up: 'Follow-Up' };
  return map[t] || t;
}

function formatDate(d) {
  if (!d) return '—';
  const dt = new Date(d + (d.includes('Z') || d.includes('+') ? '' : 'Z'));
  return dt.toLocaleDateString('en-CA', { month: 'short', day: 'numeric', year: 'numeric' }) +
    ' ' + dt.toLocaleTimeString('en-CA', { hour: '2-digit', minute: '2-digit' });
}

function formatDateShort(d) {
  if (!d) return '';
  const dt = new Date(d + (d.includes('Z') || d.includes('+') ? '' : 'Z'));
  return dt.toLocaleDateString('en-CA', { month: 'short', day: 'numeric' });
}

function statusBadge(s) {
  if (!s) return '';
  const cls = { 'Won': 'badge-won', 'Pitched': 'badge-pitched', 'Lost': 'badge-lost', 'Not Active': 'badge-not-active', 'Follow Up': 'badge-follow-up' };
  return `<span class="badge ${cls[s] || ''}">${esc(s)}</span>`;
}

function producerTags(p) {
  if (!p) return '';
  return p.split(',').map(s => s.trim()).filter(Boolean).map(s => {
    const cls = s.includes('NB') ? 'nb' : 'anu';
    return `<span class="producer-tag ${cls}">${esc(s)}</span>`;
  }).join(' ');
}

function truncate(s, n) { if (!s) return ''; return s.length > n ? s.slice(0, n) + '...' : s; }

function esc(s) {
  if (!s) return '';
  const el = document.createElement('span');
  el.textContent = s;
  return el.innerHTML;
}

function escAttr(s) {
  if (!s) return '';
  return s.replace(/'/g, "\\'").replace(/"/g, '&quot;');
}

// ======== SOD (Sale of Data) ========
const _statusLabels = { 'L': 'Listed', 'D': 'Delisting', 'F': 'Delisted' };
const _statusClass = { 'L': 'status-listed', 'D': 'status-delisting', 'F': 'status-delisted' };

async function loadSodStatus() {
  try {
    const res = await fetch('/api/sod/status');
    const d = await res.json();
    const grid = document.getElementById('sodStatusGrid');
    if (!grid) return;
    const a = (d.last_by_source && d.last_by_source.daily_a) || null;
    const b = (d.last_by_source && d.last_by_source.daily_b) || null;
    const lastWhen = a ? a.run_at : (b ? b.run_at : '—');
    const snap = (a && a.snapshot_date) || (b && b.snapshot_date) || '—';
    const inv = d.stats && d.stats.inv_rows || 0;
    const tracked = d.stats && d.stats.tracked_products || 0;
    const days = d.stats && d.stats.snapshot_days || 0;
    grid.innerHTML = `
      <div class="stat-card ${d.configured ? '' : 'stat-warn'}">
        <div class="stat-label">SOD Connection</div>
        <div class="stat-value">${d.configured ? '&#9989; Connected' : '&#9888; Not configured'}</div>
        <div class="stat-sub">Agent ${d.agent_id || '—'} &middot; Scheduler ${d.scheduler_running ? 'ON' : 'OFF'}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Latest Snapshot</div>
        <div class="stat-value">${esc(snap)}</div>
        <div class="stat-sub">Last sync: ${esc(lastWhen)}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Inventory Rows</div>
        <div class="stat-value">${inv.toLocaleString()}</div>
        <div class="stat-sub">${days} day${days===1?'':'s'} of history</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Tracked Products</div>
        <div class="stat-value">${tracked}</div>
        <div class="stat-sub">Anu &amp; NB Distillers</div>
      </div>
    `;
    // Recent runs table
    const runsBody = document.querySelector('#sodRunsTable tbody');
    if (runsBody) {
      const rows = d.recent_runs || [];
      runsBody.innerHTML = rows.length === 0
        ? '<tr><td colspan="10" class="muted">No sync runs yet.</td></tr>'
        : rows.map(r => `
          <tr>
            <td>${esc(r.run_at)}</td>
            <td>${esc(r.source)}</td>
            <td>${esc(r.file_name || '—')}</td>
            <td>${esc(r.snapshot_date || '—')}</td>
            <td><span class="status-badge status-${esc(r.status)}">${esc(r.status)}</span></td>
            <td>${(r.total_rows||0).toLocaleString()}</td>
            <td>${(r.anu_rows||0).toLocaleString()}</td>
            <td>${r.new_listings||0}</td>
            <td>${r.new_delistings||0}</td>
            <td>${Number(r.duration_seconds||0).toFixed(1)}s</td>
          </tr>`).join('');
    }
  } catch (e) { console.warn('loadSodStatus', e); }
}

async function loadSodProducts() {
  try {
    const res = await fetch('/api/sod/products?tracked_only=1');
    const d = await res.json();
    const body = document.querySelector('#sodProductsTable tbody');
    if (!body) return;
    const cnt = document.getElementById('sodProdCount');
    if (cnt) cnt.textContent = d.count;
    body.innerHTML = (d.rows || []).length === 0
      ? '<tr><td colspan="7" class="muted">No SOD data yet — click Sync Now.</td></tr>'
      : d.rows.map(r => `
        <tr>
          <td>${esc(r.brand || '—')}</td>
          <td>${esc(r.product_name || '—')}</td>
          <td><code>${esc(r.sku)}</code></td>
          <td><span class="status-badge ${_statusClass[r.current_status]||''}">${esc(_statusLabels[r.current_status]||r.current_status||'?')}</span></td>
          <td>${r.store_count||0}</td>
          <td>${(r.total_on_hand||0).toLocaleString()}</td>
          <td><button class="btn-link" onclick="showSodTrend('${esc(r.sku)}')">View trend</button></td>
        </tr>
      `).join('');
  } catch (e) { console.warn('loadSodProducts', e); }
}

async function loadSodListingChanges() {
  try {
    const days = document.getElementById('sodChangeDays').value || '30';
    const type = document.getElementById('sodChangeType').value || '';
    const qs = new URLSearchParams({ days, tracked_only: '1' });
    if (type) qs.set('type', type);
    const res = await fetch('/api/sod/listing-changes?' + qs.toString());
    const d = await res.json();
    const body = document.querySelector('#sodChangesTable tbody');
    const cnt = document.getElementById('sodChangeCount');
    if (cnt) cnt.textContent = d.count;
    if (!body) return;
    body.innerHTML = (d.rows || []).length === 0
      ? '<tr><td colspan="6" class="muted">No listing changes in this window.</td></tr>'
      : d.rows.map(r => `
        <tr>
          <td>${esc(r.change_date)}</td>
          <td><span class="change-badge change-${esc(r.change_type)}">${esc(r.change_type)}</span></td>
          <td>${esc(r.brand||'—')}</td>
          <td>${esc(r.product_name||'—')}</td>
          <td>${esc(r.old_status||'—')}</td>
          <td>${esc(r.new_status||'—')}</td>
        </tr>
      `).join('');
  } catch (e) { console.warn('loadSodListingChanges', e); }
}

async function triggerSodSync() {
  const btn = document.getElementById('sodSyncBtn');
  if (btn) { btn.disabled = true; btn.textContent = 'Syncing…'; }
  try {
    const res = await fetch('/api/sod/sync', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sources: ['daily_a', 'daily_b'] }),
    });
    const d = await res.json();
    if (res.ok || res.status === 202) {
      // Poll status every 3s until both sources are no longer 'running'
      let attempts = 0;
      const poll = async () => {
        attempts++;
        const s = await (await fetch('/api/sod/status')).json();
        const a = (s.last_by_source && s.last_by_source.daily_a) || {};
        const b = (s.last_by_source && s.last_by_source.daily_b) || {};
        const done = a.status === 'success' && b.status === 'success';
        const failed = a.status === 'failed' || b.status === 'failed';
        if (done || failed || attempts > 60) {
          await loadSodStatus();
          await loadSodProducts();
          await loadSodListingChanges();
          if (btn) { btn.disabled = false; btn.innerHTML = '&#128260; Sync Now (Daily A + B)'; }
          if (failed) alert('Sync failed — check server logs / SOD credentials.');
        } else {
          setTimeout(poll, 3000);
        }
      };
      setTimeout(poll, 2000);
    } else {
      alert('Error: ' + (d.error || res.statusText));
      if (btn) { btn.disabled = false; btn.innerHTML = '&#128260; Sync Now (Daily A + B)'; }
    }
  } catch (e) {
    console.warn('triggerSodSync', e);
    alert('Sync failed: ' + e.message);
    if (btn) { btn.disabled = false; btn.innerHTML = '&#128260; Sync Now (Daily A + B)'; }
  }
}

async function showSodTrend(sku) {
  try {
    const res = await fetch(`/api/sod/trend/${encodeURIComponent(sku)}?days=60`);
    const d = await res.json();
    const modal = document.createElement('div');
    modal.className = 'sod-modal';
    modal.onclick = (e) => { if (e.target === modal) modal.remove(); };
    const rows = d.rows || [];
    const content = rows.length === 0
      ? '<p class="muted">No trend data yet — need at least 2 snapshots.</p>'
      : `<table class="data-table"><thead><tr><th>Date</th><th>Stores</th><th>On-Hand</th><th>Listed</th><th>Delisting</th></tr></thead><tbody>`
        + rows.map(r => `<tr><td>${esc(r.snapshot_date)}</td><td>${r.store_count}</td><td>${(r.total_on_hand||0).toLocaleString()}</td><td>${r.listed_stores}</td><td>${r.delisting_stores}</td></tr>`).join('')
        + '</tbody></table>';
    modal.innerHTML = `
      <div class="sod-modal-content">
        <h2>SKU ${esc(sku)} &mdash; 60-day Trend</h2>
        <button class="btn-close" onclick="this.closest('.sod-modal').remove()">&times;</button>
        ${content}
      </div>`;
    document.body.appendChild(modal);
  } catch (e) { console.warn('showSodTrend', e); }
}

// ======== REPORTS (Daily / Weekly / Monthly / Rep) ========
async function loadReport(kind, btn) {
  if (btn) {
    btn.parentElement.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
  }
  const container = document.getElementById('reportContent');
  if (!container) return;
  container.innerHTML = '<p class="muted">Loading&hellip;</p>';
  try {
    const res = await fetch(`/api/reports/${kind}`);
    const d = await res.json();
    const title = kind === 'daily' ? 'Daily Summary' : kind === 'weekly' ? 'Weekly Summary (7 days)' : 'Monthly Summary (MTD)';
    const w = d.window || {};
    const t = d.totals || {};
    container.innerHTML = `
      <div class="stats-grid">
        <div class="stat-card"><div class="stat-label">Window</div><div class="stat-value">${esc(w.start)} &rarr; ${esc(w.end)}</div><div class="stat-sub">Latest snapshot: ${esc(w.latest_snapshot || '—')}</div></div>
        <div class="stat-card"><div class="stat-label">Products Tracked</div><div class="stat-value">${t.products_tracked||0}</div></div>
        <div class="stat-card stat-good"><div class="stat-label">New Listings</div><div class="stat-value">${t.new_listings||0}</div></div>
        <div class="stat-card stat-bad"><div class="stat-label">Delistings</div><div class="stat-value">${t.delistings||0}</div></div>
        <div class="stat-card"><div class="stat-label">Re-listings</div><div class="stat-value">${t.relistings||0}</div></div>
        <div class="stat-card"><div class="stat-label">Total Status Events</div><div class="stat-value">${t.changes_in_window||0}</div></div>
      </div>
      <div class="dashboard-grid" style="margin-top:16px">
        <div class="card">
          <h3>${esc(title)} &mdash; Snapshot Metrics</h3>
          <table class="data-table">
            <thead><tr><th>Brand</th><th>Product</th><th>Stores</th><th>On-Hand</th><th>Listed</th><th>Delisting</th><th>Fully Delisted</th></tr></thead>
            <tbody>
              ${(d.snapshot_metrics||[]).map(m => `<tr>
                <td>${esc(m.brand||'—')}</td>
                <td>${esc(m.product_name||'—')}</td>
                <td>${m.store_count||0}</td>
                <td>${(m.total_on_hand||0).toLocaleString()}</td>
                <td>${m.listed_stores||0}</td>
                <td>${m.delisting_stores||0}</td>
                <td>${m.fully_delisted_stores||0}</td>
              </tr>`).join('') || '<tr><td colspan="7" class="muted">No data — run /api/sod/sync.</td></tr>'}
            </tbody>
          </table>
        </div>
        <div class="card">
          <h3>Listing Events in Window</h3>
          <table class="data-table">
            <thead><tr><th>Date</th><th>Type</th><th>Product</th><th>Old&rarr;New</th></tr></thead>
            <tbody>
              ${(d.listing_changes||[]).map(c => `<tr>
                <td>${esc(c.change_date)}</td>
                <td><span class="change-badge change-${esc(c.change_type)}">${esc(c.change_type)}</span></td>
                <td>${esc(c.product_name||'—')}</td>
                <td>${esc(c.old_status||'—')} &rarr; ${esc(c.new_status||'—')}</td>
              </tr>`).join('') || '<tr><td colspan="4" class="muted">No listing events in this window.</td></tr>'}
            </tbody>
          </table>
        </div>
      </div>
    `;
  } catch (e) {
    container.innerHTML = `<p class="muted">Failed to load report: ${esc(e.message)}</p>`;
  }
}

async function loadRepReport(btn) {
  if (btn) {
    btn.parentElement.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
  }
  const container = document.getElementById('reportContent');
  if (!container) return;
  container.innerHTML = '<p class="muted">Loading per-rep report&hellip;</p>';
  try {
    const res = await fetch('/api/reports/rep');
    const d = await res.json();
    const reps = d.reps || [];
    const anuProducts = reps[0] && reps[0].per_product || [];
    const productHeaders = anuProducts.map(p => `<th colspan="2">${esc(p.brand)}<br><small>${esc(p.product_name)}</small></th>`).join('');
    const subHeaders = anuProducts.map(() => '<th>Carrying</th><th>Delisting</th>').join('');
    container.innerHTML = `
      <div class="card">
        <h3>Per-Rep Performance &mdash; Snapshot ${esc(d.snapshot_date || '—')}</h3>
        <p class="view-desc">Stores carrying each tracked SKU, by rep. "Delisting" = stores where SOD flags the product for removal.</p>
        <div class="scroll-x">
          <table class="data-table">
            <thead>
              <tr><th rowspan="2">Rep</th><th rowspan="2">Total Stores</th>${productHeaders}</tr>
              <tr>${subHeaders}</tr>
            </thead>
            <tbody>
              ${reps.map(r => `
                <tr>
                  <td><strong>${esc(r.rep)}</strong></td>
                  <td>${r.total_stores}</td>
                  ${(r.per_product||[]).map(p => `<td>${p.stores_carrying}</td><td class="${p.stores_delisting>0?'text-bad':''}">${p.stores_delisting}</td>`).join('')}
                </tr>`).join('') || '<tr><td colspan="20" class="muted">No rep data.</td></tr>'}
            </tbody>
          </table>
        </div>
      </div>
    `;
  } catch (e) {
    container.innerHTML = `<p class="muted">Failed to load rep report: ${esc(e.message)}</p>`;
  }
}


// ====================================================================
// ============================ CRM VIEWS =============================
// ====================================================================

let _crmFilterOptionsLoaded = false;
let _crmTerritoriesCache = null;
let _crmTrackedSkusCache = null;

async function loadCrmFilterOptions() {
  // Only need to do this once per page load
  if (_crmFilterOptionsLoaded) return;
  try {
    const [terrRes, skuRes] = await Promise.all([
      fetch('/api/crm/territories'),
      fetch('/api/sod/products?tracked=1'),
    ]);
    _crmTerritoriesCache = await terrRes.json();
    const skuList = await skuRes.json();
    _crmTrackedSkusCache = (skuList.products || skuList || []).map(p => ({
      sku: p.sku,
      label: `${p.brand || ''} ${p.product_name || p.name || ''} (${p.sku})`.trim(),
    }));

    const terrOpts = '<option value="">All territories</option>' +
      _crmTerritoriesCache.map(t => `<option value="${t.id}">${esc(t.name)} (${t.store_count})</option>`).join('');
    ['oosTerr','mapTerritory','oppsTerr','oppsFullTerr','hFilterTerr'].forEach(id => {
      const el = document.getElementById(id); if (el) el.innerHTML = terrOpts;
    });
    const skuOpts = _crmTrackedSkusCache.map(s => `<option value="${s.sku}">${esc(s.label)}</option>`).join('');
    const skuOptsAll = '<option value="">All tracked SKUs</option>' + skuOpts;
    ['oosSku','oppsSku','oppsFullSku','mapSku'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.innerHTML = (id === 'oppsFullSku' || id === 'oppsSku') ? skuOpts : skuOptsAll;
    });
    _crmFilterOptionsLoaded = true;
  } catch(e) { console.warn('loadCrmFilterOptions', e); }
}

// ====== CRM Dashboard ======
async function loadCrmDashboard() {
  await loadCrmFilterOptions();
  try {
    const [dashRes, digestRes] = await Promise.all([
      fetch('/api/crm/dashboard'),
      fetch('/api/crm/listing-digest?days=14'),
    ]);
    const d = await dashRes.json();
    const dig = await digestRes.json();
    document.getElementById('kpiSnapshot').textContent = d.latest_snapshot || '—';
    document.getElementById('kpiOos').textContent = d.oos_brink_count;
    document.getElementById('kpiNewListings').textContent =
      (d.digest_last_7_days?.NEW_LISTING || 0) + (d.digest_last_7_days?.RELISTED || 0);
    document.getElementById('kpiDelistings').textContent =
      (d.digest_last_7_days?.DELISTED || 0);

    // Tracked rollup table
    const tt = document.getElementById('crmTrackedTable');
    const _statusLabel = (s) => ({L:'Listed', D:'Delisting', F:'Delisted'})[s] || s;
    tt.innerHTML = `<table class="data-table"><thead><tr>
        <th>SKU</th><th>Brand</th><th>Product</th>
        <th>Status</th><th>Stores</th><th>Total On-Hand</th>
      </tr></thead><tbody>${
        (d.tracked_sku_rollup||[]).map(p => `<tr>
          <td><code>${esc(p.sku)}</code></td>
          <td>${esc(p.brand)}</td>
          <td>${esc(p.product_name)}</td>
          <td><span class="status-badge status-${(p.current_status||'L').toLowerCase()=='l'?'listed':((p.current_status||'').toLowerCase()=='d'?'delisting':'delisted')}">${esc(_statusLabel(p.current_status))}</span></td>
          <td>${p.store_count}</td>
          <td>${p.total_on_hand}</td>
        </tr>`).join('') || '<tr><td colspan="6" class="muted">No SOD data yet — wait for first sync.</td></tr>'
      }</tbody></table>`;

    // Territory list
    const tl = document.getElementById('crmTerritoryList');
    tl.innerHTML = (d.territories||[]).map(t => `
      <div class="terr-row" style="border-left:5px solid ${esc(t.color||'#888')}">
        <div class="terr-name">${esc(t.name)}</div>
        <div class="terr-count">${t.store_count} stores</div>
      </div>`).join('') || '<p class="muted">No territories yet.</p>';

    // Digest
    const dt = document.getElementById('crmDigestTable');
    const _ct = (c) => ({NEW_LISTING:'New', DELISTED:'Delisted', RELISTED:'Relisted', BASELINE:'Baseline', STATUS_FLIP:'Flip'})[c] || c;
    dt.innerHTML = `<table class="data-table"><thead><tr>
        <th>Date</th><th>Type</th><th>SKU</th><th>Product</th><th>Old → New</th>
      </tr></thead><tbody>${
        (dig.changes||[]).slice(0, 50).map(c => `<tr ${c.is_tracked?'class="row-tracked"':''}>
          <td>${esc(c.change_date)}</td>
          <td><span class="change-badge change-${esc(c.change_type)}">${esc(_ct(c.change_type))}</span></td>
          <td><code>${esc(c.sku)}</code></td>
          <td>${esc(c.product_name||'').slice(0,40)}</td>
          <td>${esc(c.old_status||'-')} &rarr; ${esc(c.new_status||'-')}</td>
        </tr>`).join('') || '<tr><td colspan="5" class="muted">No changes in window.</td></tr>'
      }</tbody></table>`;

    // Opportunities (compact)
    loadOpportunitiesCompact();
  } catch(e) {
    console.error('loadCrmDashboard', e);
  }
}

async function loadOpportunitiesCompact() {
  // Compact version embedded in CRM dashboard
  const sku = document.getElementById('oppsSku')?.value || '';
  const tid = document.getElementById('oppsTerr')?.value || '';
  const thr = document.getElementById('oppsThreshold')?.value || 3;
  const cont = document.getElementById('oppsTable');
  if (!cont) return;
  cont.innerHTML = '<p class="muted">Loading opportunities…</p>';
  try {
    const qs = new URLSearchParams();
    if (sku) qs.set('sku', sku);
    if (tid) qs.set('territory_id', tid);
    qs.set('slow_threshold', thr);
    qs.set('limit', '50');
    const r = await fetch('/api/crm/opportunities?' + qs.toString());
    const data = await r.json();
    cont.innerHTML = renderOppsTable(data);
  } catch(e) { cont.innerHTML = '<p class="text-bad">Failed to load: ' + esc(e.message) + '</p>'; }
}

async function loadOpportunitiesFull() {
  await loadCrmFilterOptions();
  const sku = document.getElementById('oppsFullSku')?.value || '';
  const tid = document.getElementById('oppsFullTerr')?.value || '';
  const thr = document.getElementById('oppsFullThreshold')?.value || 3;
  const cont = document.getElementById('oppsFullTable');
  if (!cont) return;
  cont.innerHTML = '<p class="muted">Loading opportunities…</p>';
  try {
    const qs = new URLSearchParams();
    if (sku) qs.set('sku', sku);
    if (tid) qs.set('territory_id', tid);
    qs.set('slow_threshold', thr);
    qs.set('limit', '500');
    const r = await fetch('/api/crm/opportunities?' + qs.toString());
    const data = await r.json();
    document.getElementById('oppsCount').textContent = `${data.length} opportunities`;
    cont.innerHTML = renderOppsTable(data);
  } catch(e) { cont.innerHTML = '<p class="text-bad">Failed: ' + esc(e.message) + '</p>'; }
}

function renderOppsTable(data) {
  if (!data || !data.length) return '<p class="muted">No replacement opportunities right now. Run a SOD sync or widen the slow threshold.</p>';
  return `<table class="data-table"><thead><tr>
    <th>Score</th><th>Pitch SKU</th><th>Slow Competitor</th><th>Category</th>
    <th>Store #</th><th>City</th><th>Territory</th>
    <th>Comp Status</th><th>Comp On-Hand</th>
  </tr></thead><tbody>${data.map(o => `<tr>
    <td><strong style="color:${o.opportunity_score>=50?'#d63031':o.opportunity_score>=25?'#e17055':'#636e72'}">${o.opportunity_score}</strong></td>
    <td><strong>${esc(o.our_brand)} ${esc(o.our_product)}</strong><br><code class="muted-small">${esc(o.our_sku)}</code></td>
    <td>${esc((o.competitor_name||'').slice(0,32))}<br><code class="muted-small">${esc(o.competitor_sku)}</code></td>
    <td>${esc(o.category)}</td>
    <td>#${o.store_number}</td>
    <td>${esc(o.city||'')}</td>
    <td><span class="terr-pill" style="background:${esc(o.territory_color||'#888')}">${esc(o.territory_name||'')}</span></td>
    <td><span class="status-badge status-${o.competitor_status==='L'?'listed':o.competitor_status==='D'?'delisting':'delisted'}">${esc(o.competitor_status)}</span></td>
    <td>${o.competitor_on_hand}</td>
  </tr>`).join('')}</tbody></table>`;
}

// ====== OOS Risk ======
async function loadOos() {
  await loadCrmFilterOptions();
  const sku = document.getElementById('oosSku')?.value || '';
  const tid = document.getElementById('oosTerr')?.value || '';
  const thr = document.getElementById('oosThreshold')?.value || 2;
  const cont = document.getElementById('oosTable');
  if (!cont) return;
  cont.innerHTML = '<p class="muted">Scanning…</p>';
  try {
    const qs = new URLSearchParams();
    if (sku) qs.set('sku', sku);
    if (tid) qs.set('territory_id', tid);
    qs.set('threshold', thr);
    const r = await fetch('/api/crm/oos-risk?' + qs.toString());
    const data = await r.json();
    if (!data.length) { cont.innerHTML = '<p class="muted">No stores at OOS risk. Excellent!</p>'; return; }
    cont.innerHTML = `<table class="data-table"><thead><tr>
      <th>Severity</th><th>SKU</th><th>Product</th>
      <th>Store #</th><th>City</th><th>Territory</th>
      <th>On-Hand</th><th>Snapshot</th>
    </tr></thead><tbody>${data.map(r => `<tr>
      <td><span class="sev-${esc(r.severity)}">${esc(r.severity.toUpperCase())}</span></td>
      <td><code>${esc(r.sku)}</code></td>
      <td>${esc(r.product_name||'').slice(0,32)}</td>
      <td>#${r.store_number}</td>
      <td>${esc(r.city||'')}</td>
      <td><span class="terr-pill" style="background:${esc(r.territory_color||'#888')}">${esc(r.territory_name||'')}</span></td>
      <td><strong style="color:${(r.on_hand||0)===0?'#d63031':(r.on_hand||0)<=1?'#e17055':'#fdcb6e'}">${r.on_hand}</strong></td>
      <td>${esc(r.snapshot_date)}</td>
    </tr>`).join('')}</tbody></table>`;
  } catch(e) { cont.innerHTML = '<p class="text-bad">Failed: ' + esc(e.message) + '</p>'; }
}

// ====== Map (Leaflet) ======
let _leafletMap = null, _leafletLayer = null;
async function initMap() {
  await loadCrmFilterOptions();
  if (typeof L === 'undefined') {
    setTimeout(initMap, 200);  // wait for Leaflet to load
    return;
  }
  if (!_leafletMap) {
    _leafletMap = L.map('storeMap').setView([43.7, -79.4], 8);  // Toronto-centered
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; OpenStreetMap contributors',
      maxZoom: 18,
    }).addTo(_leafletMap);
  }
  reloadMap();
}

async function reloadMap() {
  if (!_leafletMap || typeof L === 'undefined') return;
  const tid = document.getElementById('mapTerritory')?.value || '';
  const colorBy = document.getElementById('mapColorBy')?.value || 'territory';
  const sku = document.getElementById('mapSku')?.value || '';

  if (_leafletLayer) { _leafletLayer.remove(); _leafletLayer = null; }
  const qs = new URLSearchParams();
  if (tid) qs.set('territory_id', tid);
  qs.set('with_coords_only', '1');
  let stores = [];
  let listingMap = {};  // store_number -> status (for tracked sku)
  try {
    const sr = await fetch('/api/crm/stores?' + qs.toString());
    stores = await sr.json();
    if (colorBy === 'status' && sku) {
      // get listing status for the chosen SKU
      const ir = await fetch('/api/sod/inventory?sku=' + sku);
      const inv = await ir.json();
      (inv.rows || inv || []).forEach(r => { listingMap[r.store_number] = r.status; });
    }
  } catch(e) { console.warn('reloadMap fetch', e); }

  document.getElementById('mapCount').textContent = `${stores.length} stores plotted`;

  const markers = [];
  const grp = L.layerGroup();
  stores.forEach(s => {
    if (!s.lat || !s.lng) return;
    let color = s.territory_color || '#888';
    let popup = `<strong>#${s.store_number} — ${esc(s.account||'')}</strong><br>
      ${esc(s.address||'')}<br>${esc(s.city||'')} ${esc(s.postal||'')}<br>
      Territory: <span style="color:${color}">${esc(s.territory_name||'Unassigned')}</span><br>
      Rep: ${esc(s.rep||'—')} · ${esc(s.priority||'')}`;
    if (colorBy === 'status' && sku) {
      const st = listingMap[s.store_number];
      color = st === 'L' ? '#00b894' : st === 'D' ? '#fdcb6e' : st === 'F' ? '#d63031' : '#bdc3c7';
      popup += `<br><br>Status for SKU ${sku}: <strong>${st || 'Not Listed'}</strong>`;
    }
    const m = L.circleMarker([s.lat, s.lng], {
      radius: 6, color, fillColor: color, fillOpacity: 0.7, weight: 1,
    }).bindPopup(popup);
    grp.addLayer(m);
    markers.push(m);
  });
  grp.addTo(_leafletMap);
  _leafletLayer = grp;
  if (markers.length) {
    const fg = L.featureGroup(markers);
    try { _leafletMap.fitBounds(fg.getBounds().pad(0.1)); } catch(e) {}
  }

  // Legend
  const lg = document.getElementById('mapLegend');
  if (colorBy === 'territory') {
    lg.innerHTML = (_crmTerritoriesCache||[]).map(t =>
      `<span class="legend-item"><span class="dot" style="background:${esc(t.color)}"></span> ${esc(t.name)} (${t.store_count})</span>`
    ).join('');
  } else {
    lg.innerHTML = `
      <span class="legend-item"><span class="dot" style="background:#00b894"></span> Listed</span>
      <span class="legend-item"><span class="dot" style="background:#fdcb6e"></span> Delisting</span>
      <span class="legend-item"><span class="dot" style="background:#d63031"></span> Delisted</span>
      <span class="legend-item"><span class="dot" style="background:#bdc3c7"></span> Not Listed</span>
    `;
  }
}

// ====== Goals ======
async function loadGoals() {
  try {
    const [list, prog] = await Promise.all([
      fetch('/api/crm/goals').then(r=>r.json()),
      fetch('/api/crm/goals/progress').then(r=>r.json()),
    ]);
    const progMap = {};
    prog.forEach(p => { progMap[p.id] = p; });
    const cont = document.getElementById('goalsTable');
    if (!list.length) { cont.innerHTML = '<p class="muted">No goals yet — add one above.</p>'; return; }
    cont.innerHTML = `<table class="data-table"><thead><tr>
      <th>Scope</th><th>Key</th><th>Period</th>
      <th>Listings (achieved/target)</th><th>%</th>
      <th>Units (achieved/target)</th><th>%</th>
      <th>Notes</th><th></th>
    </tr></thead><tbody>${list.map(g => {
      const p = progMap[g.id] || {};
      return `<tr>
        <td>${esc(g.scope)}</td>
        <td><code>${esc(g.scope_key)}</code></td>
        <td>${esc(g.period_start)} → ${esc(g.period_end)}</td>
        <td>${p.achieved_listings ?? '—'} / ${g.target_listings}</td>
        <td>${renderProgressBar(p.pct_listings)}</td>
        <td>${p.achieved_units ?? '—'} / ${g.target_units}</td>
        <td>${renderProgressBar(p.pct_units)}</td>
        <td>${esc(g.notes||'')}</td>
        <td><button class="btn-link" onclick="deleteGoal(${g.id})">Delete</button></td>
      </tr>`;
    }).join('')}</tbody></table>`;
  } catch(e) { console.warn('loadGoals', e); }
}

function renderProgressBar(pct) {
  if (pct == null) return '—';
  const clamped = Math.max(0, Math.min(100, pct));
  const color = pct >= 100 ? '#00b894' : pct >= 75 ? '#a3d977' : pct >= 50 ? '#fdcb6e' : pct >= 25 ? '#e17055' : '#d63031';
  return `<div class="progress-bar"><div class="progress-fill" style="width:${clamped}%;background:${color}"></div><span class="progress-label">${pct}%</span></div>`;
}

async function createGoal() {
  const body = {
    scope: document.getElementById('goalScope').value,
    scope_key: document.getElementById('goalKey').value.trim(),
    period_start: document.getElementById('goalStart').value,
    period_end: document.getElementById('goalEnd').value,
    target_listings: parseInt(document.getElementById('goalListings').value || 0),
    target_units: parseInt(document.getElementById('goalUnits').value || 0),
    target_revenue: parseFloat(document.getElementById('goalRevenue').value || 0),
    notes: document.getElementById('goalNotes').value || '',
  };
  try {
    const r = await fetch('/api/crm/goals', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)});
    if (!r.ok) throw new Error((await r.json()).error || 'failed');
    document.getElementById('goalForm').reset();
    loadGoals();
  } catch(e) { alert('Failed to save goal: ' + e.message); }
}

async function deleteGoal(id) {
  if (!confirm('Delete this goal?')) return;
  await fetch('/api/crm/goals/' + id, {method:'DELETE'});
  loadGoals();
}

// ====== HORECA ======
async function loadHoreca() {
  await loadCrmFilterOptions();
  const t = document.getElementById('hFilterType').value;
  const s = document.getElementById('hFilterStatus').value;
  const tid = document.getElementById('hFilterTerr').value;
  const qs = new URLSearchParams();
  if (t) qs.set('type', t);
  if (s) qs.set('status', s);
  if (tid) qs.set('territory_id', tid);
  try {
    const r = await fetch('/api/crm/horeca?' + qs.toString());
    const data = await r.json();
    const cont = document.getElementById('horecaTable');
    if (!data.length) { cont.innerHTML = '<p class="muted">No HORECA accounts yet — add one above.</p>'; return; }
    cont.innerHTML = `<table class="data-table"><thead><tr>
      <th>Name</th><th>Type</th><th>City</th><th>Territory</th>
      <th>Contact</th><th>Phone</th><th>Status</th><th>Priority</th>
      <th>Last Visit</th><th>Next Visit</th><th>Products</th><th></th>
    </tr></thead><tbody>${data.map(h => `<tr>
      <td><strong>${esc(h.name)}</strong></td>
      <td>${esc(h.account_type)}</td>
      <td>${esc(h.city||'')}</td>
      <td><span class="terr-pill" style="background:${esc(h.territory_color||'#888')}">${esc(h.territory_name||'')}</span></td>
      <td>${esc(h.contact_name||'')} <span class="muted-small">${esc(h.contact_title||'')}</span></td>
      <td>${esc(h.phone||'')}</td>
      <td><span class="status-pill status-${esc(h.status)}">${esc(h.status)}</span></td>
      <td>${esc(h.priority||'')}</td>
      <td>${esc(h.last_visit||'')}</td>
      <td>${esc(h.next_visit||'')}</td>
      <td>${esc((h.products_carried||'').slice(0,30))}</td>
      <td><button class="btn-link text-bad" onclick="deleteHoreca(${h.id})">Delete</button></td>
    </tr>`).join('')}</tbody></table>`;
  } catch(e) { console.warn('loadHoreca', e); }
}

async function createHoreca() {
  const body = {
    name: document.getElementById('hName').value.trim(),
    account_type: document.getElementById('hType').value,
    address: document.getElementById('hAddress').value,
    city: document.getElementById('hCity').value,
    postal: document.getElementById('hPostal').value,
    phone: document.getElementById('hPhone').value,
    email: document.getElementById('hEmail').value,
    contact_name: document.getElementById('hContact').value,
    contact_title: document.getElementById('hTitle').value,
    status: document.getElementById('hStatus').value,
    priority: document.getElementById('hPriority').value,
    products_carried: document.getElementById('hProducts').value,
    notes: document.getElementById('hNotes').value,
  };
  try {
    const r = await fetch('/api/crm/horeca', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)});
    if (!r.ok) throw new Error((await r.json()).error || 'failed');
    document.getElementById('horecaForm').reset();
    loadHoreca();
  } catch(e) { alert('Failed: ' + e.message); }
}

async function deleteHoreca(id) {
  if (!confirm('Delete this account?')) return;
  await fetch('/api/crm/horeca/' + id, {method:'DELETE'});
  loadHoreca();
}

// ====== Territories ======
async function loadTerritories() {
  try {
    const r = await fetch('/api/crm/territories');
    const data = await r.json();
    const cont = document.getElementById('terrTable');
    cont.innerHTML = `<table class="data-table"><thead><tr>
      <th></th><th>Code</th><th>Name</th><th>Region</th>
      <th>Stores</th><th>HORECA</th><th>FSA Prefixes</th><th>Rep</th>
    </tr></thead><tbody>${data.map(t => `<tr>
      <td><span class="dot" style="background:${esc(t.color)}"></span></td>
      <td><code>${esc(t.code)}</code></td>
      <td><strong>${esc(t.name)}</strong></td>
      <td>${esc(t.region||'')}</td>
      <td>${t.store_count}</td>
      <td>${t.horeca_count}</td>
      <td><code class="muted-small">${esc(t.fsa_prefixes||'')}</code></td>
      <td><input type="text" data-tid="${t.id}" value="${esc(t.rep_name||'')}" onblur="setTerritoryRep(this)" placeholder="(unassigned)"></td>
    </tr>`).join('')}</tbody></table>`;
  } catch(e) { console.warn('loadTerritories', e); }
}

async function setTerritoryRep(input) {
  const tid = input.dataset.tid;
  const rep = input.value.trim();
  await fetch('/api/crm/territories/' + tid, {method:'PUT', headers:{'Content-Type':'application/json'}, body: JSON.stringify({rep_name: rep})});
}

async function reassignTerritories() {
  if (!confirm('Re-run FSA-based auto-assignment for all stores?')) return;
  const r = await fetch('/api/crm/territories/reassign', {method:'POST'});
  const data = await r.json();
  alert(`Reassigned ${data.reassigned} stores.`);
  loadTerritories();
}
