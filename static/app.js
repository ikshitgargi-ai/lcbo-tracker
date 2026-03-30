/* ===== Anu Spirits LCBO Tracker Pro — Frontend Engine ===== */

let currentPage = 1;
let activeRepId = null;
let currentModalStoreId = null;
let searchTimeout = null;
let selectedProducers = [];
let selectedActivities = [];
let selectedVenue = '';

// === INIT ===
document.addEventListener('DOMContentLoaded', async () => {
  await loadReps();
  showView('dashboard');
  startLiveClock();
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
  if (view === 'followups') loadFollowups();
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
    citySel.innerHTML = '<option value="">All Cities</option>' + cities.map(c => `<option value="${c.city}">${c.city} (${c.store_count} stores, ${c.distance_km}km)</option>`).join('');
  }

  const city = document.getElementById('routeCity').value;
  const maxKm = document.getElementById('routeMaxKm').value;
  const limit = document.getElementById('routeLimit').value;

  const res = await fetch(`/api/routes?city=${encodeURIComponent(city)}&max_km=${maxKm}&limit=${limit}`);
  const data = await res.json();

  const countEl = document.getElementById('routeCount');
  if (countEl) countEl.textContent = data.total;

  // Route map link
  const linkDiv = document.getElementById('routeMapLink');
  if (data.route_url && data.stores.length) {
    linkDiv.innerHTML = `<a href="${data.route_url}" target="_blank" class="btn-primary" style="display:inline-block;text-decoration:none">&#128506; Open Route in Google Maps (Top ${Math.min(9, data.stores.length)} stores)</a>`;
  }

  // Store table
  const tbody = document.querySelector('#routesTable tbody');
  tbody.innerHTML = data.stores.map(s => `
    <tr onclick="openStoreModal(${s.id})" style="cursor:pointer">
      <td><strong>${s.distance_km} km</strong></td>
      <td>#${s.store_number}</td>
      <td>${esc(truncate(s.account, 25))}</td>
      <td>${esc(s.city)}</td>
      <td>${s.last_activity ? `<span class="type-badge ${s.last_activity.activity_type}">${formatType(s.last_activity.activity_type)}</span> ${formatDateShort(s.last_activity.created_at)}` : '<span class="muted">Never</span>'}</td>
      <td>${s.activity_count}</td>
      <td><a href="https://www.google.com/maps/dir/43.6558,-79.3628/${s.lat},${s.lng}" target="_blank" class="btn-sm" onclick="event.stopPropagation()">&#128506; Go</a></td>
    </tr>
  `).join('');

  // Cities distance list
  const citiesDiv = document.getElementById('citiesRoute');
  citiesDiv.innerHTML = '<div class="city-route-grid">' + cities.slice(0, 30).map(c =>
    `<div class="city-route-card" onclick="document.getElementById('routeCity').value='${escAttr(c.city)}';loadRoutes()">
      <div class="crc-name">${esc(c.city)}</div>
      <div class="crc-dist">${c.distance_km} km</div>
      <div class="crc-count">${c.store_count} stores</div>
    </div>`
  ).join('') + '</div>';
}

// === FOLLOW-UPS ===
async function loadFollowups() {
  const res = await fetch('/api/followups');
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
  return `<div class="followup-card ${overdue ? 'followup-overdue' : ''}" onclick="openStoreModal(${f.store_id})" style="cursor:pointer">
    <div class="fu-top">
      <span class="fu-store">LCBO #${f.store_number} — ${esc(f.account)}</span>
      <span class="fu-date">${f.follow_up_date}${overdue ? ' (OVERDUE)' : ''}</span>
    </div>
    <div class="fu-meta">${esc(f.rep_name)} · <span class="type-badge ${f.activity_type}">${formatType(f.activity_type)}</span> ${producerTags(f.producer)} ${f.venue_type ? `<span class="venue-tag">${esc(f.venue_type)}</span>` : ''}</div>
    ${f.notes ? `<div class="fu-notes">${esc(truncate(f.notes, 120))}</div>` : ''}
    ${f.city ? `<div class="fu-city">${esc(f.city)}${f.address ? ' — ' + esc(f.address) : ''}</div>` : ''}
  </div>`;
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
