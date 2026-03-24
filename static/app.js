let currentPage = 1;
let activeRepId = null;
let currentModalStoreId = null;
let searchTimeout = null;
let selectedType = null;
let selectedProducer = '';
let selectedVenue = '';

document.addEventListener('DOMContentLoaded', async () => {
  await loadReps();
  showView('dashboard');
});

// --- Navigation ---
function showView(view) {
  document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
  document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('view-' + view).classList.add('active');
  document.querySelector(`[data-view="${view}"]`).classList.add('active');

  if (view === 'dashboard') loadDashboard();
  if (view === 'stores') { loadCities(); loadStores(); }
  if (view === 'producers') loadProducerData();
  if (view === 'followups') loadFollowups();
}

// --- Reps ---
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

// --- Dashboard ---
async function loadDashboard() {
  const res = await fetch('/api/dashboard');
  const d = await res.json();
  const types = d.by_type || {};
  const prods = d.by_producer || {};

  document.getElementById('statsGrid').innerHTML = `
    <div class="stat-card accent"><div class="label">Total Stores</div><div class="value">${d.total_stores}</div></div>
    <div class="stat-card green"><div class="label">Activities Logged</div><div class="value">${d.total_activities}</div></div>
    <div class="stat-card blue"><div class="label">Site Visits</div><div class="value">${types.site_visit || 0}</div></div>
    <div class="stat-card orange"><div class="label">Tastings</div><div class="value">${types.tasting || 0}</div></div>
    <div class="stat-card" style="border-left:3px solid #e17055"><div class="label">NB Distillers</div><div class="value" style="color:#e17055">${prods['NB Distillers'] || 0}</div></div>
    <div class="stat-card" style="border-left:3px solid #00cec9"><div class="label">Anu Portfolio</div><div class="value" style="color:#00cec9">${prods['Anu Portfolio'] || 0}</div></div>
  `;

  const tbody = document.querySelector('#recentTable tbody');
  if (!d.recent.length) {
    tbody.innerHTML = '<tr><td colspan="6" class="muted">No activity yet. Start logging!</td></tr>';
    return;
  }
  tbody.innerHTML = d.recent.map(a => `
    <tr onclick="openStoreModal(${a.store_id})">
      <td>${formatDate(a.created_at)}</td>
      <td>${esc(a.rep_name)}</td>
      <td>LCBO #${a.store_number}</td>
      <td>${a.producer ? `<span class="producer-tag ${a.producer === 'NB Distillers' ? 'nb' : 'anu'}">${esc(a.producer)}</span>` : '—'}</td>
      <td>${formatType(a.activity_type)}</td>
      <td>${esc(truncate(a.notes, 50))}</td>
    </tr>
  `).join('');
}

// --- Stores ---
async function loadCities() {
  const res = await fetch('/api/cities');
  const cities = await res.json();
  const sel = document.getElementById('cityFilter');
  sel.innerHTML = '<option value="">All Cities</option>' +
    cities.map(c => `<option value="${c}">${c}</option>`).join('');
}

function debounceSearch() {
  clearTimeout(searchTimeout);
  searchTimeout = setTimeout(() => { currentPage = 1; loadStores(); }, 300);
}

async function loadStores() {
  const search = document.getElementById('storeSearch').value;
  const city = document.getElementById('cityFilter').value;
  const res = await fetch(`/api/stores?search=${encodeURIComponent(search)}&city=${encodeURIComponent(city)}&page=${currentPage}&per_page=50`);
  const data = await res.json();

  const tbody = document.querySelector('#storesTable tbody');
  tbody.innerHTML = data.stores.map(s => `
    <tr onclick="openStoreModal(${s.id})">
      <td><strong>${s.store_number}</strong></td>
      <td>${esc(s.account)}</td>
      <td>${esc(s.city)}</td>
      <td>${esc(s.manager_name || '—')}</td>
      <td>${esc(s.manager_phone || s.phone || '—')}</td>
      <td>${statusBadge(s.status)}</td>
      <td>${s.producer ? `<span class="producer-tag ${s.producer === 'NB Distillers' ? 'nb' : 'anu'}">${esc(truncate(s.producer,10))}</span>` : ''}</td>
      <td><button class="btn-sm" onclick="event.stopPropagation(); openStoreModal(${s.id})">View</button></td>
    </tr>
  `).join('');

  renderPagination(data.page, data.pages);
}

function renderPagination(page, pages) {
  const div = document.getElementById('pagination');
  if (pages <= 1) { div.innerHTML = ''; return; }
  let btns = [];
  if (page > 1) btns.push(`<button onclick="goPage(${page-1})">&laquo; Prev</button>`);
  let start = Math.max(1, page - 3), end = Math.min(pages, page + 3);
  for (let i = start; i <= end; i++) {
    btns.push(`<button class="${i === page ? 'active' : ''}" onclick="goPage(${i})">${i}</button>`);
  }
  if (page < pages) btns.push(`<button onclick="goPage(${page+1})">Next &raquo;</button>`);
  div.innerHTML = btns.join('');
}

function goPage(p) { currentPage = p; loadStores(); }

// --- Store Modal ---
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
  document.getElementById('saveSuccess').style.display = 'none';

  switchTab('info', document.querySelector('.tab-btn'));
  document.getElementById('storeModal').style.display = 'flex';
  loadModalActivities(storeId);
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

  await fetch(`/api/stores/${id}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data)
  });

  document.querySelectorAll('.success-msg').forEach(m => {
    m.style.display = 'inline';
    setTimeout(() => m.style.display = 'none', 2000);
  });
  loadStores();
}

function switchTab(tab, el) {
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('tab-' + tab).classList.add('active');
  el.classList.add('active');
  if (tab === 'activities' && currentModalStoreId) loadModalActivities(currentModalStoreId);
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
        <span class="ac-type ${a.activity_type}">${formatType(a.activity_type)}</span>
        <span class="ac-date">${formatDate(a.created_at)}</span>
      </div>
      <div class="ac-rep">by ${esc(a.rep_name)} ${a.producer ? `· <span class="producer-tag ${a.producer === 'NB Distillers' ? 'nb' : 'anu'}">${esc(a.producer)}</span>` : ''} ${a.venue_type ? `· <span class="venue-tag">${esc(a.venue_type)}</span>` : ''}</div>
      ${a.notes ? `<div class="ac-notes">${esc(a.notes)}</div>` : ''}
      ${a.follow_up_date ? `<div style="font-size:11px;color:var(--orange);margin-top:4px">Follow-up: ${a.follow_up_date}</div>` : ''}
    </div>
  `).join('');
}

function filterActivities(type, el) {
  document.querySelectorAll('.filter-pill').forEach(p => p.classList.remove('active'));
  el.classList.add('active');
  if (currentModalStoreId) loadModalActivities(currentModalStoreId, type);
}

// --- Producers Tab ---
async function loadProducerData() {
  const res = await fetch('/api/dashboard');
  const d = await res.json();
  const types = d.by_type || {};

  // Load NB Distillers activities
  const nbRes = await fetch('/api/export/activities');
  const nbText = await nbRes.text();
  const lines = nbText.split('\n').slice(1).filter(l => l.trim());

  let nbCount = 0, anuCount = 0;
  let nbActivities = [], anuActivities = [];

  for (const line of lines) {
    const parts = parseCSVLine(line);
    if (parts.length >= 8) {
      const producer = parts[5];
      const entry = { store: parts[0], account: parts[1], city: parts[2], rep: parts[3], type: parts[4], producer: parts[5], notes: parts[6], date: parts[7] };
      if (producer === 'NB Distillers') { nbCount++; nbActivities.push(entry); }
      if (producer === 'Anu Portfolio') { anuCount++; anuActivities.push(entry); }
    }
  }

  document.getElementById('nbStats').innerHTML = `
    <div class="stat-card" style="border-left:3px solid #e17055"><div class="label">Total Activities</div><div class="value" style="color:#e17055">${nbCount}</div></div>
    <div class="stat-card blue"><div class="label">Stores Touched</div><div class="value">${new Set(nbActivities.map(a => a.store)).size}</div></div>
  `;

  document.getElementById('anuStats').innerHTML = `
    <div class="stat-card" style="border-left:3px solid #00cec9"><div class="label">Total Activities</div><div class="value" style="color:#00cec9">${anuCount}</div></div>
    <div class="stat-card blue"><div class="label">Stores Touched</div><div class="value">${new Set(anuActivities.map(a => a.store)).size}</div></div>
  `;

  document.getElementById('nbActivities').innerHTML = nbActivities.length
    ? nbActivities.slice(0, 20).map(a => activityCardHTML(a)).join('')
    : '<p class="muted">No NB Distillers activities yet. Log one to see it here.</p>';

  document.getElementById('anuActivities').innerHTML = anuActivities.length
    ? anuActivities.slice(0, 20).map(a => activityCardHTML(a)).join('')
    : '<p class="muted">No Anu Portfolio activities yet. Log one to see it here.</p>';
}

function activityCardHTML(a) {
  return `<div class="activity-card">
    <div class="ac-header">
      <span class="ac-type ${a.type}">${formatType(a.type)}</span>
      <span class="ac-date">${a.date || '—'}</span>
    </div>
    <div class="ac-rep">LCBO #${esc(a.store)} · ${esc(a.rep)}</div>
    ${a.notes ? `<div class="ac-notes">${esc(a.notes)}</div>` : ''}
  </div>`;
}

function parseCSVLine(line) {
  const result = [];
  let current = '', inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    if (line[i] === '"') { inQuotes = !inQuotes; }
    else if (line[i] === ',' && !inQuotes) { result.push(current.trim()); current = ''; }
    else { current += line[i]; }
  }
  result.push(current.trim());
  return result;
}

function switchProducerView(id, el) {
  document.querySelectorAll('.producer-panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.producer-tab').forEach(t => t.classList.remove('active'));
  document.getElementById('producer-' + id).classList.add('active');
  el.classList.add('active');
}

// --- Log Activity ---
function selectProducer(el) {
  document.querySelectorAll('.producer-btn').forEach(b => b.classList.remove('active'));
  el.classList.add('active');
  selectedProducer = el.dataset.producer;
}

function selectVenue(el) {
  document.querySelectorAll('.venue-btn').forEach(b => b.classList.remove('active'));
  el.classList.add('active');
  selectedVenue = el.dataset.venue;
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
    <div class="dropdown-item" onclick="selectLogStore(${s.id}, ${s.store_number}, '${esc(s.account)}', '${esc(s.address)}', '${esc(s.city)}')">
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
  const res = await fetch(`/api/activities/${storeId}`);
  const acts = await res.json();
  const summRes = await fetch(`/api/activities/summary/${storeId}`);
  const summary = await summRes.json();
  const div = document.getElementById('storeHistory');

  let summaryHtml = '<div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:16px">';
  for (const t of ['tasting', 'site_visit', 'listing', 'email', 'call']) {
    const s = summary[t];
    summaryHtml += `<div style="text-align:center;min-width:70px">
      <div style="font-size:20px;font-weight:700;color:var(--accent-light)">${s ? s.count : 0}</div>
      <div style="font-size:10px;color:var(--muted);text-transform:uppercase">${formatType(t)}</div>
    </div>`;
  }
  summaryHtml += '</div>';

  if (!acts.length) { div.innerHTML = summaryHtml + '<p class="muted">No activities yet.</p>'; return; }
  div.innerHTML = summaryHtml + acts.slice(0, 10).map(a => `
    <div class="activity-card">
      <div class="ac-header">
        <span class="ac-type ${a.activity_type}">${formatType(a.activity_type)}</span>
        <span class="ac-date">${formatDate(a.created_at)}</span>
      </div>
      <div class="ac-rep">by ${esc(a.rep_name)} ${a.producer ? `· ${esc(a.producer)}` : ''}</div>
      ${a.notes ? `<div class="ac-notes">${esc(a.notes)}</div>` : ''}
    </div>
  `).join('');
}

function selectType(el) {
  document.querySelectorAll('#activityTypes .type-btn').forEach(b => b.classList.remove('active'));
  el.classList.add('active');
  selectedType = el.dataset.type;
}

async function submitActivity() {
  const storeId = document.getElementById('logStoreId').value;
  const notes = document.getElementById('logNotes').value.trim();
  if (!storeId) { alert('Please select a store'); return; }
  if (!selectedType) { alert('Please select an activity type'); return; }
  if (!activeRepId) { alert('Please select a rep'); return; }
  if (!selectedProducer) { alert('Please select a producer (NB Distillers or Anu Portfolio)'); return; }

  const followUpDate = document.getElementById('logFollowUpDate').value || '';

  await fetch('/api/activities', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      store_id: parseInt(storeId),
      rep_id: activeRepId,
      activity_type: selectedType,
      producer: selectedProducer,
      venue_type: selectedVenue,
      notes: notes,
      follow_up_date: followUpDate
    })
  });

  const msg = document.getElementById('logSuccess');
  msg.style.display = 'inline';
  setTimeout(() => msg.style.display = 'none', 2500);
  document.getElementById('logNotes').value = '';
  document.getElementById('logFollowUpDate').value = '';
  document.querySelectorAll('#activityTypes .type-btn').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.venue-btn').forEach(b => b.classList.remove('active'));
  selectedType = null;
  selectedVenue = '';
  loadStoreHistoryPanel(storeId);
}

document.addEventListener('click', e => {
  if (!e.target.closest('.form-group')) {
    document.getElementById('logStoreResults').classList.remove('show');
  }
});

// --- Follow-Ups ---
async function loadFollowups() {
  const res = await fetch('/api/followups');
  const followups = await res.json();
  const div = document.getElementById('followupsList');
  const today = new Date().toISOString().split('T')[0];

  if (!followups.length) {
    div.innerHTML = '<p class="muted">No follow-ups scheduled. Set a follow-up date when logging an activity.</p>';
    return;
  }

  // Split into overdue, today, upcoming
  const overdue = followups.filter(f => f.follow_up_date < today);
  const todayItems = followups.filter(f => f.follow_up_date === today);
  const upcoming = followups.filter(f => f.follow_up_date > today);

  let html = '';

  if (overdue.length) {
    html += '<h4 style="color:var(--red);margin-bottom:10px;font-size:14px">OVERDUE</h4>';
    html += overdue.map(f => followupCardHTML(f, true)).join('');
  }
  if (todayItems.length) {
    html += '<h4 style="color:var(--orange);margin:16px 0 10px;font-size:14px">TODAY</h4>';
    html += todayItems.map(f => followupCardHTML(f, false)).join('');
  }
  if (upcoming.length) {
    html += '<h4 style="color:var(--green);margin:16px 0 10px;font-size:14px">UPCOMING</h4>';
    html += upcoming.map(f => followupCardHTML(f, false)).join('');
  }

  div.innerHTML = html;
}

function followupCardHTML(f, overdue) {
  return `<div class="followup-card ${overdue ? 'followup-overdue' : ''}" onclick="openStoreModal(${f.store_id})" style="cursor:pointer">
    <div style="display:flex;justify-content:space-between;align-items:center">
      <span class="fu-store">LCBO #${f.store_number} — ${esc(f.account)}</span>
      <span class="fu-date">${f.follow_up_date}${overdue ? ' (OVERDUE)' : ''}</span>
    </div>
    <div style="font-size:12px;color:var(--muted);margin-top:2px">${esc(f.rep_name)} · ${formatType(f.activity_type)} ${f.producer ? '· ' + esc(f.producer) : ''} ${f.venue_type ? '· ' + esc(f.venue_type) : ''}</div>
    ${f.notes ? `<div class="fu-notes">${esc(truncate(f.notes, 100))}</div>` : ''}
  </div>`;
}

// --- Helpers ---
function formatType(t) {
  const map = { tasting: 'Tasting', site_visit: 'Site Visit', listing: 'Listing', email: 'Email', call: 'Call' };
  return map[t] || t;
}

function formatDate(d) {
  if (!d) return '—';
  const dt = new Date(d + (d.includes('Z') ? '' : 'Z'));
  return dt.toLocaleDateString('en-CA', { month: 'short', day: 'numeric', year: 'numeric' }) +
    ' ' + dt.toLocaleTimeString('en-CA', { hour: '2-digit', minute: '2-digit' });
}

function statusBadge(s) {
  if (!s) return '';
  const cls = { 'Won': 'badge-won', 'Pitched': 'badge-pitched', 'Lost': 'badge-lost', 'Not Active': 'badge-not-active', 'Follow Up': 'badge-follow-up' };
  return `<span class="badge ${cls[s] || ''}">${esc(s)}</span>`;
}

function truncate(s, n) { if (!s) return ''; return s.length > n ? s.slice(0, n) + '...' : s; }

function esc(s) {
  if (!s) return '';
  const el = document.createElement('span');
  el.textContent = s;
  return el.innerHTML;
}
