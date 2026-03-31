require('dotenv').config();
const express  = require('express');
const axios    = require('axios');
const path     = require('path');
const fs       = require('fs');
const crypto   = require('crypto');
const Anthropic = require('@anthropic-ai/sdk');
const xmlrpc   = require('xmlrpc');
const twilio   = require('twilio');

const app = express();
app.use(express.json({ limit: '25mb' }));
const PORT = 3000;

// ── Sesiones (definidas temprano para que requireToken pueda usarlas) ──
const sessions = new Map();
function getSession(token) {
  const s = sessions.get(token);
  if (!s) return null;
  if (s.expiresAt < Date.now()) { sessions.delete(token); return null; }
  return s;
}

const { CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, ANTHROPIC_API_KEY } = process.env;
const anthropic = ANTHROPIC_API_KEY ? new Anthropic({ apiKey: ANTHROPIC_API_KEY }) : null;
const ML_AUTH_URL = 'https://auth.mercadolibre.com.uy';
const ML_API_URL = 'https://api.mercadolibre.com';

let tokenData      = null;
let cachedClaims   = [];
let pendingRedirect = '/';

// ── Persistencia del token ML ────────────────────────────────────
const TOKEN_FILE = path.join(__dirname, 'data', 'ml_token.json');
function saveToken(data) {
  try { fs.writeFileSync(TOKEN_FILE, JSON.stringify(data), 'utf8'); } catch(e) {}
}
function loadToken() {
  try {
    if (fs.existsSync(TOKEN_FILE)) {
      const t = JSON.parse(fs.readFileSync(TOKEN_FILE, 'utf8'));
      if (t?.access_token) { tokenData = t; console.log('[token] cargado desde disco'); }
    }
  } catch(e) {}
}
loadToken();

// Cache de publicaciones (se carga una sola vez, persiste en disco)
let cachedItemIds = [];
let cachedItems   = [];
const STOCK_FILE  = path.join(__dirname, 'data', 'stock_cache.json');

function loadStockFromDisk() {
  try {
    if (fs.existsSync(STOCK_FILE)) {
      const saved = JSON.parse(fs.readFileSync(STOCK_FILE, 'utf8'));
      cachedItems   = saved.items   || [];
      cachedItemIds = cachedItems.map(i => i.id);
      console.log(`[stock] cache cargado desde disco: ${cachedItems.length} publicaciones`);
    }
  } catch(e) { console.error('[stock] error leyendo cache:', e.message); }
}

function saveStockToDisk() {
  try {
    fs.writeFileSync(STOCK_FILE, JSON.stringify({ items: cachedItems, savedAt: new Date().toISOString() }), 'utf8');
  } catch(e) { console.error('[stock] error guardando cache:', e.message); }
}

// Stats históricos por mes — { 'YYYY-MM': { count, revenue, units, by_status, lastFetched } }
let monthlyStats = {};
let syncState    = { running: false, done: false, progress: 0, total: 36, currentMonth: null, error: null };

const MONTHLY_FILE = path.join(__dirname, 'data', 'monthly_cache.json');

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function pad(n) { return String(n).padStart(2, '0'); }

function loadMonthlyFromDisk() {
  try {
    if (fs.existsSync(MONTHLY_FILE)) {
      monthlyStats = JSON.parse(fs.readFileSync(MONTHLY_FILE, 'utf8'));
      const n = Object.keys(monthlyStats).length;
      if (n > 0) { syncState.done = true; syncState.progress = n; }
      console.log(`[historico] cargado desde disco: ${n} meses`);
    }
  } catch(e) { console.error('[historico] error leyendo cache:', e.message); }
}

function saveMonthlyToDisk() {
  try {
    if (!fs.existsSync(path.dirname(MONTHLY_FILE))) fs.mkdirSync(path.dirname(MONTHLY_FILE), { recursive: true });
    fs.writeFileSync(MONTHLY_FILE, JSON.stringify(monthlyStats), 'utf8');
  } catch(e) { console.error('[historico] error guardando cache:', e.message); }
}

function getLast3YearsMonths() {
  const now = new Date();
  const months = [];
  for (let i = 35; i >= 0; i--) {
    const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
    months.push({ year: d.getFullYear(), month: d.getMonth() + 1 });
  }
  return months;
}

// Barrido completo usando scroll cursor (sin límite de offset).
// ML devuelve un scroll_id nuevo en cada respuesta; lo usamos como cursor.
async function runSync(force = false) {
  if (syncState.running) return;

  const headers     = { Authorization: `Bearer ${tokenData.access_token}` };
  const uid         = tokenData.user_id;
  const threeYrsAgo = new Date();
  threeYrsAgo.setFullYear(threeYrsAgo.getFullYear() - 3);

  if (!force) {
    const now     = new Date();
    const current = `${now.getFullYear()}-${pad(now.getMonth() + 1)}`;
    const pending = getLast3YearsMonths().filter(({ year, month }) => {
      const key = `${year}-${pad(month)}`;
      return !monthlyStats[key] || key === current;
    });
    if (!pending.length) { syncState.done = true; return; }
  }

  console.log(`[historico] iniciando scroll completo (force=${force})`);
  syncState = { running: true, done: false, progress: 0, total: null, currentMonth: null, error: null };

  const sweep    = {};
  let fetched    = 0;
  let page       = 0;
  let scrollId   = null;

  while (true) {
    let r;
    try {
      // scan no soporta sort → devuelve en orden interno (cronológico ascendente)
      // no cortamos por fecha, dejamos correr hasta el final
      const params = { seller: uid, limit: 50, search_type: 'scan' };
      if (scrollId) params.scroll_id = scrollId;

      r = await axios.get(`${ML_API_URL}/orders/search`, { headers, params });
    } catch(e) {
      console.error(`[historico] error pág ${page}:`, e.message);
      break;
    }

    const results    = r.data.results || [];
    const nextScroll = r.data.scroll_id;
    page++;

    if (syncState.total === null) syncState.total = r.data.paging?.total || 0;

    for (const order of results) {
      const d = new Date(order.date_created);
      if (d < threeYrsAgo) continue; // filtrar viejos pero no parar
      const key = `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}`;
      if (!sweep[key]) sweep[key] = [];
      sweep[key].push(order);
      fetched++;
    }

    const lastDate = results.length ? new Date(results[results.length - 1].date_created) : null;
    syncState.progress     = page * 50;
    syncState.currentMonth = lastDate
      ? `${lastDate.getUTCFullYear()}-${pad(lastDate.getUTCMonth() + 1)}`
      : null;

    // Log cada 10 páginas + resumen de meses acumulados
    if (page % 10 === 0 || page === 1) {
      const meses = Object.entries(sweep)
        .sort(([a], [b]) => b.localeCompare(a))
        .slice(0, 5)
        .map(([k, v]) => `${k}(${v.length})`)
        .join(' ');
      console.log(`[historico] pág ${page} | ${fetched} órdenes | último: ${lastDate?.toISOString().slice(0,10) || '?'} | meses: ${meses || '—'}`);
    }

    if (!results.length || !nextScroll) break;
    scrollId = nextScroll;
    await sleep(300);
  }

  // Computar stats por mes
  const PAID = new Set(['paid', 'confirmed']);
  for (const [key, orders] of Object.entries(sweep)) {
    const paid = orders.filter(o => PAID.has(o.status));
    const by_status = {};
    for (const o of orders) by_status[o.status] = (by_status[o.status] || 0) + 1;
    monthlyStats[key] = {
      count:       orders.length,
      revenue:     paid.reduce((s, o) => s + (o.total_amount || 0), 0),
      units:       paid.reduce((s, o) => s + (o.order_items||[]).reduce((q, oi) => q + (oi.quantity||1), 0), 0),
      by_status,
      lastFetched: new Date().toISOString(),
    };
    console.log(`[historico] ${key} — ${monthlyStats[key].count} órdenes, $${Math.round(monthlyStats[key].revenue).toLocaleString('es-UY')}`);
  }

  saveMonthlyToDisk();
  console.log(`[historico] scroll completo — ${fetched} órdenes, ${Object.keys(sweep).length} meses cubiertos`);
  syncState.running = false;
  syncState.done    = true;
  syncState.currentMonth = null;
}

loadMonthlyFromDisk();
loadStockFromDisk();

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// PKCE helpers
let pkceVerifier = null;
function generateCodeVerifier() {
  return crypto.randomBytes(32).toString('base64url');
}
function generateCodeChallenge(verifier) {
  return crypto.createHash('sha256').update(verifier).digest('base64url');
}

// GET /login
app.get('/login', (req, res) => {
  pkceVerifier = generateCodeVerifier();
  const challenge = generateCodeChallenge(pkceVerifier);
  const authUrl =
    `${ML_AUTH_URL}/authorization` +
    `?response_type=code` +
    `&client_id=${CLIENT_ID}` +
    `&redirect_uri=${encodeURIComponent(REDIRECT_URI)}` +
    `&scope=read_orders+offline_access` +
    `&code_challenge=${challenge}` +
    `&code_challenge_method=S256`;
  res.redirect(authUrl);
});

// GET /callback
app.get('/callback', async (req, res) => {
  const { code } = req.query;
  if (!code) return res.status(400).json({ error: 'Falta el parámetro code' });
  try {
    const response = await axios.post(`${ML_API_URL}/oauth/token`, {
      grant_type: 'authorization_code',
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
      code,
      redirect_uri: REDIRECT_URI,
      code_verifier: pkceVerifier,
    });
    pkceVerifier = null;
    tokenData = response.data;
    saveToken(tokenData);
    // Arrancar refreshes en background
    runSync();
    refreshStockCache();
    const dest = pendingRedirect || '/';
    pendingRedirect = '/';
    res.redirect(dest);
  } catch (err) {
    res.status(500).json({ error: 'Error al obtener el token', detail: err.response?.data || err.message });
  }
});

function requireToken(req, res, next) {
  // Verificar sesión de usuario
  const sessionTok = req.headers['x-session-token'] || req.query._token;
  if (sessionTok && !getSession(sessionTok)) {
    return res.status(401).json({ error: 'Sesión inválida' });
  }
  if (!tokenData?.access_token) {
    const isHtml = req.headers.accept?.includes('text/html');
    if (isHtml) {
      pendingRedirect = req.originalUrl;
      return res.redirect('/login');
    }
    return res.status(401).json({ error: 'No autenticado con ML' });
  }
  next();
}

app.get('/api/auth-status', (req, res) => {
  res.json({ authenticated: !!tokenData?.access_token });
});

app.get('/api/user', requireToken, async (req, res) => {
  try {
    const r = await axios.get(`${ML_API_URL}/users/${tokenData.user_id}`, {
      headers: { Authorization: `Bearer ${tokenData.access_token}` },
    });
    res.json(r.data);
  } catch (err) {
    res.status(500).json({ error: err.response?.data || err.message });
  }
});

// GET /api/sync — inicia sync en background
app.get('/api/sync', requireToken, (req, res) => {
  const force = req.query.force === 'true';
  if (!syncState.running) runSync(force);
  res.json({ started: true, state: syncState });
});

// GET /api/sync/status
app.get('/api/sync/status', requireToken, (req, res) => {
  res.json({ ...syncState, cached: Object.keys(monthlyStats).length });
});

// GET /api/ordenes — siempre desde ML en tiempo real con paginación
app.get('/api/ordenes', requireToken, async (req, res) => {
  const offset = parseInt(req.query.offset) || 0;
  const limit  = Math.min(parseInt(req.query.limit) || 50, 50);
  try {
    const r = await axios.get(`${ML_API_URL}/orders/search`, {
      headers: { Authorization: `Bearer ${tokenData.access_token}` },
      params: { seller: tokenData.user_id, offset, limit, sort: 'date_desc' },
    });
    res.json(r.data);
  } catch (err) {
    res.status(500).json({ error: err.response?.data || err.message });
  }
});

// GET /api/stats — KPIs desde muestra reciente + gráfico desde monthlyStats si existe
app.get('/api/stats', requireToken, async (req, res) => {
  try {
    // Muestra reciente para KPIs (últimas 200 órdenes)
    const pages = await Promise.all(
      [0, 50, 100, 150].map(o =>
        axios.get(`${ML_API_URL}/orders/search`, {
          headers: { Authorization: `Bearer ${tokenData.access_token}` },
          params: { seller: tokenData.user_id, offset: o, limit: 50, sort: 'date_desc' },
        })
      )
    );

    const recentOrders = pages.flatMap(p => p.data.results || []);
    const total = pages[0].data.paging?.total || 0;

    const byStatus = {};
    const byMonthRecent = {};
    recentOrders.forEach(o => {
      byStatus[o.status] = (byStatus[o.status] || 0) + 1;
      const d   = new Date(o.date_created);
      const key = `${d.getFullYear()}-${pad(d.getMonth() + 1)}`;
      if (!byMonthRecent[key]) byMonthRecent[key] = { count: 0, amount: 0 };
      byMonthRecent[key].count++;
      byMonthRecent[key].amount += o.total_amount || 0;
    });

    const totalRevenue = recentOrders
      .filter(o => o.status === 'paid')
      .reduce((s, o) => s + (o.total_amount || 0), 0);

    // Usar monthlyStats para el gráfico si ya se sincronizó
    let byMonth = byMonthRecent;
    let synced  = false;
    let historicRevenue = 0;
    let historicUnits   = 0;
    if (Object.keys(monthlyStats).length > 0) {
      byMonth = {};
      Object.entries(monthlyStats).forEach(([k, v]) => {
        byMonth[k] = { count: v.count, amount: v.revenue, units: v.units || 0 };
        historicRevenue += v.revenue || 0;
        historicUnits   += v.units   || 0;
      });
      synced = true;
    }

    res.json({
      total_orders: total, total_revenue: totalRevenue,
      historic_revenue: historicRevenue, historic_units: historicUnits,
      by_status: byStatus, by_month: byMonth,
      synced, sync_months: Object.keys(monthlyStats).length,
    });
  } catch (err) {
    res.status(500).json({ error: err.response?.data || err.message });
  }
});

// POST /notifications — webhook de MercadoLibre (claims, orders, etc.)
app.post('/notifications', async (req, res) => {
  // ML requiere respuesta 200 inmediata
  res.sendStatus(200);

  const { topic, resource } = req.body || {};
  if (!resource || !tokenData?.access_token) return;

  // Solo procesar notificaciones de reclamos
  if (topic !== 'claims' && topic !== 'claims_actions') return;

  // El resource viene como "/post-purchase/v1/claims/5281510459"
  const claimId = String(resource).split('/').pop();
  if (!claimId || isNaN(claimId)) return;

  try {
    const r = await axios.get(`${ML_API_URL}/post-purchase/v1/claims/${claimId}`, {
      headers: { Authorization: `Bearer ${tokenData.access_token}` },
    });
    const claim = r.data;
    const idx = cachedClaims.findIndex(c => c.id === claim.id);
    if (idx >= 0) cachedClaims[idx] = claim; // actualizar si ya existe
    else cachedClaims.unshift(claim);         // agregar nuevo al inicio
    console.log(`[claim] ${topic} — ID ${claimId} guardado (total: ${cachedClaims.length})`);
  } catch (e) {
    console.error(`[claim] Error al obtener claim ${claimId}:`, e.response?.data || e.message);
  }
});

// GET /api/reclamos — reclamos acumulados por notificaciones
app.get('/api/reclamos', requireToken, (req, res) => {
  const offset = parseInt(req.query.offset) || 0;
  const limit  = parseInt(req.query.limit)  || 50;
  const status = req.query.status || '';

  let claims = cachedClaims;
  if (status) claims = claims.filter(c => c.status === status);

  res.json({
    data: claims.slice(offset, offset + limit),
    paging: { total: claims.length, offset, limit },
  });
});

// GET /api/reclamos/stats
app.get('/api/reclamos/stats', requireToken, (req, res) => {
  const byStatus = {};
  const byType   = {};
  const byReason = {};

  cachedClaims.forEach(c => {
    byStatus[c.status] = (byStatus[c.status] || 0) + 1;
    byType[c.type]     = (byType[c.type]     || 0) + 1;
    if (c.resolution?.reason) byReason[c.resolution.reason] = (byReason[c.resolution.reason] || 0) + 1;
  });

  res.json({ total: cachedClaims.length, by_status: byStatus, by_type: byType, by_reason: byReason });
});

// GET /api/reclamos/scan — escanea órdenes canceladas y carga reclamos históricos
let scanState = { running: false, done: false, checked: 0, total: 0, found: 0, error: null };

async function runClaimsScan(months = 3) {
  if (scanState.running) return;
  scanState = { running: true, done: false, checked: 0, total: 0, found: 0, error: null };

  const headers = { Authorization: `Bearer ${tokenData.access_token}` };
  const from = new Date();
  from.setMonth(from.getMonth() - months);
  const fromStr = from.toISOString().slice(0, 19) + '.000-00:00';

  try {
    // Traer total de órdenes canceladas
    const first = await axios.get(`${ML_API_URL}/orders/search`, {
      headers, params: { seller: tokenData.user_id, 'order.status': 'cancelled', 'date_created.from': fromStr, limit: 1 },
    });
    scanState.total = first.data.paging?.total || 0;

    let offset = 0;
    while (offset < scanState.total) {
      const r = await axios.get(`${ML_API_URL}/orders/search`, {
        headers, params: { seller: tokenData.user_id, 'order.status': 'cancelled', 'date_created.from': fromStr, offset, limit: 50 },
      });
      const orders = r.data.results || [];

      await Promise.all(orders.map(async o => {
        try {
          const cr = await axios.get(`${ML_API_URL}/post-purchase/v1/claims/search`, {
            headers, params: { resource_id: o.id, resource: 'order', limit: 10 },
          });
          const claims = cr.data.data || [];
          claims.forEach(claim => {
            const exists = cachedClaims.find(c => c.id === claim.id);
            if (!exists) { cachedClaims.push(claim); scanState.found++; }
          });
        } catch {}
        scanState.checked++;
      }));

      offset += 50;
      await sleep(200);
    }

    cachedClaims.sort((a, b) => new Date(b.date_created) - new Date(a.date_created));
  } catch (e) {
    scanState.error = e.message;
  }

  scanState.running = false;
  scanState.done = true;
}

app.get('/api/reclamos/scan', requireToken, (req, res) => {
  const months = parseInt(req.query.months) || 3;
  if (!scanState.running) runClaimsScan(months);
  res.json({ started: true, state: scanState });
});

app.get('/api/reclamos/scan/status', requireToken, (req, res) => {
  res.json(scanState);
});

// GET /api/debug/order-claims — busca claims en órdenes canceladas recientes
app.get('/api/debug/order-claims', requireToken, async (req, res) => {
  const headers = { Authorization: `Bearer ${tokenData.access_token}` };
  const uid = tokenData.user_id;

  // Traer 10 órdenes canceladas
  const ordersRes = await axios.get(`${ML_API_URL}/orders/search`, {
    headers, params: { seller: uid, 'order.status': 'cancelled', limit: 10, sort: 'date_desc' },
  });
  const orders = ordersRes.data.results || [];

  const results = await Promise.all(orders.map(async o => {
    try {
      const r = await axios.get(`${ML_API_URL}/post-purchase/v1/claims/search`, {
        headers, params: { resource_id: o.id, resource: 'order', limit: 5 },
      });
      return { order_id: o.id, total_claims: r.data.paging?.total, claims: r.data.data };
    } catch (e) {
      return { order_id: o.id, error: e.response?.data?.error };
    }
  }));

  res.json({ orders_checked: orders.length, results });
});

let cachedStock     = [];   // resultado final procesado
let stockFetching   = false;
let stockLastUpdate = null;
const STOCK_RESULT_FILE = path.join(__dirname, 'data', 'stock_result.json');

// Cargar resultado de stock desde disco
try {
  if (fs.existsSync(STOCK_RESULT_FILE)) {
    const s = JSON.parse(fs.readFileSync(STOCK_RESULT_FILE, 'utf8'));
    cachedStock     = s.items     || [];
    stockLastUpdate = s.savedAt   || null;
    console.log(`[stock] resultado cargado desde disco: ${cachedStock.length} items`);
  }
} catch(e) { console.error('[stock] error leyendo resultado:', e.message); }

async function refreshStockCache(forceRefresh = false) {
  if (stockFetching) return;
  if (!tokenData?.access_token) return;
  stockFetching = true;
  console.log(`[stock] iniciando refresh (force=${forceRefresh})`);

  const headers = { Authorization: `Bearer ${tokenData.access_token}` };
  const uid = tokenData.user_id;

  try {
    let items;

    if (cachedItems.length > 0 && !forceRefresh) {
      console.log(`[stock] actualizando stock de ${cachedItems.length} items cacheados`);
      const allIds = cachedItems.map(i => i.id);
      const freshItems = [];
      for (let i = 0; i < allIds.length; i += 20) {
        const batch = allIds.slice(i, i + 20);
        const r = await axios.get(`${ML_API_URL}/items`, { headers, params: { ids: batch.join(',') } });
        const details = (r.data || []).map(e => e.body).filter(Boolean);
        freshItems.push(...details);
        await sleep(100);
      }
      items = freshItems;
      cachedItems = items;
    } else {
      // Carga completa: traer IDs + detalles de publicaciones
      console.log(`[stock] ${forceRefresh ? 'Refresh forzado' : 'Primera carga'}: trayendo lista de publicaciones...`);

      async function fetchItemIds(status) {
        const ids = [];
        // Primera página para saber el total
        const first = await axios.get(`${ML_API_URL}/users/${uid}/items/search`, {
          headers, params: { status, limit: 50, offset: 0 },
        });
        const total = first.data.paging?.total || 0;
        ids.push(...(first.data.results || []));
        console.log(`[stock] ${status}: ${ids.length}/${total}`);

        // Páginas siguientes en paralelo
        const offsets = [];
        for (let o = 50; o < total; o += 50) offsets.push(o);

        for (let i = 0; i < offsets.length; i += 5) {
          const batch = offsets.slice(i, i + 5);
          const pages = await Promise.allSettled(
            batch.map(o => axios.get(`${ML_API_URL}/users/${uid}/items/search`, {
              headers, params: { status, limit: 50, offset: o },
            }))
          );
          pages.forEach(p => {
            if (p.status === 'fulfilled') ids.push(...(p.value.data.results || []));
          });
          console.log(`[stock] ${status}: ${ids.length}/${total}`);
          await sleep(150);
        }
        return ids;
      }

      const [activeIds, pausedIds] = await Promise.all([fetchItemIds('active'), fetchItemIds('paused')]);
      const allIds = [...new Set([...activeIds, ...pausedIds])];
      cachedItemIds = allIds;
      console.log(`[stock] Total IDs únicos: ${allIds.length}`);

      items = [];
      for (let i = 0; i < allIds.length; i += 20) {
        const batch = allIds.slice(i, i + 20);
        const r = await axios.get(`${ML_API_URL}/items`, { headers, params: { ids: batch.join(',') } });
        const details = (r.data || []).map(e => e.body).filter(Boolean);
        items.push(...details);
        await sleep(100);
      }
      cachedItems = items;
      saveStockToDisk();
    }

    // 3. Traer TODAS las órdenes de los últimos 30 días en batches paralelos
    const from30 = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000)
      .toISOString().slice(0, 19) + '.000-00:00';

    const firstPage = await axios.get(`${ML_API_URL}/orders/search`, {
      headers,
      params: { seller: uid, 'date_created.from': from30, offset: 0, limit: 50, sort: 'date_asc' },
    });
    const total30 = firstPage.data.paging?.total || 0;
    const allPages = [firstPage.data.results || []];

    const MAX_OFFSET = 9950; // ML no permite offset + limit > 10000
    const offsets = [];
    for (let o = 50; o <= Math.min(total30 - 50, MAX_OFFSET); o += 50) offsets.push(o);

    const BATCH = 10;
    for (let i = 0; i < offsets.length; i += BATCH) {
      const pages = await Promise.allSettled(
        offsets.slice(i, i + BATCH).map(o =>
          axios.get(`${ML_API_URL}/orders/search`, {
            headers,
            params: { seller: uid, 'date_created.from': from30, offset: o, limit: 50, sort: 'date_asc' },
          })
        )
      );
      pages.forEach(p => {
        if (p.status === 'fulfilled') allPages.push(p.value.data.results || []);
      });
      await sleep(200);
    }

    const salesByItem = {};
    allPages.flat().forEach(order => {
      (order.order_items || []).forEach(oi => {
        const id = oi.item?.id;
        if (id) salesByItem[id] = (salesByItem[id] || 0) + (oi.quantity || 1);
      });
    });

    // 4. Traer nombres de categorías únicas
    const categoryIds = [...new Set(items.map(i => i.category_id).filter(Boolean))];
    const categoryNames = {};
    await Promise.all(categoryIds.map(async id => {
      try {
        const r = await axios.get(`${ML_API_URL}/categories/${id}`, { headers });
        categoryNames[id] = r.data.name;
      } catch { categoryNames[id] = id; }
    }));

    // 5. Calcular días de stock sin factor de escala (datos exactos)
    const result = items.map(item => {
      const sold30d   = salesByItem[item.id] || 0;
      const dailyRate = sold30d / 30;
      const stock     = item.available_quantity || 0;
      const daysLeft  = dailyRate > 0 ? Math.round(stock / dailyRate) : null;

      return {
        id:            item.id,
        title:         item.title,
        thumbnail:     item.thumbnail,
        price:         item.price,
        currency:      item.currency_id,
        status:        item.status,
        category_id:   item.category_id,
        category_name: categoryNames[item.category_id] || item.category_id || 'Sin categoría',
        stock,
        sold30d,
        daily_rate: parseFloat(dailyRate.toFixed(2)),
        days_left:  daysLeft,
        permalink:  item.permalink,
        sku: (item.attributes || []).find(a => a.id === 'SELLER_SKU')?.value_name
          || (item.attributes || []).find(a => a.id === 'SELLER_SKU')?.values?.[0]?.name
          || null,
        variation_skus: (item.variations || []).map(v =>
          (v.attributes || []).find(a => a.id === 'SELLER_SKU')?.value_name
          || (v.attributes || []).find(a => a.id === 'SELLER_SKU')?.values?.[0]?.name
          || null
        ).filter(Boolean),
        original_price:  item.original_price || null,
        logistic_type:   item.shipping?.logistic_type || null,
        shipping_mode:   item.shipping?.mode || null,
        free_shipping:   item.shipping?.free_shipping || false,
      };
    });

    // Ordenar: sin stock → crítico por días → poco stock físico → warning → ok → sin ventas
    result.sort((a, b) => {
      const score = item => {
        if (item.stock === 0)                                   return 0;
        if (item.days_left !== null && item.days_left < 7)     return 1;
        if (item.stock <= 5)                                    return 2;
        if (item.days_left !== null && item.days_left <= 30)   return 3;
        if (item.stock <= 15)                                   return 4;
        if (item.days_left !== null)                            return 5;
        return 6;
      };
      const sa = score(a), sb = score(b);
      if (sa !== sb) return sa - sb;
      return (a.days_left ?? a.stock) - (b.days_left ?? b.stock);
    });

    cachedStock     = result;
    stockLastUpdate = new Date().toISOString();
    fs.writeFileSync(STOCK_RESULT_FILE, JSON.stringify({ items: result, savedAt: stockLastUpdate }), 'utf8');
    console.log(`[stock] refresh completo — ${result.length} items`);
  } catch(err) {
    console.error('[stock] error en refresh:', err.response?.data || err.message);
  } finally {
    stockFetching = false;
  }
}

// Cron: actualizar stock cada 20 minutos
setInterval(() => refreshStockCache(), 20 * 60 * 1000);

// Cron: refrescar token ML cada 5 horas (expira a las 6h)
async function refreshMLToken() {
  if (!tokenData?.refresh_token) return;
  try {
    const r = await axios.post(`${ML_API_URL}/oauth/token`, {
      grant_type: 'refresh_token',
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
      refresh_token: tokenData.refresh_token,
    });
    tokenData = r.data;
    saveToken(tokenData);
    console.log('[token] ML token refrescado OK');
  } catch(e) {
    console.error('[token] error refrescando ML token:', e.response?.data || e.message);
  }
}
setInterval(refreshMLToken, 5 * 60 * 60 * 1000);
// Refrescar al arrancar si ya hay token guardado
if (tokenData?.refresh_token) refreshMLToken();

// GET /api/stock — devuelve cache inmediatamente, refresca en background si hace falta
app.get('/api/stock', requireToken, async (req, res) => {
  const forceRefresh = req.query.refresh === 'true';

  if (forceRefresh) {
    await refreshStockCache(true);
  } else if (!cachedStock.length) {
    // Sin cache: esperar a que termine (ya sea fetch en curso o uno nuevo)
    if (!stockFetching) refreshStockCache(false);
    while (stockFetching) await sleep(500);
  } else {
    // Hay cache: devolver inmediatamente y refrescar en background
    refreshStockCache(false);
  }

  // Agregar unidades en camino por SKU desde órdenes de compra
  const compras = loadCompras();
  const incoming = {}; // sku -> [{qty, expected_date, supplier, id}]
  for (const c of compras) {
    for (const it of (c.items || [])) {
      if (!it.sku) continue;
      if (!incoming[it.sku]) incoming[it.sku] = [];
      incoming[it.sku].push({ qty: it.qty, expected_date: c.expected_date, supplier: c.supplier, order_id: c.id });
    }
  }
  const itemsWithIncoming = cachedStock.map(item => ({
    ...item,
    incoming: incoming[item.sku] || [],
  }));

  res.json({ items: itemsWithIncoming, total_items: itemsWithIncoming.length, lastUpdated: stockLastUpdate });
});

// ── Órdenes de compra ────────────────────────────────────────────
const COMPRAS_FILE = path.join(__dirname, 'data', 'compras.json');

function loadCompras() {
  try {
    if (fs.existsSync(COMPRAS_FILE)) return JSON.parse(fs.readFileSync(COMPRAS_FILE, 'utf8'));
  } catch(e) {}
  return [];
}
function saveCompras(data) {
  fs.writeFileSync(COMPRAS_FILE, JSON.stringify(data, null, 2), 'utf8');
}

// ── Análisis de precios ──────────────────────────────────────────
const PRECIOS_FILE = path.join(__dirname, 'data', 'precios_cache.json');
let preciosRunning = false;

app.get('/api/precios', requireToken, async (req, res) => {
  if (req.query.refresh === 'true') {
    if (!preciosRunning) runPreciosAnalysis();
    return res.json({ running: true, message: 'Análisis iniciado en background' });
  }

  // Devolver cache existente
  try {
    if (fs.existsSync(PRECIOS_FILE)) {
      const data = JSON.parse(fs.readFileSync(PRECIOS_FILE, 'utf8'));
      return res.json({ running: preciosRunning, ...data });
    }
  } catch(e) {}
  res.json({ running: preciosRunning, items: [], savedAt: null });
});

async function runPreciosAnalysis() {
  if (preciosRunning) return;
  preciosRunning = true;
  console.log('[precios] iniciando análisis...');

  try {
    const headers = { Authorization: `Bearer ${tokenData.access_token}` };

    // Filtrar: stock > 90 días, sin ventas en 30 días, sin descuento
    const candidates = cachedStock.filter(item =>
      item.days_left !== null && item.days_left > 90 &&
      item.sold30d === 0 &&
      !item.original_price &&
      item.stock > 0
    );

    console.log(`[precios] ${candidates.length} candidatos (overstock + sin ventas + sin descuento)`);

    const results = [];

    for (const item of candidates) {
      try {
        // Buscar competidores: primeras 4 palabras del título + categoría
        const keywords = item.title.split(' ').slice(0, 4).join(' ');
        const searchRes = await axios.get(`${ML_API_URL}/sites/MLU/search`, {
          headers,
          params: { q: keywords, category: item.category_id, limit: 20 },
        });

        const prices = (searchRes.data.results || [])
          .filter(r => r.id !== item.id && r.price > 0)
          .map(r => r.price);

        if (prices.length < 3) { await sleep(200); continue; }

        prices.sort((a, b) => a - b);
        const median = prices[Math.floor(prices.length / 2)];
        const p25    = prices[Math.floor(prices.length * 0.25)];
        const p75    = prices[Math.floor(prices.length * 0.75)];
        const pctDiff = ((item.price - median) / median * 100);

        let status = 'ok';
        if (item.price > p75 * 1.1)  status = 'caro';
        if (item.price < p25 * 0.9)  status = 'barato';

        results.push({
          id:         item.id,
          title:      item.title,
          thumbnail:  item.thumbnail,
          permalink:  item.permalink,
          sku:        item.sku,
          price:      item.price,
          stock:      item.stock,
          days_left:  item.days_left,
          median,
          p25,
          p75,
          pct_diff:   parseFloat(pctDiff.toFixed(1)),
          status,
          competitors: prices.length,
        });

        await sleep(200); // respetar rate limit
      } catch(e) {
        await sleep(500);
      }
    }

    // Ordenar: primero los más fuera de rango
    results.sort((a, b) => Math.abs(b.pct_diff) - Math.abs(a.pct_diff));

    const data = { items: results, savedAt: new Date().toISOString(), total: results.length };
    fs.writeFileSync(PRECIOS_FILE, JSON.stringify(data, null, 2));
    console.log(`[precios] análisis completo: ${results.length} ítems analizados`);
  } catch(e) {
    console.error('[precios] error:', e.message);
  } finally {
    preciosRunning = false;
  }
}

// GET /api/compras
app.get('/api/compras', requireToken, (req, res) => {
  res.json(loadCompras());
});

// POST /api/compras — body: { supplier, expected_date, notes, items: [{sku, qty}] }
app.post('/api/compras', requireToken, express.json(), (req, res) => {
  const compras = loadCompras();
  const nueva = {
    id: Date.now().toString(),
    created_at: new Date().toISOString(),
    supplier:      req.body.supplier || 'China',
    expected_date: req.body.expected_date,
    notes:         req.body.notes || '',
    items:         req.body.items || [],
  };
  compras.push(nueva);
  saveCompras(compras);
  res.json(nueva);
});

// DELETE /api/compras/:id
app.delete('/api/compras/:id', requireToken, (req, res) => {
  const compras = loadCompras().filter(c => c.id !== req.params.id);
  saveCompras(compras);
  res.json({ ok: true });
});

// ── Cache de ventas ──────────────────────────────────────────────
const UY_OFFSET_MS  = 3 * 60 * 60 * 1000;
const CACHE_FILE    = path.join(__dirname, 'data', 'ventas_cache.json');
let ventasFetching  = {};

// Cargar cache desde disco al arrancar
let ventasCache = {};
try {
  fs.mkdirSync(path.join(__dirname, 'data'), { recursive: true });
  if (fs.existsSync(CACHE_FILE)) {
    ventasCache = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf8'));
    // Invalidar cache de 'today' si fue guardado en otro día
    if (ventasCache['today']) {
      const savedDay = new Date(ventasCache['today'].lastUpdated).toISOString().slice(0, 10);
      const todayDay = new Date(Date.now() - UY_OFFSET_MS).toISOString().slice(0, 10);
      if (savedDay !== todayDay) {
        delete ventasCache['today'];
        console.log('[cache] cache de hoy invalidado (día distinto)');
      }
    }
    console.log(`[cache] ventas cargado desde disco (${Object.keys(ventasCache).length} períodos)`);
  }
} catch(e) {
  console.warn('[cache] no se pudo leer cache de disco:', e.message);
  ventasCache = {};
}

function saveCacheToDisk() {
  try {
    fs.writeFileSync(CACHE_FILE, JSON.stringify(ventasCache), 'utf8');
  } catch(e) {
    console.warn('[cache] error al guardar cache:', e.message);
  }
}

function uyMidnightUTC(offsetDays = 0) {
  const uyNow = new Date(Date.now() - UY_OFFSET_MS);
  const y = uyNow.getUTCFullYear(), m = uyNow.getUTCMonth(), d = uyNow.getUTCDate() + offsetDays;
  return new Date(Date.UTC(y, m, d, 3, 0, 0, 0));
}
function toMLDateV(d) { return d.toISOString().slice(0, 19) + '.000-00:00'; }

function ventasDateRange(period, fromQ, toQ) {
  if (period === 'today') {
    return { from: toMLDateV(uyMidnightUTC(0)), to: toMLDateV(new Date(uyMidnightUTC(1).getTime() - 1000)) };
  }
  if (period === 'month') {
    const uyNow = new Date(Date.now() - UY_OFFSET_MS);
    return { from: toMLDateV(new Date(Date.UTC(uyNow.getUTCFullYear(), uyNow.getUTCMonth(), 1, 3, 0, 0, 0))), to: null };
  }
  if (period === '7' || period === '30' || period === '90') {
    return { from: toMLDateV(uyMidnightUTC(-parseInt(period))), to: null };
  }
  // custom
  const [fy, fm, fd] = (fromQ || '').split('-').map(Number);
  const [ty, tm, td] = (toQ   || '').split('-').map(Number);
  return {
    from: fy ? toMLDateV(new Date(Date.UTC(fy, fm-1, fd, 3, 0, 0, 0))) : toMLDateV(uyMidnightUTC(0)),
    to:   ty ? toMLDateV(new Date(Date.UTC(ty, tm-1, td+1, 3, 0, 0, 0) - 1000)) : null,
  };
}

async function fetchVentasData(period, fromQ, toQ) {
  if (!tokenData?.access_token) throw new Error('No autenticado');
  const headers = { Authorization: `Bearer ${tokenData.access_token}` };
  const uid = tokenData.user_id;
  const { from, to } = ventasDateRange(period, fromQ, toQ);

  const fromDate = new Date(from);
  const toDate   = to ? new Date(to) : new Date();

  // Hora UY actual para referencia
  const nowUY = new Date(Date.now() - UY_OFFSET_MS);
  console.log(`[ventas] ---- ${period} ----`);
  console.log(`[ventas] ahora UY:  ${nowUY.toISOString().slice(0,16)} (UTC-3)`);
  console.log(`[ventas] from:      ${from}  →  ${fromDate.toISOString()}`);
  console.log(`[ventas] to:        ${to || '(ahora)'}  →  ${toDate.toISOString()}`);

  const allOrders = [];
  let offset = 0, done = false;

  while (!done) {
    const r = await axios.get(`${ML_API_URL}/orders/search`, {
      headers, params: { seller: uid, limit: 50, sort: 'date_desc', offset },
    });
    const results = r.data.results || [];

    if (offset === 0 && results.length > 0) {
      const first = new Date(results[0].date_created);
      const last  = new Date(results[results.length - 1].date_created);
      console.log(`[ventas] pág 1: primera orden ${first.toISOString().slice(0,16)}, última ${last.toISOString().slice(0,16)}`);
    }

    // Procesar toda la página — ML no siempre ordena perfecto
    for (const order of results) {
      const d = new Date(order.date_created);
      if (d >= fromDate && d <= toDate) allOrders.push(order);
    }
    // Cortar solo cuando el último orden de la página es anterior a fromDate
    const lastD = results.length ? new Date(results[results.length - 1].date_created) : null;
    if (!lastD || lastD < fromDate || results.length < 50) done = true;
    offset += 50;
    if (offset > 9950) done = true;
    if (!done) await sleep(150);
  }

  const byStatusLog = {};
  allOrders.forEach(o => { byStatusLog[o.status] = (byStatusLog[o.status] || 0) + 1; });
  console.log(`[ventas] órdenes en rango: ${allOrders.length} | estados:`, JSON.stringify(byStatusLog));

  const PAID = new Set(['paid', 'confirmed']);
  const paidOrders      = allOrders.filter(o => PAID.has(o.status));
  const cancelledOrders = allOrders.filter(o => o.status === 'cancelled');
  const totalRevenue    = paidOrders.reduce((s, o) => s + (o.total_amount || 0), 0);
  const totalUnits      = paidOrders.reduce((s, o) =>
    s + (o.order_items || []).reduce((q, oi) => q + (oi.quantity || 1), 0), 0);
  const cancelRate      = allOrders.length > 0 ? (cancelledOrders.length / allOrders.length * 100) : 0;

  const byItem = {};
  paidOrders.forEach(order => {
    (order.order_items || []).forEach(oi => {
      const id = oi.item?.id;
      if (!id) return;
      if (!byItem[id]) byItem[id] = {
        id, title: oi.item.title || '', thumbnail: null, permalink: null,
        price: oi.unit_price || 0, currency: order.currency_id || 'UYU',
        listing_type: oi.listing_type_id || '', units: 0, revenue: 0,
      };
      byItem[id].units   += oi.quantity || 1;
      byItem[id].revenue += (oi.unit_price || 0) * (oi.quantity || 1);
    });
  });

  if (cachedItems.length > 0) {
    cachedItems.forEach(ci => {
      if (byItem[ci.id]) { byItem[ci.id].thumbnail = ci.thumbnail; byItem[ci.id].permalink = ci.permalink; }
    });
  } else {
    const ids = Object.keys(byItem);
    for (let i = 0; i < ids.length; i += 20) {
      try {
        const r = await axios.get(`${ML_API_URL}/items`, { headers, params: { ids: ids.slice(i, i+20).join(',') } });
        (r.data || []).forEach(e => {
          if (e.body && byItem[e.body.id]) { byItem[e.body.id].thumbnail = e.body.thumbnail; byItem[e.body.id].permalink = e.body.permalink; }
        });
      } catch {}
      await sleep(100);
    }
  }

  const items = Object.values(byItem).sort((a, b) => b.revenue - a.revenue);
  items.forEach(i => { i.participation = totalRevenue > 0 ? (i.revenue / totalRevenue * 100) : 0; });

  console.log(`[ventas] ${period} — ${paidOrders.length} pagadas, $${Math.round(totalRevenue).toLocaleString('es-UY')}`);
  return {
    period: { from, to },
    lastUpdated: new Date().toISOString(),
    metrics: {
      total_orders: allOrders.length, paid_orders: paidOrders.length,
      cancelled_orders: cancelledOrders.length, cancel_rate: parseFloat(cancelRate.toFixed(1)),
      total_units: totalUnits, total_revenue: parseFloat(totalRevenue.toFixed(2)),
      avg_ticket: parseFloat((paidOrders.length > 0 ? totalRevenue / paidOrders.length : 0).toFixed(2)),
      items_with_sales: items.length,
    },
    items,
  };
}

async function refreshVentasCache(period, fromQ, toQ) {
  const key = period + (fromQ || '') + (toQ || '');
  if (ventasFetching[key]) return;
  ventasFetching[key] = true;
  try {
    ventasCache[key] = await fetchVentasData(period, fromQ, toQ);
    saveCacheToDisk();
  } catch(e) {
    console.error(`[ventas] error refresh ${period}:`, e.message);
  } finally {
    ventasFetching[key] = false;
  }
}

// Auto-refresh "today" cada 5 minutos si hay sesión activa
setInterval(() => { if (tokenData?.access_token) refreshVentasCache('today'); }, 5 * 60 * 1000);

// GET /api/ventas
app.get('/api/ventas', requireToken, async (req, res) => {
  const period = req.query.period || 'today';
  const fromQ  = req.query.from  || null;
  const toQ    = req.query.to    || null;
  const key    = period + (fromQ || '') + (toQ || '');

  const forceRefresh = req.query.refresh === 'true';

  if (!forceRefresh && ventasCache[key]) {
    const ageMin = (Date.now() - new Date(ventasCache[key].lastUpdated)) / 60000;
    if (ageMin > 3) refreshVentasCache(period, fromQ, toQ);
    return res.json({ ...ventasCache[key], fromCache: true });
  }

  // Force refresh o primera vez: esperar la carga
  try {
    delete ventasCache[key]; // limpiar cache para forzar fetch fresco
    ventasCache[key] = await fetchVentasData(period, fromQ, toQ);
    res.json({ ...ventasCache[key], fromCache: false });
  } catch(err) {
    res.status(500).json({ error: err.response?.data || err.message });
  }
});

// GET /api/tareas — preguntas sin responder + órdenes ME1/acuerda-con-comprador
app.get('/api/tareas', requireToken, async (req, res) => {
  const headers = { Authorization: `Bearer ${tokenData.access_token}` };
  const uid = tokenData.user_id;

  try {
    // 1. Preguntas sin responder de los últimos 7 días
    const since7d = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
    const qRes = await axios.get(`${ML_API_URL}/questions/search`, {
      headers,
      params: { seller_id: uid, status: 'UNANSWERED', sort_fields: 'date_created', sort_types: 'DESC', limit: 50 },
    });
    const allQuestions = qRes.data.questions || [];
    const recentQuestions = allQuestions.filter(q => q.date_created >= since7d);
    const rawQuestions = recentQuestions.map(q => ({
      id:           q.id,
      item_id:      q.item_id,
      text:         q.text,
      date_created: q.date_created,
      from_id:      q.from?.id || null,
    }));

    // Generar respuestas sugeridas con Claude
    console.log(`[tareas] anthropic=${!!anthropic} rawQuestions=${rawQuestions.length}`);
    let suggestions = {};
    if (anthropic && rawQuestions.length > 0) {
      try {
        const reglasNegocioTareas = loadReglasNegocio();
        const reglasPromptTareas = reglasNegocioTareas.length
          ? `\nInformación del negocio:\n${reglasNegocioTareas.map(r => `- ${r.categoria ? '[' + r.categoria + '] ' : ''}${r.texto}`).join('\n')}`
          : '';
        const prompt = `Sos el asistente de MUNDO SHOP, una tienda en MercadoLibre Uruguay.
Generá respuestas cortas y amigables a estas preguntas de compradores.
El estilo es: empezar con "Hola, ¿cómo estás?" y terminar con "Agradecemos te hayas comunicado, quedamos a las órdenes! MUNDO SHOP".${reglasPromptTareas}
Respondé SOLO con un JSON válido: un objeto donde cada clave es el id de la pregunta y el valor es la respuesta sugerida.

Preguntas:
${rawQuestions.map(q => `ID ${q.id}: "${q.text}"`).join('\n')}`;

        const r = await anthropic.messages.create({
          model: 'claude-haiku-4-5',
          max_tokens: 4096,
          messages: [{ role: 'user', content: prompt }],
        });
        const text = r.content.find(b => b.type === 'text')?.text || '{}';
        fs.writeFileSync('data/debug_suggestions.json', JSON.stringify({ text }, null, 2));
        const jsonMatch = text.match(/\{[\s\S]*\}/);
        if (jsonMatch) suggestions = JSON.parse(jsonMatch[0]);
      } catch(e) {
        console.error('[tareas] sugerencias error:', e.message);
      }
    }

    const questions = rawQuestions.map(q => ({ ...q, suggestion: suggestions[q.id] || null }));

    // 2. Últimas 50 órdenes pagadas
    const ordRes = await axios.get(`${ML_API_URL}/orders/search`, {
      headers,
      params: { seller: uid, limit: 50, sort: 'date_desc', order_status: 'paid' },
    });
    const orders = ordRes.data.results || [];

    // 3. Acuerda con comprador: tag no_shipping (no necesita fetch extra)
    const acordadas = orders.filter(o => (o.tags || []).includes('no_shipping'));

    // 4. Fetch shipments para todas las órdenes con shipping.id
    const withShipping = orders.filter(o => o.shipping?.id && !(o.tags || []).includes('no_shipping'));
    const shipmentDetails = {};
    await Promise.all(withShipping.map(async o => {
      try {
        const r = await axios.get(`${ML_API_URL}/shipments/${o.shipping.id}`, { headers });
        shipmentDetails[o.shipping.id] = r.data;
      } catch(e) { /* skip */ }
    }));
    const me1Orders = withShipping.filter(o => shipmentDetails[o.shipping.id]?.mode === 'me1');
    const dacOrders = withShipping.filter(o => {
      const shp = shipmentDetails[o.shipping.id];
      if (!shp) return false;
      const mode = shp.mode || '';
      const logistic = shp.logistic_type || '';
      return mode !== 'me1' && (mode === 'custom' || logistic === 'dac' || logistic === 'self_service' || logistic === 'drop_off' || mode === 'me2');
    });

    const formatOrder = (order, label) => {
      const oi = order.order_items?.[0] || {};
      const item = oi.item || {};
      const shp = shipmentDetails[order.shipping?.id] || {};
      const addr = shp.receiver_address || {};
      const shipping_address = shp.id ? {
        id: shp.id,
        status: shp.status || '',
        mode: shp.mode || '',
        logistic_type: shp.logistic_type || '',
        receiver_name: addr.receiver_name || order.buyer?.nickname || '',
        address: addr.address_line || '',
        city: addr.city?.name || '',
        state: addr.state?.name || '',
        zip: addr.zip_code || '',
        comment: addr.comment || ''
      } : null;
      return {
        order_id:        order.id,
        date_created:    order.date_created,
        buyer_name:      order.buyer?.nickname || order.buyer?.first_name || '—',
        total_amount:    order.total_amount,
        item_title:      item.title || '—',
        item_thumbnail:  item.thumbnail || null,
        item_sku:        item.seller_sku || (item.attributes || []).find(a => a.id === 'SELLER_SKU')?.value_name || null,
        quantity:        oi.quantity || 1,
        shipping_label:  label,
        shipping_address
      };
    };

    console.log(`[tareas] ${questions.length} preguntas sin responder | ${acordadas.length} acuerda | ${me1Orders.length} ME1 | ${dacOrders.length} DAC`);

    res.json({
      questions,
      acuerda_orders: acordadas.map(o => formatOrder(o, 'Acuerda c/ comprador')),
      me1_orders:     me1Orders.map(o => formatOrder(o, 'ME1')),
      dac_orders:     dacOrders.map(o => formatOrder(o, 'ME1 a coordinar')),
    });
  } catch(err) {
    console.error('[tareas] error:', err.response?.data || err.message);
    res.status(500).json({ error: err.response?.data || err.message });
  }
});

// GET /api/devoluciones
app.get('/api/devoluciones', requireToken, async (req, res) => {
  const headers = { Authorization: `Bearer ${tokenData.access_token}` };
  const statusFilter = req.query.status || null;

  try {
    const allClaims = [];
    let offset = 0;

    while (true) {
      const params = { role: 'seller', offset, limit: 50 };
      if (statusFilter && statusFilter !== 'all') params.status = statusFilter;

      const r = await axios.get(`${ML_API_URL}/post-purchase/v1/claims/search`, { headers, params });
      const results = r.data.data || r.data.results || [];
      const total   = r.data.paging?.total ?? results.length;
      for (const c of results) allClaims.push(c);
      if (results.length < 50 || allClaims.length >= total) break;
      offset += 50;
      await sleep(150);
    }

    // Enrich with order data (buyer, shipping, items with SKU/price/variant)
    const orderIds = [...new Set(allClaims.map(c => c.resource_id).filter(Boolean))];
    const orderMap = {};
    for (let i = 0; i < orderIds.length && i < 200; i += 20) {
      const batch = orderIds.slice(i, i + 20);
      await Promise.all(batch.map(async (oid) => {
        try {
          const or = await axios.get(`${ML_API_URL}/orders/${oid}`, { headers });
          const o  = or.data;
          const oi = o.order_items?.[0] || {};
          const it = oi.item || {};
          const sh = o.shipping || {};
          orderMap[oid] = {
            buyer_nickname: o.buyer?.nickname || null,
            buyer_name:     [o.buyer?.first_name, o.buyer?.last_name].filter(Boolean).join(' ') || null,
            logistic_type:  sh.logistic_type  || null,
            shipping_id:    sh.id             || null,
            item_id:        it.id             || null,
            item_title:     it.title          || null,
            item_thumbnail: it.thumbnail      || null,
            item_sku:       it.seller_sku || oi.seller_sku || null,
            variation_name: it.variation_attributes?.map(a => `${a.name}: ${a.value_name}`).join(', ') || null,
            unit_price:     oi.unit_price     || null,
            quantity:       oi.quantity       || null,
          };
        } catch { /* skip */ }
      }));
      if (i + 20 < orderIds.length) await sleep(150);
    }

    const enriched = allClaims.map(c => ({ ...c, ...(orderMap[c.resource_id] || {}) }));

    const returns = enriched.filter(c => c.type === 'return');
    const claims  = enriched.filter(c => c.type !== 'return');

    // Count by substatus for returns summary
    const by_substatus = {};
    for (const r of returns) {
      const k = r.sub_status || r.substatus || r.status || 'other';
      by_substatus[k] = (by_substatus[k] || 0) + 1;
    }

    const by_status = { opened: 0, closed: 0, resolved: 0 };
    for (const c of enriched) {
      if (c.status === 'opened')        by_status.opened++;
      else if (c.status === 'closed')   by_status.closed++;
      else if (c.status === 'resolved') by_status.resolved++;
    }

    res.json({ returns, claims, by_status, by_substatus, total: enriched.length });
  } catch(err) {
    console.error('[devoluciones] error:', err.response?.data || err.message);
    res.status(500).json({ error: err.response?.data || err.message });
  }
});

// ── Odoo ─────────────────────────────────────────────────────────
const ODOO_HOST    = process.env.ODOO_HOST;
const ODOO_DB      = process.env.ODOO_DB;
const ODOO_USER    = process.env.ODOO_USER;
const ODOO_API_KEY = process.env.ODOO_API_KEY;

function odooCall(path, method, params) {
  return new Promise((resolve, reject) => {
    const client = xmlrpc.createSecureClient({ host: ODOO_HOST, path });
    client.methodCall(method, params, (err, val) => err ? reject(err) : resolve(val));
  });
}

async function odooAuth() {
  return odooCall('/xmlrpc/2/common', 'authenticate', [ODOO_DB, ODOO_USER, ODOO_API_KEY, {}]);
}

async function odooSearchRead(uid, model, domain, fields, opts = {}) {
  const allItems = [];
  const pageSize = 500;
  let offset = 0;
  while (true) {
    const batch = await odooCall('/xmlrpc/2/object', 'execute_kw', [
      ODOO_DB, uid, ODOO_API_KEY, model, 'search_read', [domain],
      { fields, limit: pageSize, offset, ...opts },
    ]);
    allItems.push(...batch);
    if (batch.length < pageSize) break;
    offset += pageSize;
  }
  return allItems;
}

let odooCache = null;
let odooCacheTime = 0;
const ODOO_CACHE_FILE = path.join(__dirname, 'data', 'odoo_cache.json');

function loadOdooCacheFromDisk() {
  try {
    if (fs.existsSync(ODOO_CACHE_FILE)) {
      const saved = JSON.parse(fs.readFileSync(ODOO_CACHE_FILE, 'utf8'));
      odooCache     = saved.products || [];
      odooCacheTime = new Date(saved.savedAt).getTime() || 0;
      console.log(`[odoo] cache cargado desde disco: ${odooCache.length} productos`);
    }
  } catch(e) { console.error('[odoo] error leyendo cache:', e.message); }
}

function saveOdooCacheToDisk() {
  try {
    fs.writeFileSync(ODOO_CACHE_FILE, JSON.stringify({ products: odooCache, savedAt: new Date().toISOString() }), 'utf8');
  } catch(e) { console.error('[odoo] error guardando cache:', e.message); }
}

async function getOdooProducts(force = false) {
  if (!force && odooCache && odooCache.length > 0) return odooCache;
  const uid = await odooAuth();
  const products = await odooSearchRead(uid, 'product.product', [['active', '=', true]], [
    'name', 'default_code', 'list_price', 'standard_price', 'categ_id', 'taxes_id', 'uom_id', 'x_studio_producto_mayorista', 'qty_available',
  ]);
  odooCache     = products;
  odooCacheTime = Date.now();
  saveOdooCacheToDisk();
  console.log(`[odoo] ${products.length} productos guardados en disco`);
  return products;
}

loadOdooCacheFromDisk();

app.get('/api/odoo/buscar-partner', async (req, res) => {
  try {
    const q = req.query.q || '';
    if (!q) return res.json([]);
    const uid = await odooAuth();
    // Buscar por nombre, teléfono, móvil y referencia interna
    const domain = [
      ['customer_rank', '>', 0],
      '|', '|', '|',
      ['name', 'ilike', q],
      ['phone', 'ilike', q],
      ['mobile', 'ilike', q],
      ['ref', 'ilike', q],
    ];
    const partners = await odooCall('/xmlrpc/2/object', 'execute_kw', [
      ODOO_DB, uid, ODOO_API_KEY, 'res.partner', 'search_read',
      [domain],
      { fields: ['id', 'name', 'phone', 'mobile', 'ref'], limit: 10 },
    ]);
    res.json(partners);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Sirve la imagen de un product.product por ID (con cache en disco)
const IMG_CACHE_DIR = path.join(__dirname, 'data', 'images');
if (!fs.existsSync(IMG_CACHE_DIR)) fs.mkdirSync(IMG_CACHE_DIR, { recursive: true });

app.get('/api/odoo/imagen/:id', async (req, res) => {
  const id = parseInt(req.params.id);
  if (isNaN(id)) return res.status(400).end();
  const filePath = path.join(IMG_CACHE_DIR, `${id}.png`);
  try {
    if (fs.existsSync(filePath)) {
      res.setHeader('Content-Type', 'image/png');
      res.setHeader('Cache-Control', 'public, max-age=604800');
      return res.end(fs.readFileSync(filePath));
    }
    const uid = await odooAuth();
    const rows = await odooCall('/xmlrpc/2/object', 'execute_kw', [
      ODOO_DB, uid, ODOO_API_KEY, 'product.product', 'search_read',
      [[['id', '=', id]]],
      { fields: ['image_512'], limit: 1 },
    ]);
    const b64 = rows[0]?.image_512;
    if (!b64) return res.status(404).end();
    const buf = Buffer.from(b64, 'base64');
    fs.writeFileSync(filePath, buf);
    res.setHeader('Content-Type', 'image/png');
    res.setHeader('Cache-Control', 'public, max-age=604800');
    res.end(buf);
  } catch (err) {
    res.status(500).end();
  }
});

function upgradeMlThumb(url) {
  if (!url) return null;
  return url
    .replace('http://', 'https://')
    .replace(/\/D_/, '/D_NQ_NP_')
    .replace(/-I\.jpg$/, '-O.jpg');
}

app.get('/api/odoo/productos', async (req, res) => {
  try {
    const products = await getOdooProducts(req.query.refresh === 'true');

    // Índice ML por SKU para cruzar stock (incluye variantes)
    const mlMap = buildMlSkuMap();

    // Agrupar por categoría
    const byCategory = {};
    for (const p of products) {
      const cat = Array.isArray(p.categ_id) ? p.categ_id[1] : (p.categ_id || 'Sin categoría');
      if (!byCategory[cat]) byCategory[cat] = [];
      const sku = p.default_code || '';
      const ml = mlMap[sku.trim()] || null;
      byCategory[cat].push({
        id:        p.id,
        name:      p.name,
        sku,
        price:     p.list_price,
        cost:      p.standard_price ?? 0,
        stock:     p.qty_available ?? 0,
        ml_stock:     ml ? ml.stock     : null,
        ml_price:     ml ? ml.price     : null,
        ml_status:    ml ? ml.status    : null,
        ml_thumbnail: ml ? upgradeMlThumb(ml.thumbnail) : null,
        categ:     cat,
        uom_id:    p.uom_id,
        tax_ids:   p.taxes_id || [],
        mayorista: p.x_studio_producto_mayorista || false,
      });
    }
    // Ordenar categorías y productos dentro de cada una
    const categories = Object.entries(byCategory)
      .sort(([a], [b]) => a.localeCompare(b, 'es'))
      .map(([name, items]) => ({
        name,
        items: items.sort((a, b) => a.name.localeCompare(b.name, 'es')),
      }));
    res.json({ categories, total: products.length, savedAt: new Date().toISOString() });
  } catch (err) {
    console.error('[odoo] error productos:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Mapeo manual SKU Odoo → ML item ID
const SKU_MAP_FILE = path.join(__dirname, 'data', 'sku_map_manual.json');
function loadSkuMapManual() {
  try { return fs.existsSync(SKU_MAP_FILE) ? JSON.parse(fs.readFileSync(SKU_MAP_FILE, 'utf8')) : {}; } catch { return {}; }
}
function saveSkuMapManual(map) { fs.writeFileSync(SKU_MAP_FILE, JSON.stringify(map, null, 2)); }

// Construye mapa sku → item incluyendo SKUs de variantes y mapeo manual
function buildMlSkuMap() {
  const map = {};
  const mlById = {};
  for (const item of cachedStock) {
    mlById[String(item.id)] = item;
    if (item.sku) map[item.sku.trim()] = item;
    for (const vsku of (item.variation_skus || [])) {
      if (vsku) map[vsku.trim()] = item;
    }
  }
  // Aplicar mapeo manual
  const manual = loadSkuMapManual();
  for (const [sku, mlId] of Object.entries(manual)) {
    const item = mlById[String(mlId)];
    if (item) map[sku.trim()] = item;
  }
  return map;
}

// GET /api/config/sku-map — leer mapeo manual
app.get('/api/config/sku-map', requireToken, (req, res) => res.json(loadSkuMapManual()));

// POST /api/config/sku-map — agregar entrada
app.post('/api/config/sku-map', requireToken, (req, res) => {
  const { sku, ml_id } = req.body;
  if (!sku || !ml_id) return res.status(400).json({ error: 'sku y ml_id requeridos' });
  const map = loadSkuMapManual();
  map[sku.trim()] = String(ml_id).replace(/[^0-9]/g, '');
  saveSkuMapManual(map);
  res.json({ ok: true, total: Object.keys(map).length });
});

// DELETE /api/config/sku-map/:sku — eliminar entrada
app.delete('/api/config/sku-map/:sku', requireToken, (req, res) => {
  const map = loadSkuMapManual();
  delete map[decodeURIComponent(req.params.sku)];
  saveSkuMapManual(map);
  res.json({ ok: true });
});

// GET /api/stock/discrepancias — stock en Odoo pero pausado o sin stock en ML
app.get('/api/stock/discrepancias', requireToken, async (req, res) => {
  try {
    const products = await getOdooProducts(false);
    const odooMap = {};
    for (const p of products) {
      if (p.default_code) odooMap[p.default_code.trim()] = p;
    }
    const mlMap = buildMlSkuMap();

    const rows = [];
    for (const [sku, odoo] of Object.entries(odooMap)) {
      const odooStock = odoo.qty_available ?? 0;
      if (odooStock <= 0) continue; // solo con stock en Odoo
      const ml = mlMap[sku];
      const mlPausada = ml && ml.status === 'paused';
      const mlSinStock = ml && ml.stock === 0;
      const sinPublicacion = !ml;
      if (!mlPausada && !mlSinStock && !sinPublicacion) continue;

      rows.push({
        sku,
        odoo_name:   odoo.name,
        odoo_stock:  odooStock,
        odoo_categ:  Array.isArray(odoo.categ_id) ? odoo.categ_id[1] : null,
        ml_title:    ml ? ml.title : null,
        ml_stock:    ml ? ml.stock : null,
        ml_status:   ml ? ml.status : null,
        ml_id:       ml ? ml.id : null,
        ml_permalink: ml ? ml.permalink : null,
        motivo:      sinPublicacion ? 'sin_publicacion' : mlPausada ? 'pausada' : 'sin_stock_ml',
      });
    }
    rows.sort((a, b) => b.odoo_stock - a.odoo_stock);
    res.json({ rows, total: rows.length });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stock/cruce-nombre — productos Odoo sin SKU en ML, con posible match por nombre
function simNombre(a, b) {
  const tok = s => s.toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '') // quitar tildes
    .replace(/[^a-z0-9\s]/g, '')
    .split(/\s+/).filter(w => w.length > 2);
  const wa = new Set(tok(a));
  const wb = new Set(tok(b));
  if (!wa.size || !wb.size) return 0;
  let inter = 0;
  wa.forEach(w => { if (wb.has(w)) inter++; });
  return inter / Math.sqrt(wa.size * wb.size);
}

app.get('/api/stock/cruce-nombre', requireToken, async (req, res) => {
  try {
    const products = await getOdooProducts(false);
    const mlMap = buildMlSkuMap();

    // Solo Odoo con stock y sin match exacto por SKU
    const sinMatch = products.filter(p => {
      if (!p.qty_available || p.qty_available <= 0) return false;
      if (!p.default_code) return true; // sin SKU
      return !mlMap[p.default_code.trim()]; // SKU no encontrado en ML
    });

    // Para cada uno, buscar el ML más similar por nombre
    const rows = [];
    for (const p of sinMatch) {
      let best = null, bestScore = 0;
      for (const item of cachedStock) {
        const score = simNombre(p.name, item.title);
        if (score > bestScore) { bestScore = score; best = item; }
      }
      if (bestScore >= 0.25) { // umbral mínimo
        rows.push({
          odoo_id:    p.id,
          odoo_sku:   p.default_code || null,
          odoo_name:  p.name,
          odoo_stock: p.qty_available,
          odoo_categ: Array.isArray(p.categ_id) ? p.categ_id[1] : null,
          ml_id:      best.id,
          ml_sku:     best.sku || null,
          ml_title:   best.title,
          ml_stock:   best.stock,
          ml_status:  best.status,
          ml_permalink: best.permalink,
          score:      Math.round(bestScore * 100),
        });
      }
    }
    rows.sort((a, b) => b.score - a.score);
    res.json({ rows, total: rows.length });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/odoo/cruce', async (req, res) => {
  try {
    const products = await getOdooProducts(req.query.refresh === 'true');

    // Índice Odoo por SKU (default_code)
    const odooMap = {};
    for (const p of products) {
      if (p.default_code) odooMap[p.default_code.trim()] = p;
    }

    // Índice ML por SKU (incluye variantes)
    const mlMap = buildMlSkuMap();

    // Unir: todos los SKUs de ambos lados
    const allSkus = new Set([...Object.keys(odooMap), ...Object.keys(mlMap)]);
    const rows = [];
    for (const sku of allSkus) {
      const o = odooMap[sku];
      const m = mlMap[sku];
      rows.push({
        sku,
        odoo_name:  o ? o.name : null,
        odoo_price: o ? o.list_price : null,
        odoo_categ: o && Array.isArray(o.categ_id) ? o.categ_id[1] : null,
        ml_title:   m ? m.title : null,
        ml_price:   m ? m.price : null,
        ml_stock:   m ? m.stock : null,
        ml_status:  m ? m.status : null,
        in_odoo:    !!o,
        in_ml:      !!m,
      });
    }
    rows.sort((a, b) => a.sku.localeCompare(b.sku, 'es'));
    res.json({ rows, total: rows.length, only_odoo: rows.filter(r => !r.in_ml).length, only_ml: rows.filter(r => !r.in_odoo).length, both: rows.filter(r => r.in_odoo && r.in_ml).length });
  } catch (err) {
    console.error('[odoo] error cruce:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── Cotización desde foto ────────────────────────────────────────
app.post('/api/odoo/cotizacion-foto', express.json({ limit: '20mb' }), async (req, res) => {
  try {
    const { image_base64, media_type } = req.body;
    if (!image_base64) return res.status(400).json({ error: 'Se requiere image_base64' });

    // 1. Claude lee la imagen
    const msg = await anthropic.messages.create({
      model: 'claude-opus-4-6',
      max_tokens: 1024,
      messages: [{
        role: 'user',
        content: [
          {
            type: 'image',
            source: { type: 'base64', media_type: media_type || 'image/jpeg', data: image_base64 },
          },
          {
            type: 'text',
            text: `Analizá esta imagen de un pedido escrito a mano. Extraé:
1. Nombre del cliente (suele estar arriba)
2. Lista de productos con: SKU completo (incluyendo variante de color/temperatura), cantidad y precio si aparece

IMPORTANTE — formato de variantes de SKU:
- Los SKUs tienen formato: {BASE}-{COLOR}-{TEMP}  (ej: 22306-BLA-FRI)
- Abreviaciones de color: B/BL/Blanco=BLA, N/NG/Negro=NEG, G/GR/Gris=GRI, R/RO/Rosa=ROS, V/VE/Verde=VER, D/DO/Dorado=DOR, P/PL/Plateado=PLA, AZ/AZU=AZU
- Abreviaciones de temperatura: F/FRI=FRI, C/CAL=CAL
- Cuando una línea dice "18110: 2B-2N" significa: 2 unidades de 18110-BLA y 2 unidades de 18110-NEG → generá DOS entradas separadas
- Cuando dice "22306 3FRI-2CAL" significa: 3 unidades de 22306-FRI y 2 unidades de 22306-CAL → DOS entradas separadas
- Si solo hay un color/variante, generá una sola entrada con el SKU completo

Respondé ÚNICAMENTE con un JSON válido con esta estructura exacta, sin texto adicional:
{
  "cliente": "nombre del cliente",
  "productos": [
    { "sku": "18110-BLA", "cantidad": 2, "precio": null },
    { "sku": "18110-NEG", "cantidad": 2, "precio": null }
  ],
  "notas": "cualquier nota o condición adicional que aparezca"
}
Si no encontrás algún campo ponelo como null.`,
          },
        ],
      }],
    });

    let parsed;
    try {
      const text = msg.content[0].text.trim().replace(/^```json\n?/, '').replace(/\n?```$/, '');
      parsed = JSON.parse(text);
    } catch (e) {
      return res.status(422).json({ error: 'No se pudo interpretar la imagen', raw: msg.content[0].text });
    }

    const uid = await odooAuth();

    // 2. Buscar cliente por nombre, teléfono o referencia (solo si hay valor)
    const cq = (parsed.cliente || '').trim();
    let partners = [];
    if (cq) {
      const partnerDomain = [
        ['customer_rank', '>', 0],
        '|', '|', '|',
        ['name', 'ilike', cq],
        ['phone', 'ilike', cq],
        ['mobile', 'ilike', cq],
        ['ref', 'ilike', cq],
      ];
      partners = await odooCall('/xmlrpc/2/object', 'execute_kw', [
        ODOO_DB, uid, ODOO_API_KEY, 'res.partner', 'search_read',
        [partnerDomain],
        { fields: ['id', 'name', 'phone', 'mobile', 'ref'], limit: 5 },
      ]);
    }

    // 3. Buscar productos por SKU en el cache local
    const skus = (parsed.productos || []).map(p => p.sku).filter(Boolean);
    const cache = await getOdooProducts();
    const productMap = {};
    for (const p of cache) {
      if (p.default_code) productMap[p.default_code] = p;
    }

    // Helper: busca SKU exacto, luego base (sin último segmento), luego sin dos últimos
    function findProduct(sku) {
      if (!sku) return null;
      if (productMap[sku]) return productMap[sku];
      const parts = sku.split('-');
      if (parts.length > 2) {
        const base2 = parts.slice(0, -1).join('-');
        if (productMap[base2]) return productMap[base2];
      }
      if (parts.length > 1) {
        const base1 = parts[0];
        if (productMap[base1]) return productMap[base1];
      }
      return null;
    }

    // 4. Devolver datos extraídos para que el usuario confirme
    const lineas = (parsed.productos || []).map(p => {
      const prod = findProduct(p.sku);
      return {
        sku:       p.sku,
        cantidad:  p.cantidad || 1,
        precio:    p.precio ?? prod?.list_price ?? prod?.lst_price ?? 0,
        found:     !!prod,
        product_id:   prod?.id || null,
        product_name: prod?.name || null,
        uom_id:       prod?.uom_id || null,
        tax_ids:      prod?.taxes_id || [],
      };
    });

    res.json({
      cliente_raw: parsed.cliente,
      partners,
      lineas,
      notas: parsed.notas || '',
    });
  } catch (err) {
    console.error('[cotizacion-foto] error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/odoo/cotizacion-crear', express.json(), async (req, res) => {
  try {
    const { partner_id, lineas, notas } = req.body;
    if (!partner_id) return res.status(400).json({ error: 'Se requiere partner_id' });
    if (!lineas?.length) return res.status(400).json({ error: 'Se requieren líneas de productos' });

    const uid = await odooAuth();

    const order_lines = lineas.map(l => [0, 0, {
      product_id:      l.product_id,
      name:            l.product_name,
      product_uom_qty: l.cantidad,
      price_unit:      l.precio,
      product_uom:     l.uom_id?.[0] || 1,
      tax_id:          [[6, 0, l.tax_ids || []]],
      ...(l.descuento ? { discount: l.descuento } : {}),
    }]);

    const orderId = await odooCall('/xmlrpc/2/object', 'execute_kw', [
      ODOO_DB, uid, ODOO_API_KEY, 'sale.order', 'create',
      [{
        partner_id,
        pricelist_id: 2, // MAYORISTA UYU
        date_order:   new Date().toISOString().replace('T', ' ').slice(0, 19),
        note:         notas || '',
        order_line:   order_lines,
      }],
    ]);

    // Leer el nombre asignado
    const [order] = await odooCall('/xmlrpc/2/object', 'execute_kw', [
      ODOO_DB, uid, ODOO_API_KEY, 'sale.order', 'read',
      [[orderId]], { fields: ['name', 'amount_total', 'partner_id'] },
    ]);

    res.json({ success: true, order_id: orderId, order_name: order.name, amount_total: order.amount_total });
  } catch (err) {
    console.error('[cotizacion-crear] error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── WhatsApp webhook (Twilio) ────────────────────────────────────
const TWILIO_SID   = process.env.TWILIO_ACCOUNT_SID;
const TWILIO_TOKEN = process.env.TWILIO_AUTH_TOKEN;

app.post('/webhook/whatsapp', express.urlencoded({ extended: false }), async (req, res) => {
  const twiml = new twilio.twiml.MessagingResponse();

  const reply = text => {
    twiml.message(text);
    res.type('text/xml').send(twiml.toString());
  };

  try {
    const numMedia = parseInt(req.body.NumMedia || '0');
    const from     = req.body.From;

    if (!numMedia) {
      return reply('Hola! Mandame una foto del pedido escrito a mano y lo cargo en Odoo automáticamente 📋');
    }

    const mediaUrl  = req.body.MediaUrl0;
    const mediaType = req.body.MediaContentType0 || 'image/jpeg';

    // Descargar imagen de Twilio usando el SDK
    const twilioClient = twilio(TWILIO_SID, TWILIO_TOKEN);
    const messageSid   = req.body.MessageSid || req.body.SmsSid;
    const mediaItems   = await twilioClient.messages(messageSid).media.list({ limit: 1 });
    if (!mediaItems.length) return reply('No se encontró imagen en el mensaje.');
    const mediaUri  = mediaItems[0].uri.replace('.json', '');
    const directUrl = `https://api.twilio.com${mediaUri}`;
    const imgResp = await axios.get(directUrl, {
      auth: { username: TWILIO_SID, password: TWILIO_TOKEN },
      responseType: 'arraybuffer',
    });
    const image_base64 = Buffer.from(imgResp.data).toString('base64');

    // Claude lee la imagen
    const msg = await anthropic.messages.create({
      model: 'claude-opus-4-6',
      max_tokens: 1024,
      messages: [{
        role: 'user',
        content: [
          { type: 'image', source: { type: 'base64', media_type: mediaType, data: image_base64 } },
          { type: 'text', text: `Analizá este pedido escrito a mano y extraé el cliente y los productos.

Reglas para variantes de SKU (formato {BASE}-{COLOR} o {BASE}-{COLOR}-{TEMP}):
- Colores: B/BL/Blanco=BLA, N/NG/Negro=NEG, G/GR/Gris=GRI, R/RO/Rosa=ROS, V/VE/Verde=VER, D/DO/Dorado=DOR, P/PL/Plateado=PLA
- Temperatura: F/FRI=FRI, C/CAL=CAL
- "18110: 2B-2N" → dos líneas: {sku:"18110-BLA",cantidad:2} y {sku:"18110-NEG",cantidad:2}
- "22306 3FRI-2CAL" → dos líneas: {sku:"22306-FRI",cantidad:3} y {sku:"22306-CAL",cantidad:2}
- "25203 2BLA-FRI" → una línea: {sku:"25203-BLA-FRI",cantidad:2}
- Si no hay variante, usá el SKU base tal cual

Respondé ÚNICAMENTE con JSON válido, sin texto adicional:
{
  "cliente": "nombre del cliente",
  "productos": [
    { "sku": "18110-BLA", "cantidad": 2 },
    { "sku": "18110-NEG", "cantidad": 2 }
  ]
}` },
        ],
      }],
    });

    let parsed;
    try {
      const text = msg.content[0].text.trim().replace(/^```json\n?/, '').replace(/\n?```$/, '');
      parsed = JSON.parse(text);
    } catch {
      return reply('No pude leer el pedido de la imagen. ¿Podés mandar una foto más clara?');
    }

    const uid = await odooAuth();

    // Buscar cliente
    const partners = await odooCall('/xmlrpc/2/object', 'execute_kw', [
      ODOO_DB, uid, ODOO_API_KEY, 'res.partner', 'search_read',
      [[['name', 'ilike', parsed.cliente], ['customer_rank', '>', 0]]],
      { fields: ['id', 'name'], limit: 1 },
    ]);

    if (!partners.length) {
      return reply(`No encontré el cliente "${parsed.cliente}" en Odoo. Verificá el nombre y volvé a intentar.`);
    }
    const partner = partners[0];

    // Buscar productos
    const skus = parsed.productos.map(p => p.sku);
    const products = await odooCall('/xmlrpc/2/object', 'execute_kw', [
      ODOO_DB, uid, ODOO_API_KEY, 'product.product', 'search_read',
      [[['default_code', 'in', skus], ['active', '=', true]]],
      { fields: ['id', 'name', 'default_code', 'lst_price', 'uom_id', 'taxes_id'] },
    ]);

    const productMap = {};
    for (const p of products) productMap[p.default_code] = p;

    const notFound = skus.filter(s => !productMap[s]);
    if (notFound.length) {
      return reply(`No encontré estos SKUs en Odoo: ${notFound.join(', ')}. Revisá y volvé a intentar.`);
    }

    // Crear cotización
    const order_lines = parsed.productos.map(p => {
      const prod = productMap[p.sku];
      return [0, 0, {
        product_id:      prod.id,
        name:            `[${p.sku}] ${prod.name}`,
        product_uom_qty: p.cantidad,
        price_unit:      prod.lst_price,
        product_uom:     prod.uom_id[0],
        tax_id:          [[6, 0, prod.taxes_id || []]],
      }];
    });

    const orderId = await odooCall('/xmlrpc/2/object', 'execute_kw', [
      ODOO_DB, uid, ODOO_API_KEY, 'sale.order', 'create',
      [{
        partner_id:   partner.id,
        pricelist_id: 2,
        date_order:   new Date().toISOString().replace('T', ' ').slice(0, 19),
        order_line:   order_lines,
      }],
    ]);

    const [order] = await odooCall('/xmlrpc/2/object', 'execute_kw', [
      ODOO_DB, uid, ODOO_API_KEY, 'sale.order', 'read',
      [[orderId]], { fields: ['name', 'amount_total'] },
    ]);

    const totalUnits = parsed.productos.reduce((s, p) => s + p.cantidad, 0);
    reply(`✅ Cotización creada en Odoo!\n\n📋 ${order.name}\n👤 ${partner.name}\n📦 ${parsed.productos.length} productos (${totalUnits} unidades)\n💰 $${order.amount_total.toLocaleString('es-UY')}`);

    console.log(`[whatsapp] cotización ${order.name} creada desde ${from}`);
  } catch (err) {
    console.error('[whatsapp] error:', err.message);
    reply('Ocurrió un error procesando el pedido. Intentá de nuevo.');
  }
});

// ── Usuarios y sesiones ──────────────────────────────────────────
const USERS_FILE = path.join(__dirname, 'data', 'users.json');

function hashPassword(password, salt) {
  return crypto.pbkdf2Sync(password, salt, 10000, 64, 'sha256').toString('hex');
}
function loadUsers() {
  try {
    if (fs.existsSync(USERS_FILE)) return JSON.parse(fs.readFileSync(USERS_FILE, 'utf8'));
  } catch(e) {}
  return [];
}
function saveUsers(data) {
  fs.writeFileSync(USERS_FILE, JSON.stringify(data, null, 2), 'utf8');
}

// Crear admin por defecto si no hay usuarios
(function ensureAdmin() {
  const users = loadUsers();
  if (!users.length) {
    const salt = crypto.randomBytes(16).toString('hex');
    users.push({
      id:       crypto.randomUUID(),
      username: 'admin',
      name:     'Administrador',
      role:     'admin',
      salt,
      hash:     hashPassword('admin123', salt),
      createdAt: new Date().toISOString(),
    });
    saveUsers(users);
    console.log('[usuarios] Usuario admin creado — password: admin123  (cambialo después)');
  }
})();

function createSession(userId) {
  const token = crypto.randomBytes(32).toString('hex');
  sessions.set(token, { userId, expiresAt: Date.now() + 7 * 24 * 60 * 60 * 1000 });
  return token;
}
function requireUser(req, res, next) {
  const token = req.headers['x-session-token'] || req.query._token;
  const session = token ? getSession(token) : null;
  if (!session) return res.status(401).json({ error: 'Sesión inválida' });
  const users = loadUsers();
  const user = users.find(u => u.id === session.userId);
  if (!user) return res.status(401).json({ error: 'Usuario no encontrado' });
  req.user = user;
  next();
}
function requireAdmin(req, res, next) {
  requireUser(req, res, () => {
    if (req.user.role !== 'admin') return res.status(403).json({ error: 'Sin permiso' });
    next();
  });
}

// POST /api/auth/login
app.post('/api/auth/login', (req, res) => {
  const { username, password } = req.body || {};
  if (!username || !password) return res.status(400).json({ error: 'Faltan credenciales' });
  const users = loadUsers();
  const user  = users.find(u => u.username === username);
  if (!user) return res.status(401).json({ error: 'Usuario o contraseña incorrectos' });
  const h = hashPassword(password, user.salt);
  if (h !== user.hash) return res.status(401).json({ error: 'Usuario o contraseña incorrectos' });
  const token = createSession(user.id);
  res.json({ token, user: { id: user.id, username: user.username, name: user.name, role: user.role } });
});

// POST /api/auth/logout
app.post('/api/auth/logout', (req, res) => {
  const token = req.headers['x-session-token'];
  if (token) sessions.delete(token);
  res.json({ ok: true });
});

// GET /api/auth/me
app.get('/api/auth/me', requireUser, (req, res) => {
  const { id, username, name, role } = req.user;
  res.json({ id, username, name, role });
});

// GET /api/users  (admin)
app.get('/api/users', requireAdmin, (req, res) => {
  res.json(loadUsers().map(({ id, username, name, role, createdAt }) => ({ id, username, name, role, createdAt })));
});

// POST /api/users  (admin)
app.post('/api/users', requireAdmin, (req, res) => {
  const { username, name, password, role } = req.body || {};
  if (!username || !password) return res.status(400).json({ error: 'username y password son requeridos' });
  const users = loadUsers();
  if (users.find(u => u.username === username)) return res.status(409).json({ error: 'El usuario ya existe' });
  const salt = crypto.randomBytes(16).toString('hex');
  const user = { id: crypto.randomUUID(), username, name: name || username, role: role || 'user', salt, hash: hashPassword(password, salt), createdAt: new Date().toISOString() };
  users.push(user);
  saveUsers(users);
  res.json({ id: user.id, username: user.username, name: user.name, role: user.role });
});

// PUT /api/users/:id  (admin — cambia nombre, rol o password)
app.put('/api/users/:id', requireAdmin, (req, res) => {
  const users = loadUsers();
  const idx   = users.findIndex(u => u.id === req.params.id);
  if (idx < 0) return res.status(404).json({ error: 'Usuario no encontrado' });
  const { name, role, password } = req.body || {};
  if (name)     users[idx].name = name;
  if (role)     users[idx].role = role;
  if (password) {
    const salt = crypto.randomBytes(16).toString('hex');
    users[idx].salt = salt;
    users[idx].hash = hashPassword(password, salt);
  }
  saveUsers(users);
  res.json({ id: users[idx].id, username: users[idx].username, name: users[idx].name, role: users[idx].role });
});

// DELETE /api/users/:id  (admin)
app.delete('/api/users/:id', requireAdmin, (req, res) => {
  const users = loadUsers();
  if (users.find(u => u.id === req.params.id)?.role === 'admin' &&
      users.filter(u => u.role === 'admin').length === 1)
    return res.status(400).json({ error: 'No podés eliminar el único admin' });
  saveUsers(users.filter(u => u.id !== req.params.id));
  res.json({ ok: true });
});

// ── ML: comisiones por categoría (cache en disco) ────────────────
const FEES_FILE = path.join(__dirname, 'data', 'ml_fees_cache.json');
let feesCache   = {};   // { category_id: { fee_pct, shipping_pct, updatedAt } }

try {
  if (fs.existsSync(FEES_FILE)) feesCache = JSON.parse(fs.readFileSync(FEES_FILE, 'utf8'));
} catch(e) {}

async function fetchCategoryFee(categoryId, samplePrice, headers) {
  try {
    const r = await axios.get(`${ML_API_URL}/sites/MLU/listing_prices`, {
      headers,
      params: { price: samplePrice, category_id: categoryId, listing_type_id: 'gold_special', currency_id: 'UYU' },
    });
    const fee_pct      = samplePrice > 0 ? parseFloat(((r.data.sale_fee_amount  || 0) / samplePrice * 100).toFixed(2)) : 0;
    const shipping_pct = samplePrice > 0 ? parseFloat(((r.data.free_shipping_cost?.cost || 0) / samplePrice * 100).toFixed(2)) : 0;
    return { fee_pct, shipping_cost: r.data.free_shipping_cost?.cost || 0, updatedAt: new Date().toISOString() };
  } catch(e) { return null; }
}

async function refreshFees(force = false) {
  if (!tokenData?.access_token) return;
  const headers = { Authorization: `Bearer ${tokenData.access_token}` };
  // Agrupar items por categoría con precio promedio
  const byCat = {};
  for (const item of cachedStock) {
    if (!item.category_id || !item.price) continue;
    if (!byCat[item.category_id]) byCat[item.category_id] = [];
    byCat[item.category_id].push(item.price);
  }
  let updated = 0;
  for (const [catId, prices] of Object.entries(byCat)) {
    if (!force && feesCache[catId] && feesCache[catId].updatedAt) {
      const age = Date.now() - new Date(feesCache[catId].updatedAt).getTime();
      if (age < 24 * 60 * 60 * 1000) continue; // usar cache si tiene menos de 24h
    }
    const avg = Math.round(prices.reduce((a,b) => a+b, 0) / prices.length);
    const fee = await fetchCategoryFee(catId, avg, headers);
    if (fee) { feesCache[catId] = fee; updated++; }
    await sleep(150);
  }
  if (updated > 0) {
    fs.writeFileSync(FEES_FILE, JSON.stringify(feesCache), 'utf8');
    console.log(`[fees] actualizadas ${updated} categorías`);
  }
}

// Config de costos de envío (editable por el usuario)
const SHIPPING_CFG_FILE = path.join(__dirname, 'data', 'shipping_config.json');
const DEFAULT_SHIPPING_CFG = {
  flex: {
    label: 'Flex (MUNDOSHOP)',
    note: 'Solo Montevideo y Canelones — costo por zona — vos siempre pagás',
    always_seller: true,
    // Zonas configurables con su costo. Para simular se usa el promedio ponderado.
    zones: [
      { name: 'Zona 1 (cerca)',  cost: 150 },
      { name: 'Zona 2 (media)',  cost: 200 },
      { name: 'Zona 3 (lejos)',  cost: 250 },
    ],
  },
  me2: {
    label: 'ME2 (Mercado Envíos / UES drop-off)',
    note: 'Vendedor paga solo si precio ≥ umbral — promedio histórico entre zonas',
    seller_threshold: 1200,
    avg_cost: 330,
  },
  me1: {
    label: 'ME1 (bultos grandes)',
    note: 'Costo fijo que vos configurás — se aplica por publicación o globalmente',
    always_seller: true,
    default_cost: 500,      // costo global por defecto
  },
};

function loadShippingCfg() {
  try {
    if (fs.existsSync(SHIPPING_CFG_FILE)) return JSON.parse(fs.readFileSync(SHIPPING_CFG_FILE, 'utf8'));
  } catch(e) {}
  return DEFAULT_SHIPPING_CFG;
}
function saveShippingCfg(data) {
  fs.writeFileSync(SHIPPING_CFG_FILE, JSON.stringify(data, null, 2), 'utf8');
}

// mlShippingCost: costo que da ML para ME2 (del feesCache)
function calcShippingCost(cfg, logisticType, price, mlShippingCost) {
  if (logisticType === 'self_service') {
    // Flex: promedio de zonas configuradas
    const zones = cfg.flex?.zones || [];
    if (!zones.length) return 0;
    return Math.round(zones.reduce((s, z) => s + z.cost, 0) / zones.length);
  }
  if (logisticType === 'drop_off') {
    // ME2: vendedor paga solo si precio >= threshold
    if (cfg.me2?.seller_threshold && price < cfg.me2.seller_threshold) return 0;
    return cfg.me2?.avg_cost || 0;
  }
  if (logisticType === 'default') {
    // ME1: costo fijo configurado por el vendedor
    return cfg.me1?.default_cost || 0;
  }
  return 0;
}

app.get('/api/ml/shipping-config', requireToken, (req, res) => res.json(loadShippingCfg()));
app.put('/api/ml/shipping-config', requireToken, (req, res) => {
  saveShippingCfg(req.body);
  res.json({ ok: true });
});

// GET /api/ml/fees — devuelve fees por categoría y dispara refresh si hace falta
app.get('/api/ml/fees', requireToken, async (req, res) => {
  const force = req.query.force === 'true';
  if (force || Object.keys(feesCache).length === 0) await refreshFees(force);
  res.json({ categories: Object.keys(feesCache).length, fees: feesCache });
});

// ── Cache de costos de envío reales por item (histórico 36 meses) ──
const SHIP_COSTS_FILE = path.join(__dirname, 'data', 'shipping_costs_by_item.json');
let shipCostsCache = {};   // { item_id: { avg_cost, count, logistic_type, sku } }
let shipCostsAnalyzing = false;
let shipCostsProgress  = { status: 'idle', orders: 0, shipments: 0, items: 0, error: null };

try {
  if (fs.existsSync(SHIP_COSTS_FILE)) {
    shipCostsCache = JSON.parse(fs.readFileSync(SHIP_COSTS_FILE, 'utf8'));
    console.log(`[ship-costs] cache cargado: ${Object.keys(shipCostsCache).length} items`);
  }
} catch(e) {}

// POST /api/ml/shipping-costs/analyze — analiza histórico 36 meses y promedia costo por item
app.post('/api/ml/shipping-costs/analyze', requireToken, async (req, res) => {
  if (shipCostsAnalyzing) return res.json({ ok: false, msg: 'Ya está corriendo', progress: shipCostsProgress });
  res.json({ ok: true, msg: 'Análisis iniciado en background' });
  shipCostsAnalyzing = true;
  shipCostsProgress  = { status: 'scanning', orders: 0, shipments: 0, items: 0, error: null };

  try {
    const headers = { Authorization: `Bearer ${tokenData.access_token}` };
    const uid     = tokenData.user_id;

    // 1. Scroll completo de órdenes (últimos 36 meses)
    const threeYrsAgo = new Date();
    threeYrsAgo.setFullYear(threeYrsAgo.getFullYear() - 3);

    const ordersWithShip = [];  // { shipping_id, item_id, sku, logistic_type }
    let scrollId = null;

    while (true) {
      const params = { seller: uid, limit: 50, search_type: 'scan' };
      if (scrollId) params.scroll_id = scrollId;
      const r = await axios.get(`${ML_API_URL}/orders/search`, { headers, params });
      const results = r.data.results || [];

      for (const order of results) {
        if (new Date(order.date_created) < threeYrsAgo) continue;
        const shipId    = order.shipping?.id;
        const logistic  = order.shipping?.logistic_type;
        if (!shipId) continue;
        for (const oi of (order.order_items || [])) {
          const itemId = oi.item?.id;
          const sku    = (oi.item?.seller_custom_field) || null;
          if (itemId) ordersWithShip.push({ shipping_id: shipId, item_id: itemId, sku, logistic_type: logistic });
        }
      }
      shipCostsProgress.orders = ordersWithShip.length;

      if (!results.length || !r.data.scroll_id) break;
      scrollId = r.data.scroll_id;
      await sleep(300);
    }

    console.log(`[ship-costs] ${ordersWithShip.length} líneas de orden con envío`);
    shipCostsProgress.status = 'fetching_shipments';

    // 2. Shipments únicos
    const uniqueShipIds = [...new Set(ordersWithShip.map(o => o.shipping_id))];
    const shipCostById  = {};   // { shipping_id: base_cost }

    for (let i = 0; i < uniqueShipIds.length; i += 5) {
      const batch = uniqueShipIds.slice(i, i + 5);
      await Promise.all(batch.map(async shipId => {
        try {
          const r = await axios.get(`${ML_API_URL}/shipments/${shipId}`, { headers });
          const base = r.data?.base_cost;
          if (base != null) shipCostById[shipId] = base;
        } catch(e) {}
      }));
      shipCostsProgress.shipments = Object.keys(shipCostById).length;
      if (i % 100 === 0) console.log(`[ship-costs] shipments fetched: ${shipCostsProgress.shipments}/${uniqueShipIds.length}`);
      await sleep(200);
    }

    // 3. Agrupar base_cost por item_id
    const byItem = {};
    for (const o of ordersWithShip) {
      const cost = shipCostById[o.shipping_id];
      if (cost == null || cost <= 0) continue;
      if (!byItem[o.item_id]) byItem[o.item_id] = { costs: [], logistic_type: o.logistic_type, sku: o.sku };
      byItem[o.item_id].costs.push(cost);
    }

    // 4. Calcular promedio y guardar
    const result = {};
    for (const [itemId, d] of Object.entries(byItem)) {
      const avg = d.costs.reduce((s, c) => s + c, 0) / d.costs.length;
      result[itemId] = { avg_cost: Math.round(avg), count: d.costs.length, logistic_type: d.logistic_type, sku: d.sku };
    }
    shipCostsCache = result;
    fs.writeFileSync(SHIP_COSTS_FILE, JSON.stringify(result), 'utf8');
    shipCostsProgress = { status: 'done', orders: ordersWithShip.length, shipments: Object.keys(shipCostById).length, items: Object.keys(result).length, error: null };
    console.log(`[ship-costs] análisis completo — ${Object.keys(result).length} items con costo histórico`);
  } catch(e) {
    shipCostsProgress = { ...shipCostsProgress, status: 'error', error: e.message };
    console.error('[ship-costs] error:', e.message);
  } finally {
    shipCostsAnalyzing = false;
  }
});

app.get('/api/ml/shipping-costs/status', requireToken, (req, res) => {
  res.json({ ...shipCostsProgress, cached_items: Object.keys(shipCostsCache).length });
});

// ── Analizador de publicaciones con Claude ───────────────────────
app.post('/api/ml/analizar-publicaciones', requireToken, async (req, res) => {
  const { item_ids } = req.body;
  if (!Array.isArray(item_ids) || item_ids.length === 0 || item_ids.length > 5)
    return res.status(400).json({ error: 'Enviá entre 1 y 5 item_ids' });

  const headers = { Authorization: `Bearer ${tokenData.access_token}` };

  try {
    // 1. Traer detalles completos de cada publicación
    const details = await Promise.all(item_ids.map(async id => {
      const [itemR, descR] = await Promise.all([
        axios.get(`${ML_API_URL}/items/${id}`, { headers }).catch(() => null),
        axios.get(`${ML_API_URL}/items/${id}/description`, { headers }).catch(() => null),
      ]);
      if (!itemR) return null;
      const item = itemR.data;
      // Atributos importantes vs faltantes
      const attrs = (item.attributes || []).map(a => ({
        id: a.id, name: a.name,
        value: a.value_name || a.values?.[0]?.name || null,
      }));
      const attrsFilled   = attrs.filter(a => a.value);
      const attrsEmpty    = attrs.filter(a => !a.value);
      // Info de stock del cache
      const stockItem = cachedStock.find(s => s.id === id);
      return {
        id, title: item.title, status: item.status,
        price: item.price, currency: item.currency_id,
        listing_type: item.listing_type_id,
        condition: item.condition,
        category_id: item.category_id,
        pictures_count: (item.pictures || []).length,
        pictures: (item.pictures || []).slice(0, 3).map(p => p.url),
        description: descR?.data?.plain_text?.slice(0, 800) || '',
        attrs_filled: attrsFilled.length,
        attrs_empty: attrsEmpty.length,
        attrs_empty_names: attrsEmpty.slice(0, 10).map(a => a.name),
        attrs_sample: attrsFilled.slice(0, 8).map(a => `${a.name}: ${a.value}`),
        sold30d: stockItem?.sold30d ?? null,
        stock: stockItem?.stock ?? null,
        permalink: item.permalink,
      };
    }));

    const validItems = details.filter(Boolean);
    if (!validItems.length) return res.status(404).json({ error: 'No se pudieron obtener los items' });

    // 2. Llamar a Claude para analizar
    const prompt = `Sos un experto en optimización de publicaciones de MercadoLibre Uruguay.
Analizá cada una de estas publicaciones y dá tips concretos y priorizados para mejorar la conversión.

Criterios clave para ML Uruguay:
- Título: máximo 60 caracteres, incluir marca + modelo + característica principal + condición. Usar keywords que buscan los compradores.
- Fotos: mínimo 6-8 fotos, fondo blanco en la principal, distintos ángulos, detalles importantes.
- Atributos: ML prioriza en búsqueda las publicaciones con atributos completos. Cada atributo vacío es una penalización.
- Descripción: clara, con las características más importantes primero, evitar texto genérico.
- Tipo de publicación: Gold Special tiene mejor posicionamiento.
- Precio: considerar si es competitivo para la categoría.

Para cada publicación respondé en este formato JSON exacto:
{
  "id": "MLU...",
  "score": 65,
  "resumen": "una línea con el estado general",
  "tips": [
    { "prioridad": "alta", "categoria": "titulo", "problema": "...", "accion": "..." },
    { "prioridad": "media", "categoria": "fotos", "problema": "...", "accion": "..." }
  ]
}

Categorías posibles: titulo, fotos, atributos, descripcion, precio, tipo_publicacion, otro.
Prioridades: alta, media, baja.
Máximo 5 tips por publicación, ordenados por impacto.

Publicaciones a analizar:
${JSON.stringify(validItems, null, 2)}

Respondé SOLO con un array JSON válido, sin texto adicional. No uses comillas dobles dentro de los strings — usá comillas simples o reemplazalas con espacios. No uses saltos de línea dentro de los valores de string.`;

    const msg = await anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 4096,
      messages: [{ role: 'user', content: prompt }],
    });

    const raw = msg.content[0].text.trim();
    // Extraer JSON del response — intentar parsear directamente, luego buscar array
    let analysis;
    const jsonMatch = raw.match(/\[[\s\S]*\]/s);
    if (!jsonMatch) return res.status(500).json({ error: 'Respuesta inválida de Claude', raw: raw.slice(0, 300) });
    try {
      analysis = JSON.parse(jsonMatch[0]);
    } catch(parseErr) {
      // Intentar extraer cada objeto individualmente
      analysis = [];
      for (const m of jsonMatch[0].matchAll(/\{[\s\S]*?"tips"[\s\S]*?\]\s*\}/g)) {
        try { analysis.push(JSON.parse(m[0])); } catch(e2) {}
      }
      if (!analysis.length)
        return res.status(500).json({ error: 'JSON inválido de Claude', detail: parseErr.message, raw: raw.slice(0, 300) });
    }

    res.json({ items: validItems, analysis });
  } catch(e) {
    console.error('[analizar-publicaciones]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── Mensajes post-venta ───────────────────────────────────────────

// GET /api/ml/mensajes/pendientes — hilos con último mensaje del comprador
app.get('/api/ml/mensajes/pendientes', requireToken, async (req, res) => {
  const headers = { Authorization: `Bearer ${tokenData.access_token}` };
  const uid = tokenData.user_id;
  const dias = parseInt(req.query.dias) || 14;

  try {
    const cutoff = new Date(Date.now() - dias * 24 * 60 * 60 * 1000);

    // Escanear órdenes en paralelo por páginas (en lotes de 5 páginas simultáneas)
    const allOrders = [];
    const seenPacks = new Set();

    // Primero obtener total para saber cuántas páginas hay en el rango
    const firstPage = await axios.get(`${ML_API_URL}/orders/search`, {
      headers, params: { seller: uid, limit: 50, sort: 'date_desc', offset: 0 }
    });
    const firstResults = firstPage.data.results || [];

    // Estimar cuántas páginas necesitamos (basado en que la última orden de la primera página tiene fecha X)
    // Si la primera página ya pasa el cutoff, listo
    let neededPages = 1;
    if (firstResults.length && new Date(firstResults[firstResults.length - 1].date_created) >= cutoff) {
      // Necesitamos más páginas — estimar cuántas
      const totalOrders = firstPage.data.paging?.total || 0;
      // Aproximar con un límite razonable de 20 páginas (1000 órdenes)
      neededPages = Math.min(20, Math.ceil(totalOrders / 50));
    }

    // Cargar todas las páginas necesarias en paralelo
    const pagePromises = [Promise.resolve(firstPage)];
    for (let p = 1; p < neededPages; p++) {
      pagePromises.push(axios.get(`${ML_API_URL}/orders/search`, {
        headers, params: { seller: uid, limit: 50, sort: 'date_desc', offset: p * 50 }
      }).catch(() => ({ data: { results: [] } })));
    }
    const pages = await Promise.all(pagePromises);

    for (const page of pages) {
      for (const o of page.data.results || []) {
        if (new Date(o.date_created) < cutoff) break;
        const key = o.pack_id || o.id;
        if (!seenPacks.has(key)) { seenPacks.add(key); allOrders.push(o); }
      }
    }

    const orders = allOrders;

    const itemMap = {};
    cachedItems.forEach(i => { itemMap[i.id] = i; });

    const threads = [];
    // Procesar en lotes de 30 para no saturar el rate limit
    const BATCH = 30;
    for (let i = 0; i < orders.length; i += BATCH) {
      await Promise.all(orders.slice(i, i + BATCH).map(async (order) => {
      const packOrOrder = order.pack_id || order.id;
      try {
        const mr = await axios.get(`${ML_API_URL}/messages/packs/${packOrOrder}/sellers/${uid}`, {
          headers, params: { tag: 'post_sale', limit: 50 }
        });
        const msgs = (mr.data.messages || []).filter(m => m.text && m.text.trim());
        if (!msgs.length) return;

        msgs.sort((a, b) => new Date(a.message_date.created) - new Date(b.message_date.created));
        const lastMsg = msgs[msgs.length - 1];
        const lastIsFromBuyer = lastMsg.from?.user_id !== uid;
        const lastDate = new Date(lastMsg.message_date.created);

        // Mostrar si: último mensaje del comprador reciente, O cualquier mensaje del comprador sin leer
        const hasUnreadFromBuyer = msgs.some(m => m.from?.user_id !== uid && !m.message_date?.read);
        if (!lastIsFromBuyer && !hasUnreadFromBuyer) return;
        if (lastDate < cutoff && !hasUnreadFromBuyer) return;

        const oi = order.order_items?.[0] || {};
        const itemId = oi.item?.id;
        const cachedItem = itemMap[itemId] || {};

        // Si no hay thumbnail en el cache, traerlo de ML (usa itemDetailCache para no repetir)
        let itemTitle = oi.item?.title || cachedItem.title || '';
        let itemThumbnail = cachedItem.thumbnail || '';
        if (itemId && !itemThumbnail) {
          const ctx = await fetchItemContext(itemId).catch(() => null);
          if (ctx) {
            if (!itemTitle) itemTitle = ctx.title || '';
            // fetchItemContext no devuelve thumbnail — buscar en cachedItems actualizado
            const fresh = itemMap[itemId];
            if (fresh?.thumbnail) itemThumbnail = fresh.thumbnail;
            else if (ctx.thumbnail) itemThumbnail = ctx.thumbnail;
          }
        }

        const shp = order.shipping || {};
        const addr = shp.receiver_address || {};
        const shipping = shp.id ? {
          id: shp.id,
          status: shp.status || '',
          receiver_name: addr.receiver_name || order.buyer?.nickname || '',
          address: addr.address_line || '',
          city: addr.city?.name || '',
          state: addr.state?.name || '',
          zip: addr.zip_code || '',
          comments: addr.comment || ''
        } : null;

        threads.push({
          order_id: order.id,
          pack_id: packOrOrder,
          buyer_id: order.buyer?.id,
          buyer_name: order.buyer?.nickname || '—',
          item_id: itemId,
          item_title: itemTitle || '—',
          item_thumbnail: itemThumbnail || '',
          order_status: order.status,
          total_amount: order.total_amount,
          shipping,
          last_message: lastMsg.text,
          last_message_date: lastMsg.message_date.created,
          unread: !lastMsg.message_date.read,
          messages: msgs.map(m => ({
            id: m.id,
            from_buyer: m.from?.user_id !== uid,
            text: m.text,
            date: m.message_date.created,
            read: !!m.message_date.read
          }))
        });
      } catch(e) { /* skip */ }
      })); // fin lote
    } // fin for lotes

    // Ordenar por fecha del último mensaje, más reciente primero
    threads.sort((a, b) => new Date(b.last_message_date) - new Date(a.last_message_date));
    res.json({ threads, total: threads.length });
  } catch(e) {
    console.error('[mensajes/pendientes]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/ml/mensajes/simular — sugiere respuesta IA para un hilo
app.post('/api/ml/mensajes/simular', requireToken, async (req, res) => {
  if (!anthropic) return res.status(400).json({ error: 'ANTHROPIC_API_KEY no configurada' });
  const { pack_id, item_id, item_title, order_status, messages } = req.body;
  if (!messages?.length) return res.status(400).json({ error: 'messages requerido' });

  try {
    let kb = null;
    if (fs.existsSync(QA_KB_FILE)) kb = JSON.parse(fs.readFileSync(QA_KB_FILE, 'utf8'));

    const kbText = kb ? `Estilo MUNDO SHOP:
- Saludo: "${kb.estilo.saludo}"
- Despedida: "${kb.estilo.despedida}"
- Tono: ${kb.estilo.tono}
Reglas:
${kb.reglas_generales.slice(0, 8).map(r => '- ' + r).join('\n')}` : '';

    const reglasNegocio = loadReglasNegocio();
    const reglasText = reglasNegocio.length
      ? `\nInformación del negocio:\n${reglasNegocio.map(r => `- ${r.categoria ? '[' + r.categoria + '] ' : ''}${r.texto}`).join('\n')}`
      : '';

    // Fetch item context
    const itemCtx = item_id ? await fetchItemContext(item_id) : null;
    const itemText = itemCtx ? buildItemContextText(itemCtx) : `Producto: ${item_title || 'no especificado'}`;

    const historial = messages.map(m =>
      `[${m.from_buyer ? 'COMPRADOR' : 'VENDEDOR'}]: ${m.text}`
    ).join('\n');

    const r = await anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 350,
      messages: [{
        role: 'user',
        content: `Sos el asistente post-venta de MUNDO SHOP en Mercado Libre Uruguay.
${kbText}
${reglasText ? 'REGLAS DEL NEGOCIO (usá estos datos exactos cuando apliquen, tienen prioridad):' + reglasText : ''}

${itemText}
Estado de la orden: ${order_status || 'desconocido'}

Historial del chat:
${historial}

El comprador espera respuesta. Generá una respuesta apropiada según el contexto.
Si hay un problema o reclamo implícito, sugerí también una acción concreta (ej: "Coordinar retiro", "Emitir reembolso", "Reenviar producto", etc.).
Respondé en JSON con este formato exacto:
{"respuesta":"texto de la respuesta","accion":null}
Si hay acción recomendada pon la acción en el campo accion, si no null.`
      }]
    });

    const text = r.content[0].text.trim();
    try {
      const match = text.match(/\{[\s\S]*\}/);
      const parsed = JSON.parse(match[0]);
      // Si respuesta es a su vez un JSON, extraer el campo interno
      if (typeof parsed.respuesta === 'string' && parsed.respuesta.trim().startsWith('{')) {
        try {
          const inner = JSON.parse(parsed.respuesta);
          if (inner.respuesta) { parsed.respuesta = inner.respuesta; if (!parsed.accion) parsed.accion = inner.accion; }
        } catch(_) {}
      }
      res.json(parsed);
    } catch(e) {
      // Último recurso: devolver el texto limpio sin JSON
      const clean = text.replace(/```json?/gi,'').replace(/```/g,'').trim();
      res.json({ respuesta: clean, accion: null });
    }
  } catch(e) {
    console.error('[mensajes/simular]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/ml/mensajes/responder — envía mensaje al comprador vía ML
app.post('/api/ml/mensajes/responder', requireToken, async (req, res) => {
  const { pack_id, buyer_id, text } = req.body;
  if (!pack_id || !buyer_id || !text) return res.status(400).json({ error: 'pack_id, buyer_id y text requeridos' });
  const uid = tokenData.user_id;
  try {
    const r = await axios.post(
      `${ML_API_URL}/messages/packs/${pack_id}/sellers/${uid}?tag=post_sale`,
      { from: { user_id: uid }, to: { user_id: buyer_id }, text },
      { headers: { Authorization: `Bearer ${tokenData.access_token}`, 'Content-Type': 'application/json' } }
    );
    res.json({ ok: true, message_id: r.data.id });
  } catch(e) {
    const detail = e.response?.data || e.message;
    console.error('[mensajes/responder]', detail);
    res.status(e.response?.status || 500).json({ error: detail });
  }
});

// ── Preguntas frecuentes por publicación ─────────────────────────
const PREGUNTAS_FILE  = path.join(__dirname, 'data', 'preguntas_por_publicacion.json');
const QA_KB_FILE      = path.join(__dirname, 'data', 'qa_knowledge_base.json');
const REGLAS_NEGOCIO_FILE = path.join(__dirname, 'data', 'reglas_negocio.json');

// Similitud por overlap de palabras (0 a 1) — sin dependencias externas
function similaridad(a, b) {
  const tokenize = s => s.toLowerCase().replace(/[^a-záéíóúüñ0-9\s]/g, '').split(/\s+/).filter(w => w.length > 2);
  const wa = new Set(tokenize(a));
  const wb = new Set(tokenize(b));
  if (!wa.size || !wb.size) return 0;
  let interseccion = 0;
  wa.forEach(w => { if (wb.has(w)) interseccion++; });
  return interseccion / Math.sqrt(wa.size * wb.size);
}

// Devuelve los N ejemplos aprendidos más similares a la pregunta
function buscarSimilares(pregunta, n = 8) {
  if (!fs.existsSync(LEARNED_FILE)) return [];
  try {
    const learned = JSON.parse(fs.readFileSync(LEARNED_FILE, 'utf8'));
    return learned
      .map(e => ({ ...e, score: similaridad(pregunta, e.pregunta) }))
      .filter(e => e.score > 0.1)
      .sort((a, b) => b.score - a.score)
      .slice(0, n);
  } catch(e) { return []; }
}

function loadReglasNegocio() {
  if (!fs.existsSync(REGLAS_NEGOCIO_FILE)) return [];
  try { return JSON.parse(fs.readFileSync(REGLAS_NEGOCIO_FILE, 'utf8')); } catch(e) { return []; }
}

// GET /api/config/reglas
app.get('/api/config/reglas', requireToken, (req, res) => {
  res.json(loadReglasNegocio());
});

// POST /api/config/reglas
app.post('/api/config/reglas', requireToken, (req, res) => {
  const { texto, categoria } = req.body;
  if (!texto?.trim()) return res.status(400).json({ error: 'texto requerido' });
  const reglas = loadReglasNegocio();
  const nueva = { id: Date.now(), texto: texto.trim(), categoria: categoria?.trim() || '' };
  reglas.push(nueva);
  fs.writeFileSync(REGLAS_NEGOCIO_FILE, JSON.stringify(reglas, null, 2));
  res.json(nueva);
});

// DELETE /api/config/reglas/:id
app.delete('/api/config/reglas/:id', requireToken, (req, res) => {
  const id = parseInt(req.params.id);
  const reglas = loadReglasNegocio().filter(r => r.id !== id);
  fs.writeFileSync(REGLAS_NEGOCIO_FILE, JSON.stringify(reglas, null, 2));
  res.json({ ok: true });
});

// GET /api/config/reglas/interpretar — Claude resume cómo va a aplicar las reglas
app.get('/api/config/reglas/interpretar', requireToken, async (req, res) => {
  if (!anthropic) return res.status(400).json({ error: 'ANTHROPIC_API_KEY no configurada' });
  const reglas = loadReglasNegocio();
  if (!reglas.length) return res.json({ interpretacion: '' });
  try {
    const r = await anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 400,
      messages: [{
        role: 'user',
        content: `Sos el asistente de MUNDO SHOP. Te dieron las siguientes reglas de negocio para usar al responder clientes en Mercado Libre:

${reglas.map(r => `- ${r.categoria ? '[' + r.categoria + '] ' : ''}${r.texto}`).join('\n')}

Resumí cada regla en formato diagrama de una línea: "situación → acción/dato clave". Una línea por regla, sin explicaciones, sin puntos, directo al grano. Ejemplo: "retiro muebles → Av. Italia 1234"`
      }]
    });
    res.json({ interpretacion: r.content[0].text.trim() });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/ml/preguntas/pendientes — preguntas sin responder de ML
app.get('/api/ml/preguntas/pendientes', requireToken, async (req, res) => {
  try {
    const dias = parseInt(req.query.dias) || 7; // por defecto últimos 7 días
    const r = await axios.get(`${ML_API_URL}/my/received_questions/search`, {
      params: { status: 'UNANSWERED', limit: 50 },
      headers: { Authorization: `Bearer ${tokenData.access_token}` }
    });
    const questions = r.data.questions || [];
    const cutoff = new Date(Date.now() - dias * 24 * 60 * 60 * 1000);
    const filtered = dias === 0 ? questions : questions.filter(q => new Date(q.date_created) >= cutoff);

    const itemMap = {};
    cachedItems.forEach(i => { itemMap[i.id] = i; });
    const enriched = await Promise.all(filtered.map(async q => {
      const item = itemMap[q.item_id] || {};
      let item_title = item.title || '';
      let item_thumbnail = item.thumbnail || '';
      if (q.item_id && (!item_title || !item_thumbnail)) {
        const ctx = await fetchItemContext(q.item_id).catch(() => null);
        if (ctx) {
          if (!item_title) item_title = ctx.title || q.item_id;
          if (!item_thumbnail) item_thumbnail = ctx.thumbnail || '';
        }
      }
      return {
        id: q.id,
        item_id: q.item_id,
        item_title: item_title || q.item_id,
        item_thumbnail,
        text: q.text,
        date_created: q.date_created,
        from_id: q.from?.id
      };
    }));
    res.json({ questions: enriched, total: enriched.length, total_ml: r.data.total });
  } catch(e) {
    console.error('[preguntas/pendientes]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// Helper: trae atributos + descripción de un ítem de ML
const itemDetailCache = {};
async function fetchItemContext(itemId) {
  if (itemDetailCache[itemId]) return itemDetailCache[itemId];
  try {
    const [itemR, descR] = await Promise.allSettled([
      axios.get(`${ML_API_URL}/items/${itemId}`, { headers: { Authorization: `Bearer ${tokenData.access_token}` } }),
      axios.get(`${ML_API_URL}/items/${itemId}/description`, { headers: { Authorization: `Bearer ${tokenData.access_token}` } })
    ]);
    const item = itemR.status === 'fulfilled' ? itemR.value.data : {};
    const desc = descR.status === 'fulfilled' ? descR.value.data?.plain_text || '' : '';

    // Extraer atributos relevantes
    const attrs = (item.attributes || [])
      .filter(a => a.value_name)
      .map(a => `${a.name}: ${a.value_name}`)
      .join(', ');

    const ctx = {
      title: item.title || itemId,
      price: item.price,
      thumbnail: item.thumbnail || '',
      attrs,
      description: desc.slice(0, 800)
    };
    itemDetailCache[itemId] = ctx;
    return ctx;
  } catch(e) {
    return { title: itemId, attrs: '', description: '' };
  }
}

function buildItemContextText(ctx) {
  let text = `Producto: ${ctx.title}`;
  if (ctx.price) text += `\nPrecio: $${ctx.price}`;
  if (ctx.attrs) text += `\nAtributos: ${ctx.attrs}`;
  if (ctx.description) text += `\nDescripción: ${ctx.description}`;
  return text;
}

// POST /api/ml/preguntas/simular — genera sugerencias IA para múltiples preguntas
app.post('/api/ml/preguntas/simular', requireToken, async (req, res) => {
  if (!anthropic) return res.status(400).json({ error: 'ANTHROPIC_API_KEY no configurada' });
  const { questions } = req.body; // [{ id, item_id, item_title, text }]
  if (!questions?.length) return res.status(400).json({ error: 'questions requerido' });

  let kb = null;
  if (fs.existsSync(QA_KB_FILE)) kb = JSON.parse(fs.readFileSync(QA_KB_FILE, 'utf8'));

  const kbText = kb ? `Estilo MUNDO SHOP:
- Saludo: "${kb.estilo.saludo}"
- Despedida: "${kb.estilo.despedida}"
- Tono: ${kb.estilo.tono}
Reglas clave:
${kb.reglas_generales.slice(0, 10).map(r => '- ' + r).join('\n')}` : '';

  const reglasNegocio = loadReglasNegocio();
  const reglasText = reglasNegocio.length
    ? `\nInformación del negocio:\n${reglasNegocio.map(r => `- ${r.categoria ? '[' + r.categoria + '] ' : ''}${r.texto}`).join('\n')}`
    : '';

  // Load QA examples per item
  let preguntasData = null;
  if (fs.existsSync(PREGUNTAS_FILE)) {
    try { preguntasData = JSON.parse(fs.readFileSync(PREGUNTAS_FILE, 'utf8')); } catch(e) {}
  }

  const results = [];
  for (const q of questions) {
    try {
      // Fetch real item data from ML
      const itemCtx = await fetchItemContext(q.item_id);
      const itemText = buildItemContextText(itemCtx);

      let ejemplos = '';
      if (preguntasData && q.item_id && preguntasData.byPub[q.item_id]) {
        const prevQA = preguntasData.byPub[q.item_id].qa.slice(-8); // últimas 8 incluyendo aprendidas
        if (prevQA.length) {
          ejemplos = '\nEjemplos anteriores de esta publicación:\n' +
            prevQA.map(e => `P: ${e.q}\nR: ${e.a}`).join('\n---\n');
        }
      }
      // Agregar respuestas aprendidas más similares a la pregunta actual
      const similares = buscarSimilares(q.text, 6);
      if (similares.length) {
        ejemplos += '\nRespuestas validadas similares:\n' +
          similares.map(e => `P: ${e.pregunta}\nR: ${e.respuesta}`).join('\n---\n');
      }
      const r = await anthropic.messages.create({
        model: 'claude-sonnet-4-6',
        max_tokens: 250,
        messages: [{
          role: 'user',
          content: `Sos el asistente de MUNDO SHOP en Mercado Libre Uruguay.
${kbText}
${reglasText ? 'REGLAS DEL NEGOCIO (usá estos datos exactos cuando apliquen, tienen prioridad):' + reglasText : ''}
${ejemplos}
${itemText}
Pregunta: "${q.text}"

Instrucciones:
- Responde SOLO con el texto final a enviar, sin explicaciones
- Las direcciones de retiro mencionarlas SOLO si preguntan explícitamente cómo retirar un producto ya comprado, no para "verlo" o visitarlo
- MUNDO SHOP debe aparecer UNA SOLA VEZ, al final de la despedida
- Si la info no está en las reglas, no la inventes`
        }]
      });
      results.push({ id: q.id, respuesta: r.content[0].text.trim() });
    } catch(e) {
      results.push({ id: q.id, error: e.message });
    }
  }
  res.json({ results });
});

// POST /api/ml/preguntas/feedback — guarda respuesta buena en el historial para aprendizaje
const LEARNED_FILE = path.join(__dirname, 'data', 'respuestas_aprendidas.json');
app.post('/api/ml/preguntas/feedback', requireToken, (req, res) => {
  const { pregunta, respuesta, item_id, item_title, tipo } = req.body;
  if (!pregunta || !respuesta) return res.status(400).json({ error: 'pregunta y respuesta requeridos' });
  try {
    let learned = [];
    if (fs.existsSync(LEARNED_FILE)) learned = JSON.parse(fs.readFileSync(LEARNED_FILE, 'utf8'));
    learned.push({ pregunta, respuesta, item_id, item_title, tipo: tipo || 'pregunta', fecha: new Date().toISOString() });
    // Mantener últimas 500 entradas
    if (learned.length > 500) learned = learned.slice(-500);
    fs.writeFileSync(LEARNED_FILE, JSON.stringify(learned, null, 2));

    // Si tiene item_id, guardar también en preguntas_por_publicacion para enriquecer el historial
    if (item_id && fs.existsSync(PREGUNTAS_FILE)) {
      const data = JSON.parse(fs.readFileSync(PREGUNTAS_FILE, 'utf8'));
      if (!data.byPub[item_id]) data.byPub[item_id] = { titulo: item_title || item_id, qa: [] };
      data.byPub[item_id].qa.push({ q: pregunta, a: respuesta, aprendida: true });
      fs.writeFileSync(PREGUNTAS_FILE, JSON.stringify(data));
    }

    res.json({ ok: true, total_aprendidas: learned.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/ml/preguntas/responder-ml — publica respuesta en ML y auto-aprende
app.post('/api/ml/preguntas/responder-ml', requireToken, async (req, res) => {
  const { question_id, text, pregunta, item_id, item_title } = req.body;
  if (!question_id || !text) return res.status(400).json({ error: 'question_id y text requeridos' });
  try {
    const r = await axios.post(`${ML_API_URL}/answers`,
      { question_id, text },
      { headers: { Authorization: `Bearer ${tokenData.access_token}`, 'Content-Type': 'application/json' } }
    );

    // Auto-aprendizaje: guardar en base de conocimiento cada respuesta enviada a ML
    if (pregunta) {
      try {
        let learned = [];
        if (fs.existsSync(LEARNED_FILE)) learned = JSON.parse(fs.readFileSync(LEARNED_FILE, 'utf8'));
        // Evitar duplicados exactos
        const yaExiste = learned.some(e => e.pregunta === pregunta && e.respuesta === text);
        if (!yaExiste) {
          learned.push({ pregunta, respuesta: text, item_id: item_id || null, item_title: item_title || null, tipo: 'pregunta', fecha: new Date().toISOString() });
          if (learned.length > 2000) learned = learned.slice(-2000);
          fs.writeFileSync(LEARNED_FILE, JSON.stringify(learned, null, 2));
          // Guardar también en preguntas_por_publicacion
          if (item_id && fs.existsSync(PREGUNTAS_FILE)) {
            const data = JSON.parse(fs.readFileSync(PREGUNTAS_FILE, 'utf8'));
            if (!data.byPub[item_id]) data.byPub[item_id] = { titulo: item_title || item_id, qa: [] };
            const yaEnPub = data.byPub[item_id].qa.some(e => e.q === pregunta);
            if (!yaEnPub) {
              data.byPub[item_id].qa.push({ q: pregunta, a: text, aprendida: true });
              fs.writeFileSync(PREGUNTAS_FILE, JSON.stringify(data));
            }
          }
          console.log(`[auto-learn] guardado: "${pregunta.slice(0, 50)}..."`);
        }
      } catch(e) { console.error('[auto-learn]', e.message); }
    }

    res.json({ ok: true, data: r.data });
  } catch(e) {
    const detail = e.response?.data || e.message;
    console.error('[responder-ml]', detail);
    res.status(e.response?.status || 500).json({ error: detail });
  }
});

// POST /api/ml/preguntas/importar-historial — importa preguntas ya respondidas desde ML API
let importState = { running: false, progress: 0, total: 0, importadas: 0, error: null };
app.get('/api/ml/preguntas/importar-estado', requireToken, (req, res) => res.json(importState));

app.post('/api/ml/preguntas/importar-historial', requireToken, async (req, res) => {
  if (importState.running) return res.json({ ok: false, msg: 'ya corriendo' });
  importState = { running: true, progress: 0, total: 0, importadas: 0, error: null };
  res.json({ ok: true, msg: 'importación iniciada' });

  (async () => {
    try {
      let offset = 0;
      const limit = 50;
      let total = null;
      let learned = fs.existsSync(LEARNED_FILE) ? JSON.parse(fs.readFileSync(LEARNED_FILE, 'utf8')) : [];
      let preguntasData = fs.existsSync(PREGUNTAS_FILE) ? JSON.parse(fs.readFileSync(PREGUNTAS_FILE, 'utf8')) : { byPub: {} };
      const existentes = new Set(learned.map(e => e.pregunta + '||' + e.respuesta));
      let importadas = 0;

      while (true) {
        const r = await axios.get(`${ML_API_URL}/my/received_questions/search`, {
          params: { status: 'ANSWERED', limit, offset },
          headers: { Authorization: `Bearer ${tokenData.access_token}` }
        });
        const qs = r.data.questions || [];
        if (total === null) { total = r.data.total || 0; importState.total = total; }
        if (!qs.length) break;

        for (const q of qs) {
          const answer = q.answer?.text;
          if (!q.text || !answer) continue;
          const key = q.text + '||' + answer;
          if (existentes.has(key)) continue;
          existentes.add(key);
          learned.push({
            pregunta: q.text,
            respuesta: answer,
            item_id: q.item_id || null,
            item_title: null,
            tipo: 'pregunta',
            fecha: q.date_created || new Date().toISOString()
          });
          // Guardar en preguntas_por_publicacion
          if (q.item_id) {
            if (!preguntasData.byPub[q.item_id]) preguntasData.byPub[q.item_id] = { titulo: q.item_id, qa: [] };
            const yaEnPub = preguntasData.byPub[q.item_id].qa.some(e => e.q === q.text);
            if (!yaEnPub) preguntasData.byPub[q.item_id].qa.push({ q: q.text, a: answer });
          }
          importadas++;
        }

        offset += qs.length;
        importState.progress = offset;
        importState.importadas = importadas;
        if (offset >= total) break;
        await sleep(300); // respetar rate limit ML
      }

      if (learned.length > 5000) learned = learned.slice(-5000);
      fs.writeFileSync(LEARNED_FILE, JSON.stringify(learned, null, 2));
      fs.writeFileSync(PREGUNTAS_FILE, JSON.stringify(preguntasData));
      importState = { running: false, progress: offset, total, importadas, error: null };
      console.log(`[importar-historial] importadas ${importadas} preguntas nuevas`);
    } catch(e) {
      importState = { running: false, progress: importState.progress, total: importState.total, importadas: importState.importadas, error: e.message };
      console.error('[importar-historial]', e.message);
    }
  })();
});

// GET /api/ml/preguntas/stats — top publicaciones por preguntas
app.get('/api/ml/preguntas/stats', requireToken, (req, res) => {
  try {
    if (!fs.existsSync(PREGUNTAS_FILE)) return res.json({ pubs: [], total: 0 });
    const data = JSON.parse(fs.readFileSync(PREGUNTAS_FILE, 'utf8'));
    const pubs = Object.entries(data.byPub).map(([id, p]) => ({
      id,
      titulo: p.titulo,
      total: p.qa.length,
      categorias: p.categorias || {}
    })).sort((a, b) => b.total - a.total);
    res.json({ pubs, total: pubs.reduce((s, p) => s + p.total, 0) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/ml/preguntas/:itemId — preguntas de una publicación
app.get('/api/ml/preguntas/:itemId', requireToken, (req, res) => {
  try {
    if (!fs.existsSync(PREGUNTAS_FILE)) return res.json({ qa: [] });
    const data = JSON.parse(fs.readFileSync(PREGUNTAS_FILE, 'utf8'));
    const pub  = data.byPub[req.params.itemId];
    if (!pub) return res.json({ qa: [], titulo: '' });
    res.json({ qa: pub.qa, titulo: pub.titulo });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/ml/preguntas/responder — responder pregunta con IA usando knowledge base
app.post('/api/ml/preguntas/responder', requireToken, async (req, res) => {
  if (!anthropic) return res.status(400).json({ error: 'ANTHROPIC_API_KEY no configurada' });
  const { pregunta, itemId, titulo } = req.body;
  if (!pregunta) return res.status(400).json({ error: 'pregunta requerida' });

  try {
    let kb = null;
    if (fs.existsSync(QA_KB_FILE)) kb = JSON.parse(fs.readFileSync(QA_KB_FILE, 'utf8'));

    // Ejemplos por publicación
    let ejemplosPub = [];
    if (itemId && fs.existsSync(PREGUNTAS_FILE)) {
      const data = JSON.parse(fs.readFileSync(PREGUNTAS_FILE, 'utf8'));
      const pub  = data.byPub[itemId];
      if (pub) ejemplosPub = pub.qa.slice(-10);
    }

    // Ejemplos similares de toda la base aprendida
    const similares = buscarSimilares(pregunta, 6);

    const kbText = kb ? `
Estilo de respuesta:
- Saludo: "${kb.estilo.saludo}"
- Despedida: "${kb.estilo.despedida}"
- Tono: ${kb.estilo.tono}

Reglas:
${kb.reglas_generales.slice(0, 8).map(r => '- ' + r).join('\n')}
` : '';

    const ejemplosPubText = ejemplosPub.length ? `
Ejemplos anteriores para esta publicación:
${ejemplosPub.slice(0, 5).map(e => `P: ${e.q}\nR: ${e.a}`).join('\n---\n')}
` : '';

    const similoresText = similares.length ? `
Respuestas validadas similares (de otras publicaciones):
${similares.map(e => `P: ${e.pregunta}\nR: ${e.respuesta}`).join('\n---\n')}
` : '';

    const ejemplosText = ejemplosPubText + similoresText;

    // Fetch real item context from ML
    const itemCtx = itemId ? await fetchItemContext(itemId) : null;
    const itemText = itemCtx ? buildItemContextText(itemCtx) : `Producto: ${titulo || 'no especificado'}`;

    const r = await anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 300,
      messages: [{
        role: 'user',
        content: `Sos el asistente de MUNDO SHOP en Mercado Libre Uruguay. Responde la siguiente pregunta de un comprador.
${kbText}${ejemplosText}${(() => { const r = loadReglasNegocio(); return r.length ? '\nInformación del negocio:\n' + r.map(x => `- ${x.categoria ? '['+x.categoria+'] ' : ''}${x.texto}`).join('\n') : ''; })()}
${itemText}

Pregunta del comprador: "${pregunta}"

Responde SOLO con el texto de la respuesta, sin explicaciones adicionales. Si no sabes un dato específico, no lo inventes — decí que lo consulten por el chat de la compra.`
      }]
    });

    res.json({ respuesta: r.content[0].text.trim() });
  } catch(e) {
    console.error('[preguntas/responder]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── ML: sin descuento y ofertas ──────────────────────────────────

// GET /api/ml/rentabilidad — simulación de rentabilidad por producto
app.get('/api/ml/rentabilidad', requireToken, async (req, res) => {
  // 1. Asegurar fees cargadas
  if (Object.keys(feesCache).length === 0) await refreshFees(false);

  // 2. Costos desde Odoo
  const costoBySku = {};
  try {
    if (fs.existsSync(ODOO_CACHE_FILE)) {
      const odooRaw = JSON.parse(fs.readFileSync(ODOO_CACHE_FILE, 'utf8'));
      const xmlrpc  = require('xmlrpc');
      const uid     = await new Promise((resolve, reject) => {
        const c = xmlrpc.createSecureClient({ host: ODOO_HOST, path: '/xmlrpc/2/common' });
        c.methodCall('authenticate', [ODOO_DB, ODOO_USER, ODOO_API_KEY, {}], (e,v) => e ? reject(e) : resolve(v));
      });
      // Traer standard_price de todos los productos con SKU
      const skus   = [...new Set(cachedStock.map(i => i.sku).filter(Boolean))];
      const batch  = 200;
      for (let i = 0; i < skus.length; i += batch) {
        const slice = skus.slice(i, i + batch);
        const prods = await odooSearchRead(uid, 'product.product', [['default_code','in',slice]], ['default_code','standard_price']);
        for (const p of prods) costoBySku[p.default_code] = p.standard_price || 0;
        await sleep(100);
      }
    }
  } catch(e) { console.error('[rentabilidad] error cargando costos Odoo:', e.message); }

  // 3. Cargar config de envíos
  const shippingCfg = loadShippingCfg();

  // 4. Calcular rentabilidad para cada item de ofertas
  const ofertasItems = cachedStock
    .filter(i => !i.original_price)
    .map(i => {
      const fee        = feesCache[i.category_id] || { fee_pct: 13 };
      const costo      = costoBySku[i.sku] || 0;
      const feePct     = fee.fee_pct || 13;
      const logistic   = i.logistic_type || 'drop_off';
      // Usar costo histórico real si existe, si no, usar config global
      const histCost   = shipCostsCache[i.id];
      const envio      = histCost ? histCost.avg_cost : calcShippingCost(shippingCfg, logistic, i.price);

      const IVA = 1.22; // IVA Uruguay 22%

      // Margen real al precio actual (para referencia)
      const precioSinIvaActual = i.price / IVA;
      const comisionActual     = i.price * feePct / 100;
      const netoActual         = precioSinIvaActual - comisionActual - envio;
      const margenActual       = costo > 0 ? parseFloat(((netoActual - costo) / precioSinIvaActual * 100).toFixed(1)) : null;

      // Calcular precio necesario para alcanzar un margen objetivo
      // Para ME2: el envío depende de si el precio >= threshold (circular) → resuelvo con 2 casos
      function calcPrecioParaMargen(targetPct) {
        if (!costo) return null;
        const t       = targetPct / 100;
        const divisor = (1 - t) / IVA - feePct / 100;
        if (divisor <= 0) return null;

        const me2Threshold = shippingCfg.me2?.seller_threshold || 1200;

        function resolver(envioAsumido) {
          return Math.round((envioAsumido + costo) / divisor);
        }

        let precio, envioUsado;

        if (logistic === 'drop_off') {
          // Caso 1: asumir que vendedor paga envío
          const p1 = resolver(calcShippingCost(shippingCfg, 'drop_off', me2Threshold)); // precio como si pagara
          if (p1 >= me2Threshold) {
            // Consistente: precio >= threshold y vendedor paga
            precio = p1;
            envioUsado = calcShippingCost(shippingCfg, 'drop_off', p1);
          } else {
            // Caso 2: precio < threshold → comprador paga envío (envio=0)
            precio = resolver(0);
            envioUsado = 0;
          }
        } else {
          envioUsado = envio; // Flex y ME1: no cambia con el precio
          precio = resolver(envioUsado);
        }

        const descuento = parseFloat(((1 - precio / i.price) * 100).toFixed(1));
        return { precio, descuento, envioUsado };
      }

      const sim = {
        actual: margenActual,
        m30: calcPrecioParaMargen(30),
        m20: calcPrecioParaMargen(20),
        m15: calcPrecioParaMargen(15),
        m10: calcPrecioParaMargen(10),
        m0:  calcPrecioParaMargen(0),  // break-even
      };

      // Score oferta (igual que antes)
      let score = 0, tipo = '';
      if (i.sold30d === 0 && i.days_left === null)                              { score = 100; tipo = 'sin_ventas'; }
      else if (i.days_left > 90 && i.sold30d === 0)                            { score = 95;  tipo = 'overstock_sin_ventas'; }
      else if (i.days_left > 90 && i.sold30d <= 2)                             { score = 80;  tipo = 'overstock_lento'; }
      else if (i.sold30d <= 1 && i.stock > 5)                                  { score = 60;  tipo = 'muy_lento'; }
      else if (i.sold30d <= 3 && i.days_left !== null && i.days_left > 60)     { score = 40;  tipo = 'lento'; }

      return {
        id: i.id, title: i.title, thumbnail: i.thumbnail, permalink: i.permalink,
        sku: i.sku, price: i.price, stock: i.stock, status: i.status, sold30d: i.sold30d,
        days_left: i.days_left, category_name: i.category_name,
        costo, fee_pct: feePct, envio, logistic_type: logistic, score, tipo, sim,
        tiene_costo: costo > 0,
      };
    })
    .filter(i => i.score > 0)
    .sort((a, b) => b.score - a.score);

  res.json({ items: ofertasItems, total: ofertasItems.length, lastUpdated: stockLastUpdate });
});

// GET /api/ml/sin-descuento — productos con stock y sin descuento en ML
app.get('/api/ml/sin-descuento', requireToken, (req, res) => {
  const items = cachedStock
    .filter(i => i.sku && i.stock > 0 && !i.original_price)
    .map(i => ({
      id: i.id, title: i.title, thumbnail: i.thumbnail, permalink: i.permalink,
      sku: i.sku, price: i.price, stock: i.stock, sold30d: i.sold30d,
      daily_rate: i.daily_rate, days_left: i.days_left, category_name: i.category_name,
    }));
  res.json({ items, total: items.length, lastUpdated: stockLastUpdate });
});

// GET /api/ml/ofertas — candidatos para oferta del día / relámpago
// Criterios: stock > 0, pocas ventas (sold30d <= 2 o sin ventas), overstock
app.get('/api/ml/ofertas', requireToken, (req, res) => {
  const items = cachedStock
    .filter(i => i.stock > 0 && !i.original_price)
    .map(i => {
      // Score: más urgente = más candidato a oferta
      let score = 0;
      let tipo  = '';
      if (i.sold30d === 0 && i.days_left === null) { score = 100; tipo = 'sin_ventas'; }
      else if (i.days_left !== null && i.days_left > 90 && i.sold30d === 0) { score = 95; tipo = 'overstock_sin_ventas'; }
      else if (i.days_left !== null && i.days_left > 90 && i.sold30d <= 2) { score = 80; tipo = 'overstock_lento'; }
      else if (i.sold30d <= 1 && i.stock > 5)  { score = 60; tipo = 'muy_lento'; }
      else if (i.sold30d <= 3 && i.days_left !== null && i.days_left > 60) { score = 40; tipo = 'lento'; }
      return { ...i, score, tipo };
    })
    .filter(i => i.score > 0)
    .sort((a, b) => b.score - a.score || (b.days_left ?? 9999) - (a.days_left ?? 9999));

  res.json({ items, total: items.length, lastUpdated: stockLastUpdate });
});

// ── Previsiones / Reglas de reabastecimiento ─────────────────────
const REGLAS_FILE = path.join(__dirname, 'data', 'reglas_reabastecimiento.json');

function loadReglas() {
  try {
    if (fs.existsSync(REGLAS_FILE)) return JSON.parse(fs.readFileSync(REGLAS_FILE, 'utf8'));
  } catch(e) {}
  return [];
}
function saveReglas(data) {
  fs.writeFileSync(REGLAS_FILE, JSON.stringify(data, null, 2), 'utf8');
}

// GET /api/previsiones/reglas
app.get('/api/previsiones/reglas', requireToken, (req, res) => {
  res.json(loadReglas());
});

// POST /api/previsiones/reglas — crea o actualiza por SKU
app.post('/api/previsiones/reglas', requireToken, (req, res) => {
  const { sku, lead_time_days, safety_days, notes } = req.body;
  if (!sku) return res.status(400).json({ error: 'Falta SKU' });
  const reglas = loadReglas();
  const idx = reglas.findIndex(r => r.sku === sku);
  const regla = { sku, lead_time_days: parseInt(lead_time_days) || 30, safety_days: parseInt(safety_days) || 7, notes: notes || '' };
  if (idx >= 0) reglas[idx] = regla; else reglas.push(regla);
  saveReglas(reglas);
  res.json(regla);
});

// DELETE /api/previsiones/reglas/:sku
app.delete('/api/previsiones/reglas/:sku', requireToken, (req, res) => {
  saveReglas(loadReglas().filter(r => r.sku !== req.params.sku));
  res.json({ ok: true });
});

// GET /api/previsiones — forecast con stock actual + entrante + reglas
app.get('/api/previsiones', requireToken, (req, res) => {
  const reglas = loadReglas();
  const reglaMap = {};
  for (const r of reglas) reglaMap[r.sku] = r;

  const compras = loadCompras();
  // incoming por SKU: { sku -> [{qty, expected_date, supplier, order_id}] }
  const incoming = {};
  for (const c of compras) {
    for (const it of (c.items || [])) {
      if (!it.sku) continue;
      if (!incoming[it.sku]) incoming[it.sku] = [];
      incoming[it.sku].push({ qty: parseInt(it.qty) || 0, expected_date: c.expected_date, supplier: c.supplier, order_id: c.id });
    }
  }

  const today = new Date();
  today.setHours(0, 0, 0, 0);

  const items = cachedStock
    .filter(item => item.sku) // solo items con SKU
    .map(item => {
      const itemIncoming = (incoming[item.sku] || []).map(e => {
        const arrivalDate = e.expected_date ? new Date(e.expected_date) : null;
        const daysUntilArrival = arrivalDate ? Math.ceil((arrivalDate - today) / 86400000) : null;
        return { ...e, days_until_arrival: daysUntilArrival };
      }).sort((a, b) => (a.days_until_arrival ?? 9999) - (b.days_until_arrival ?? 9999));

      const totalIncoming = itemIncoming.reduce((s, e) => s + e.qty, 0);

      // Días de stock sin entrantes
      const daysLeft = item.days_left;
      // Días de stock con entrantes (simplificado: suma total incoming / daily_rate)
      const daysLeftWithIncoming = item.daily_rate > 0
        ? Math.round((item.stock + totalIncoming) / item.daily_rate)
        : (item.stock + totalIncoming > 0 ? null : 0);

      const regla = reglaMap[item.sku] || { lead_time_days: 30, safety_days: 7 };
      const reorder_point_days = regla.lead_time_days + regla.safety_days;

      // Cuándo debería emitirse la orden de compra (días desde hoy)
      // El stock (con entrantes) debería bajar hasta reorder_point_days
      let days_until_order = null;
      let status = 'ok';
      let should_order = false;

      if (item.daily_rate > 0) {
        // Tiempo hasta que el stock (con entrantes) llegue al punto de reorden
        const stockWithIncoming = item.stock + totalIncoming;
        const targetStock = reorder_point_days * item.daily_rate;
        days_until_order = Math.round((stockWithIncoming - targetStock) / item.daily_rate);

        if (days_until_order <= 0) {
          status = 'pedir_ya';
          should_order = true;
        } else if (days_until_order <= 7) {
          status = 'pedir_pronto';
        } else if (days_until_order <= 30) {
          status = 'atención';
        } else {
          status = 'ok';
        }
      } else if (item.stock === 0 && totalIncoming === 0) {
        status = 'sin_stock';
        should_order = true;
      } else if (item.stock === 0) {
        status = 'sin_stock_con_entrante';
      } else {
        status = 'sin_ventas';
      }

      return {
        id:                   item.id,
        title:                item.title,
        thumbnail:            item.thumbnail,
        permalink:            item.permalink,
        sku:                  item.sku,
        stock:                item.stock,
        sold30d:              item.sold30d,
        daily_rate:           item.daily_rate,
        days_left:            daysLeft,
        incoming:             itemIncoming,
        total_incoming:       totalIncoming,
        days_left_with_inc:   daysLeftWithIncoming,
        lead_time_days:       regla.lead_time_days,
        safety_days:          regla.safety_days,
        reorder_point_days,
        days_until_order,
        status,
        should_order,
      };
    });

  // Ordenar: pedir_ya → pedir_pronto → atención → sin_stock → ok → sin_ventas
  const ORDER = { pedir_ya: 0, sin_stock: 1, pedir_pronto: 2, sin_stock_con_entrante: 3, atención: 4, ok: 5, sin_ventas: 6 };
  items.sort((a, b) => (ORDER[a.status] ?? 9) - (ORDER[b.status] ?? 9) || (a.days_until_order ?? 9999) - (b.days_until_order ?? 9999));

  res.json({ items, total: items.length, lastUpdated: stockLastUpdate });
});

app.get('/api/debug/shipment/:id', requireToken, async (req, res) => {
  try {
    const r = await axios.get(`${ML_API_URL}/shipments/${req.params.id}`, {
      headers: { Authorization: `Bearer ${tokenData.access_token}` },
    });
    res.json(r.data);
  } catch(e) { res.status(500).json({ error: e.response?.data || e.message }); }
});

app.listen(PORT, () => {
  console.log(`Servidor corriendo en http://localhost:${PORT}`);
  console.log(`Iniciá el flujo OAuth en http://localhost:${PORT}/login`);
});
