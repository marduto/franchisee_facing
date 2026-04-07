// update.js — Netlify Function
// POST /api/update
// 1. UPDATEs kfc_assetuniverse_2025
// 2. If ic_date >= today, syncs INSERT/UPDATE to kfc_ic_agenda
const https = require('https');
const crypto = require('crypto');

const JWT_SECRET = process.env.JWT_SECRET || 'kfc-franchisee-app-secret-change-me';

function verifyJWT(token) {
  try {
    const [header, body, sig] = token.split('.');
    const expectedSig = crypto.createHmac('sha256', JWT_SECRET)
      .update(header + '.' + body).digest('base64url');
    if (sig !== expectedSig) return null;
    const payload = JSON.parse(Buffer.from(body, 'base64url').toString());
    if (payload.exp && payload.exp < Math.floor(Date.now() / 1000)) return null;
    return payload;
  } catch(e) { return null; }
}

function getUser(event) {
  const auth = (event.headers || {})['authorization'] || '';
  const token = auth.startsWith('Bearer ') ? auth.slice(7) : null;
  if (!token) return null;
  return verifyJWT(token);
}

const HOST      = process.env.DATABRICKS_HOST        || 'adb-1077037977847740.0.azuredatabricks.net';
const HTTP_PATH = process.env.DATABRICKS_HTTP_PATH   || '/sql/1.0/warehouses/1ad19be06aab1a65';
const TOKEN     = process.env.DATABRICKS_TOKEN;
const WAREHOUSE = process.env.DATABRICKS_WAREHOUSE_ID || HTTP_PATH.split('/').pop();

const cors = {
  'Content-Type': 'application/json',
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization'
};

function httpsPost(path, body) {
  return new Promise((resolve, reject) => {
    const buf = JSON.stringify(body);
    const req = https.request({
      hostname: HOST, path, method: 'POST',
      headers: { 'Authorization': `Bearer ${TOKEN}`, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(buf) }
    }, res => { let d=''; res.on('data',c=>d+=c); res.on('end',()=>{ try{resolve(JSON.parse(d));}catch(e){reject(new Error('Parse:'+d.slice(0,200)));} }); });
    req.on('error', reject); req.write(buf); req.end();
  });
}

function httpsGet(path) {
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname: HOST, path, method: 'GET',
      headers: { 'Authorization': `Bearer ${TOKEN}` }
    }, res => { let d=''; res.on('data',c=>d+=c); res.on('end',()=>{ try{resolve(JSON.parse(d));}catch(e){reject(new Error('Parse:'+d.slice(0,200)));} }); });
    req.on('error', reject); req.end();
  });
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function runQuery(sql) {
  const deadline = Date.now() + 22000;
  const submit = await httpsPost('/api/2.0/sql/statements', {
    statement: sql, warehouse_id: WAREHOUSE,
    wait_timeout: '0s', disposition: 'INLINE', format: 'JSON_ARRAY'
  });
  if (!submit.statement_id) throw new Error('No statement_id: ' + JSON.stringify(submit).slice(0,200));
  let result = submit;
  for (let i = 0; i < 40; i++) {
    const state = result.status?.state;
    if (state === 'SUCCEEDED') break;
    if (['FAILED','CANCELED','CLOSED'].includes(state))
      throw new Error(result.status?.error?.message || `Query ${state}`);
    if (Date.now() > deadline) throw new Error('WAREHOUSE_COLD: warehouse starting — retry in 30s');
    await sleep(state === 'PENDING' ? 5000 : 3000);
    result = await httpsGet(`/api/2.0/sql/statements/${submit.statement_id}`);
  }
  if (result.status?.state !== 'SUCCEEDED')
    throw new Error(`Timed out. Last state: ${result.status?.state}`);
  const cols = result.manifest?.schema?.columns?.map(c => c.name) || [];
  const rows = result.result?.data_array || [];
  return rows.map(row => { const o={}; cols.forEach((c,i)=>{ o[c]=row[i]??null; }); return o; });
}

const s = v => v != null && v !== '' ? `'${String(v).replace(/'/g,"''")}'` : 'NULL';
const n = v => v != null && v !== '' && !isNaN(v) ? parseFloat(v) : 'NULL';

async function syncAgenda(p) {
  // Normalise ic_date to YYYY-MM-DD regardless of timestamp format from Databricks
  const rawDate = p.ic_date ? String(p.ic_date).slice(0, 10) : null;
  if (!rawDate) return { skipped: true, reason: 'no ic_date' };

  const today = new Date().toISOString().slice(0, 10);
  if (rawDate < today) return { skipped: true, reason: 'ic_date is in the past' };

  const storeName = p.map_label || p.store_name || p.near_id;

  const existing = await runQuery(`
    SELECT agenda_id FROM kfc_ic_agenda
    WHERE near_id = ${s(p.near_id)} AND CAST(ic_date AS STRING) LIKE '${rawDate}%'
    LIMIT 1`);

  if (existing.length > 0) {
    const agendaId = existing[0].agenda_id;
    const icAskClause      = p.ic_ask      !== undefined ? `ic_ask      = ${s(p.ic_ask)},`      : '';
    const icCategoryClause = p.ic_category !== undefined ? `ic_category = ${s(p.ic_category)},` : '';
    await runQuery(`
      UPDATE kfc_ic_agenda SET
        store_name  = ${s(storeName)},
        franchisee  = ${s(p.franchisee  || null)},
        dev_manager = ${s(p.dev_manager || null)},
        facilityty  = ${s(p.facility || p.facilityty || null)},
        adm0_en     = ${s(p.adm0_en    || null)},
        adm1_en     = ${s(p.adm1_en    || null)},
        ${icAskClause}
        ${icCategoryClause}
        modified_at = CURRENT_TIMESTAMP()
      WHERE agenda_id = ${s(agendaId)}`);
    return { action: 'updated', agenda_id: agendaId };
  }

  const agendaId = `AGD-${p.near_id}-${rawDate.replace(/-/g,'')}`;
  await runQuery(`
    INSERT INTO kfc_ic_agenda
      (agenda_id, near_id, store_name, ic_date, franchisee, dev_manager,
       facilityty, adm0_en, adm1_en, ic_ask, ic_category, created_at, modified_at)
    VALUES (
      ${s(agendaId)}, ${s(p.near_id)}, ${s(storeName)}, TO_DATE(${s(rawDate)}),
      ${s(p.franchisee || null)}, ${s(p.dev_manager || null)},
      ${s(p.facility || p.facilityty || null)},
      ${s(p.adm0_en || null)}, ${s(p.adm1_en || null)},
      ${s(p.ic_ask || null)}, ${s(p.ic_category || null)},
      CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    )`);
  return { action: 'inserted', agenda_id: agendaId };
}

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: cors, body: '' };
  const user = getUser(event);
  if (!user) return { statusCode: 401, headers: cors, body: JSON.stringify({ error: 'Authentication required' }) };
  if (event.httpMethod !== 'POST')
    return { statusCode: 405, headers: cors, body: JSON.stringify({ error: 'Method not allowed' }) };
  if (!TOKEN)
    return { statusCode: 500, headers: cors, body: JSON.stringify({ error: 'DATABRICKS_TOKEN not set' }) };

  let p;
  try { p = JSON.parse(event.body); }
  catch(e) { return { statusCode: 400, headers: cors, body: JSON.stringify({ error: 'Invalid JSON' }) }; }
  if (!p.near_id)
    return { statusCode: 400, headers: cors, body: JSON.stringify({ error: 'near_id required' }) };

  // RLS: franchisees can only update their own sites, can't change franchisee field
  if (user.role === 'franchisee' && user.franchisee) {
    p.franchisee = user.franchisee;
  }

  try {
    await runQuery(`
      UPDATE kfc_assetuniverse_2025 SET
        status      = ${s(p.status)},
        franchisee  = ${s(p.franchisee)},
        facilityty  = ${s(p.facility)},
        ic_date     = ${p.ic_date ? `TO_DATE(${s(String(p.ic_date).slice(0,10))})` : 'NULL'},
        ic_ask      = ${s(p.ic_ask)},
        ic_category = ${s(p.ic_category)},
        dev_manager = ${s(p.dev_manager)},
        map_label   = ${s(p.map_label)},
        adm0_en     = ${s(p.adm0_en)},
        adm1_en     = ${s(p.adm1_en)},
        lat         = ${n(p.lat)},
        lon         = ${n(p.lon)},
        modifiedon  = CURRENT_DATE()
      WHERE near_id = ${s(p.near_id)}`);

    let agendaSync = { skipped: true };
    try { agendaSync = await syncAgenda(p); }
    catch(e) { agendaSync = { error: e.message }; }

    return { statusCode: 200, headers: cors, body: JSON.stringify({ success: true, near_id: p.near_id, agenda_sync: agendaSync }) };
  } catch(e) {
    return { statusCode: 500, headers: cors, body: JSON.stringify({ error: e.message }) };
  }
};
