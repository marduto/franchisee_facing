// site-add.js — Netlify Function
// POST: INSERTs a new site into kfc_assetuniverse_2025
//       and optionally creates a kfc_ic_agenda record if ic_date is provided
// Returns: { near_id, agenda_id? }
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

// Generate a near_id in the format A-XXXX (find current max and increment)
async function generateNearId() {
  const rows = await runQuery(`
    SELECT near_id FROM kfc_assetuniverse_2025
    WHERE near_id RLIKE '^A-[0-9]+$'
    ORDER BY CAST(SUBSTRING(near_id, 3) AS INT) DESC
    LIMIT 1`);
  if (rows.length > 0) {
    const last = parseInt(rows[0].near_id.replace('A-', ''));
    return `A-${last + 1}`;
  }
  return 'A-5000'; // fallback starting point
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

  // RLS: franchisees can only add sites under their own franchisee
  if (user.role === 'franchisee' && user.franchisee) {
    p.franchisee = user.franchisee;
  }

  if (!p.store_name)
    return { statusCode: 400, headers: cors, body: JSON.stringify({ error: 'store_name required' }) };
  if (!p.lat || !p.lon)
    return { statusCode: 400, headers: cors, body: JSON.stringify({ error: 'lat and lon required' }) };

  try {
    // Generate new near_id
    const nearId  = await generateNearId();
    const icDate  = p.ic_date ? String(p.ic_date).slice(0, 10) : null;

    // INSERT into kfc_assetuniverse_2025
    await runQuery(`
      INSERT INTO kfc_assetuniverse_2025 (
        near_id, store_name, map_label, lat, lon,
        status, franchisee, facilityty, dev_manager,
        adm0_en, adm1_en, bem_address,
        ic_date, ic_ask, ic_category,
        store_type, modifiedon
      ) VALUES (
        ${s(nearId)}, ${s(p.store_name)}, ${s(p.store_name)},
        ${n(p.lat)}, ${n(p.lon)},
        ${s(p.status || 'Site Identified')},
        ${s(p.franchisee || null)},
        ${s(p.facilityty || null)},
        ${s(p.dev_manager || null)},
        ${s(p.adm0_en || null)},
        ${s(p.adm1_en || null)},
        ${s(p.bem_address || null)},
        ${icDate ? `TO_DATE(${s(icDate)})` : 'NULL'},
        ${s(p.ic_ask || null)},
        ${s(p.ic_category || null)},
        'KFC_FUTURE',
        CURRENT_DATE()
      )`);

    // Optionally create agenda record if ic_date provided
    let agendaId = null;
    if (icDate) {
      agendaId = `AGD-${nearId}-${icDate.replace(/-/g,'')}`;
      try {
        await runQuery(`
          INSERT INTO kfc_ic_agenda (
            agenda_id, near_id, store_name, ic_date,
            franchisee, dev_manager, facilityty,
            adm0_en, adm1_en, ic_ask, ic_category,
            created_at, modified_at
          ) VALUES (
            ${s(agendaId)}, ${s(nearId)}, ${s(p.store_name)},
            TO_DATE(${s(icDate)}),
            ${s(p.franchisee || null)}, ${s(p.dev_manager || null)},
            ${s(p.facilityty || null)},
            ${s(p.adm0_en || null)}, ${s(p.adm1_en || null)},
            ${s(p.ic_ask || null)}, ${s(p.ic_category || null)},
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
          )`);
      } catch(e) {
        // Agenda insert failure is non-fatal — site was already created
        console.warn('Agenda insert failed:', e.message);
        agendaId = null;
      }
    }

    return {
      statusCode: 200, headers: cors,
      body: JSON.stringify({ success: true, near_id: nearId, agenda_id: agendaId })
    };

  } catch(e) {
    console.error('site-add error:', e);
    return { statusCode: 500, headers: cors, body: JSON.stringify({ error: e.message }) };
  }
};
