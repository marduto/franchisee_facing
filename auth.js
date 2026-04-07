// agenda.js — Netlify Function
// GET: returns all IC agenda records from kfc_ic_agenda
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
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization'
};

function httpsPost(path, body) {
  return new Promise((resolve, reject) => {
    const buf = JSON.stringify(body);
    const req = https.request({
      hostname: HOST, path, method: 'POST',
      headers: { 'Authorization': `Bearer ${TOKEN}`, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(buf) }
    }, res => { let d=''; res.on('data',c=>d+=c); res.on('end',()=>{ try{resolve(JSON.parse(d));}catch(e){reject(new Error(d.slice(0,200)));} }); });
    req.on('error', reject); req.write(buf); req.end();
  });
}

function httpsGet(path) {
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname: HOST, path, method: 'GET',
      headers: { 'Authorization': `Bearer ${TOKEN}` }
    }, res => { let d=''; res.on('data',c=>d+=c); res.on('end',()=>{ try{resolve(JSON.parse(d));}catch(e){reject(new Error(d.slice(0,200)));} }); });
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
  if (!submit.statement_id) throw new Error('No statement_id: ' + JSON.stringify(submit).slice(0,300));
  let result = submit;
  for (let i = 0; i < 40; i++) {
    const state = result.status?.state;
    if (state === 'SUCCEEDED') break;
    if (['FAILED','CANCELED','CLOSED'].includes(state))
      throw new Error(result.status?.error?.message || `Query ${state}`);
    if (Date.now() > deadline)
      throw new Error('WAREHOUSE_COLD: Databricks warehouse is starting up — retry in 30 seconds');
    await sleep(state === 'PENDING' ? 5000 : 3000);
    result = await httpsGet(`/api/2.0/sql/statements/${submit.statement_id}`);
  }
  if (result.status?.state !== 'SUCCEEDED')
    throw new Error('Timed out: ' + result.status?.state);
  const cols = result.manifest?.schema?.columns?.map(c => c.name) || [];
  let rows = result.result?.data_array || [];
  let chunkIndex = result.result?.next_chunk_index;
  while (chunkIndex != null) {
    const chunk = await httpsGet(`/api/2.0/sql/statements/${submit.statement_id}/result/chunks/${chunkIndex}`);
    rows = rows.concat(chunk.data_array || []);
    chunkIndex = chunk.next_chunk_index;
  }
  return rows.map(row => { const o = {}; cols.forEach((col, i) => { o[col] = row[i] ?? null; }); return o; });
}

const SQL = `
  SELECT
    near_id, store_name, project_name,
    CAST(ic_date AS STRING) AS ic_date,
    ic_ask, ic_category, ic_decision, ic_minutes,
    franchisee, facilityty, dev_manager,
    lat, lon, status, seq,
    ic_background, ic_market_planning, ic_risk, ic_next_steps
  FROM kfc_ic_agenda
  ORDER BY ic_date DESC, seq ASC
`;

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 204, headers: cors, body: '' };

  const user = getUser(event);
  if (!user) return { statusCode: 401, headers: cors, body: JSON.stringify({ error: 'Authentication required' }) };

  if (!TOKEN)
    return { statusCode: 500, headers: cors, body: JSON.stringify({ error: 'DATABRICKS_TOKEN not set' }) };

  try {
    const records = await runQuery(SQL);
    let filtered = records;
    if (user.role === 'franchisee' && user.franchisee) {
      const fz = user.franchisee.toLowerCase();
      filtered = records.filter(r => (r.franchisee || '').toLowerCase() === fz);
    }
    return { statusCode: 200, headers: cors, body: JSON.stringify({ records: filtered, count: filtered.length }) };
  } catch(e) {
    const isCold = (e.message || '').includes('WAREHOUSE_COLD');
    return {
      statusCode: isCold ? 503 : 500,
      headers: cors,
      body: JSON.stringify({ error: e.message })
    };
  }
};
