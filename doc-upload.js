// doc-list.js — Netlify Function
// GET /?near_id=xxx — returns all documents for a site from kfc_site_documents
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

const SAS_TOKEN = process.env.AZURE_SAS_TOKEN || '';
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
  const submit = await httpsPost('/api/2.0/sql/statements', {
    statement: sql, warehouse_id: WAREHOUSE,
    wait_timeout: '0s', disposition: 'INLINE', format: 'JSON_ARRAY'
  });
  if (!submit.statement_id) throw new Error('No statement_id');
  let result = submit;
  for (let i = 0; i < 40; i++) {
    const state = result.status?.state;
    if (state === 'SUCCEEDED') break;
    if (['FAILED','CANCELED','CLOSED'].includes(state))
      throw new Error(result.status?.error?.message || `Query ${state}`);
    await sleep(state === 'PENDING' ? 5000 : 3000);
    result = await httpsGet(`/api/2.0/sql/statements/${submit.statement_id}`);
  }
  if (result.status?.state !== 'SUCCEEDED') throw new Error('Timed out');
  const cols = result.manifest?.schema?.columns?.map(c => c.name) || [];
  const rows = result.result?.data_array || [];
  return rows.map(row => { const o={}; cols.forEach((c,i)=>{ o[c]=row[i]??null; }); return o; });
}

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: cors, body: '' };

  const user = getUser(event);
  if (!user) return { statusCode: 401, headers: cors, body: JSON.stringify({ error: 'Authentication required' }) };

  if (!TOKEN) return { statusCode: 500, headers: cors, body: JSON.stringify({ error: 'DATABRICKS_TOKEN not set' }) };

  const near_id = (event.queryStringParameters || {}).near_id;
  if (!near_id) return { statusCode: 400, headers: cors, body: JSON.stringify({ error: 'near_id required' }) };

  const s = v => v != null && v !== '' ? `'${String(v).replace(/'/g,"''")}'` : 'NULL';

  try {
    const docs = await runQuery(`
      SELECT doc_id, near_id, store_name, franchisee, dev_manager,
             CAST(ic_date AS STRING) AS ic_date,
             doc_type, blob_url, filename, uploaded_by,
             CAST(uploaded_at AS STRING) AS uploaded_at
      FROM hive_metastore.default.kfc_site_documents
      WHERE near_id = ${s(near_id)}
      ORDER BY uploaded_at DESC`);
    // Append SAS token to blob URLs so private blobs are accessible
    const sas = SAS_TOKEN ? (SAS_TOKEN.startsWith('?') ? SAS_TOKEN : '?' + SAS_TOKEN) : '';
    if (sas) docs.forEach(d => { if (d.blob_url) d.blob_url = d.blob_url + sas; });
    return { statusCode: 200, headers: cors, body: JSON.stringify({ docs, count: docs.length }) };
  } catch(e) {
    return { statusCode: 500, headers: cors, body: JSON.stringify({ error: e.message }) };
  }
};
