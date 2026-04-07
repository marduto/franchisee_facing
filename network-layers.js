// doc-upload.js — Netlify Function
// POST: uploads a PDF to Azure Blob Storage (SAS auth) and registers metadata in kfc_site_documents
// Body (JSON): { near_id, store_name, franchisee, dev_manager, ic_date, doc_type, filename, file_base64, uploaded_by }
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

const ACCOUNT   = process.env.AZURE_STORAGE_ACCOUNT;
const SAS_TOKEN = process.env.AZURE_SAS_TOKEN;       // e.g. ?sv=2022-11-02&ss=b&srt=sco&...&sig=...
const CONTAINER = process.env.AZURE_CONTAINER || 'kfc-site-docs';

const DB_HOST      = process.env.DATABRICKS_HOST        || 'adb-1077037977847740.0.azuredatabricks.net';
const DB_HTTP_PATH = process.env.DATABRICKS_HTTP_PATH   || '/sql/1.0/warehouses/1ad19be06aab1a65';
const DB_TOKEN     = process.env.DATABRICKS_TOKEN;
const DB_WAREHOUSE = process.env.DATABRICKS_WAREHOUSE_ID || DB_HTTP_PATH.split('/').pop();

const cors = {
  'Content-Type': 'application/json',
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization'
};

// ── Azure Blob upload via SAS token ───────────────────────────────────────────
function uploadBlob(blobPath, fileBuffer, contentType) {
  return new Promise((resolve, reject) => {
    const sas  = SAS_TOKEN.startsWith('?') ? SAS_TOKEN : '?' + SAS_TOKEN;
    const host = `${ACCOUNT}.blob.core.windows.net`;
    const path = `/${CONTAINER}/${blobPath}${sas}`;
    const req  = https.request({
      hostname: host, path, method: 'PUT',
      headers: {
        'x-ms-blob-type':   'BlockBlob',
        'Content-Type':     contentType,
        'Content-Length':    fileBuffer.length
      }
    }, res => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => {
        if (res.statusCode >= 200 && res.statusCode < 300) resolve();
        else reject(new Error(`Azure upload failed ${res.statusCode}: ${d.slice(0,300)}`));
      });
    });
    req.on('error', reject);
    req.write(fileBuffer);
    req.end();
  });
}

// ── Databricks helpers ────────────────────────────────────────────────────────
function dbPost(path, body) {
  return new Promise((resolve, reject) => {
    const buf = JSON.stringify(body);
    const req = https.request({
      hostname: DB_HOST, path, method: 'POST',
      headers: { 'Authorization': `Bearer ${DB_TOKEN}`, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(buf) }
    }, res => { let d=''; res.on('data',c=>d+=c); res.on('end',()=>{ try{resolve(JSON.parse(d));}catch(e){reject(new Error(d.slice(0,200)));} }); });
    req.on('error', reject); req.write(buf); req.end();
  });
}

function dbGet(path) {
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname: DB_HOST, path, method: 'GET',
      headers: { 'Authorization': `Bearer ${DB_TOKEN}` }
    }, res => { let d=''; res.on('data',c=>d+=c); res.on('end',()=>{ try{resolve(JSON.parse(d));}catch(e){reject(new Error(d.slice(0,200)));} }); });
    req.on('error', reject); req.end();
  });
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function runQuery(sql) {
  const submit = await dbPost('/api/2.0/sql/statements', {
    statement: sql, warehouse_id: DB_WAREHOUSE,
    wait_timeout: '0s', disposition: 'INLINE', format: 'JSON_ARRAY'
  });
  if (!submit.statement_id) throw new Error('No statement_id: ' + JSON.stringify(submit).slice(0,200));
  let result = submit;
  for (let i = 0; i < 40; i++) {
    const state = result.status?.state;
    if (state === 'SUCCEEDED') break;
    if (['FAILED','CANCELED','CLOSED'].includes(state))
      throw new Error(result.status?.error?.message || `Query ${state}`);
    await sleep(state === 'PENDING' ? 5000 : 3000);
    result = await dbGet(`/api/2.0/sql/statements/${submit.statement_id}`);
  }
  if (result.status?.state !== 'SUCCEEDED')
    throw new Error('Timed out: ' + result.status?.state);
  return result;
}

const s = v => v != null && v !== '' ? `'${String(v).replace(/'/g,"''")}'` : 'NULL';

// ── Handler ───────────────────────────────────────────────────────────────────
exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: cors, body: '' };

  const user = getUser(event);
  if (!user) return { statusCode: 401, headers: cors, body: JSON.stringify({ error: 'Authentication required' }) };

  if (event.httpMethod !== 'POST')
    return { statusCode: 405, headers: cors, body: JSON.stringify({ error: 'Method not allowed' }) };

  if (!ACCOUNT || !SAS_TOKEN)
    return { statusCode: 500, headers: cors, body: JSON.stringify({ error: 'Azure env vars not configured (AZURE_STORAGE_ACCOUNT, AZURE_SAS_TOKEN)' }) };

  if (!DB_TOKEN)
    return { statusCode: 500, headers: cors, body: JSON.stringify({ error: 'DATABRICKS_TOKEN not set' }) };

  let p;
  try { p = JSON.parse(event.body); }
  catch(e) { return { statusCode: 400, headers: cors, body: JSON.stringify({ error: 'Invalid JSON' }) }; }

  const { near_id, store_name, franchisee, dev_manager, ic_date, doc_type, filename, file_base64, uploaded_by } = p;

  if (!near_id)      return { statusCode: 400, headers: cors, body: JSON.stringify({ error: 'near_id required' }) };
  if (!doc_type)     return { statusCode: 400, headers: cors, body: JSON.stringify({ error: 'doc_type required (SDP or LOI)' }) };
  if (!file_base64)  return { statusCode: 400, headers: cors, body: JSON.stringify({ error: 'file_base64 required' }) };

  try {
    // Build blob path: near_id/DOC_TYPE_timestamp_filename
    const ts        = Date.now();
    const safeName  = (filename || 'document.pdf').replace(/[^a-zA-Z0-9._-]/g, '_');
    const blobPath  = `${near_id}/${doc_type}_${ts}_${safeName}`;
    const blobUrl   = `https://${ACCOUNT}.blob.core.windows.net/${CONTAINER}/${blobPath}`;
    const fileBuffer= Buffer.from(file_base64, 'base64');
    const docId     = `DOC-${near_id}-${ts}`;
    const icDateStr = ic_date ? String(ic_date).slice(0,10) : null;

    // 1. Upload to Azure Blob
    await uploadBlob(blobPath, fileBuffer, 'application/pdf');

    // 2. Register metadata in Databricks
    await runQuery(`
      INSERT INTO hive_metastore.default.kfc_site_documents (
        doc_id, near_id, store_name, franchisee, dev_manager,
        ic_date, doc_type, blob_url, filename, uploaded_by, uploaded_at
      ) VALUES (
        ${s(docId)}, ${s(near_id)}, ${s(store_name||null)}, ${s(franchisee||null)},
        ${s(dev_manager||null)}, ${icDateStr ? `TO_DATE(${s(icDateStr)})` : 'NULL'},
        ${s(doc_type)}, ${s(blobUrl)}, ${s(safeName)},
        ${s(uploaded_by||'web-app')}, CURRENT_TIMESTAMP()
      )`);

    return { statusCode: 200, headers: cors, body: JSON.stringify({ success: true, doc_id: docId, blob_url: blobUrl, filename: safeName }) };

  } catch(e) {
    console.error('doc-upload error:', e);
    return { statusCode: 500, headers: cors, body: JSON.stringify({ error: e.message }) };
  }
};
