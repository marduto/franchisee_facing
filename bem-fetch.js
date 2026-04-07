// auth.js — Netlify Function
// POST /auth  body: { action: 'login'|'signup'|'verify', email, password, token }
// Backed by franchisee_login table in Databricks
const https = require('https');
const crypto = require('crypto');

const HOST      = process.env.DATABRICKS_HOST        || 'adb-1077037977847740.0.azuredatabricks.net';
const HTTP_PATH = process.env.DATABRICKS_HTTP_PATH   || '/sql/1.0/warehouses/1ad19be06aab1a65';
const TOKEN     = process.env.DATABRICKS_TOKEN;
const WAREHOUSE = process.env.DATABRICKS_WAREHOUSE_ID || HTTP_PATH.split('/').pop();
const JWT_SECRET = process.env.JWT_SECRET || 'kfc-franchisee-app-secret-change-me';

const cors = {
  'Content-Type': 'application/json',
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, x-app-secret'
};

// ── Databricks helpers ────────────────────────────────────────────
function dbPost(path, body) {
  return new Promise((resolve, reject) => {
    const buf = JSON.stringify(body);
    const req = https.request({
      hostname: HOST, path, method: 'POST',
      headers: { 'Authorization': `Bearer ${TOKEN}`, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(buf) }
    }, res => { let d=''; res.on('data',c=>d+=c); res.on('end',()=>{ try{resolve(JSON.parse(d));}catch(e){reject(new Error(d.slice(0,200)));} }); });
    req.on('error', reject); req.write(buf); req.end();
  });
}

function dbGet(path) {
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
  const submit = await dbPost('/api/2.0/sql/statements', {
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
    await sleep(state === 'PENDING' ? 5000 : 3000);
    result = await dbGet(`/api/2.0/sql/statements/${submit.statement_id}`);
  }
  if (result.status?.state !== 'SUCCEEDED') throw new Error('Timed out');
  const cols = result.manifest?.schema?.columns?.map(c => c.name) || [];
  const rows = result.result?.data_array || [];
  return rows.map(row => { const o={}; cols.forEach((c,i)=>{ o[c]=row[i]??null; }); return o; });
}

// ── Password hashing ──────────────────────────────────────────────
function hashPassword(password, salt) {
  if (!salt) salt = crypto.randomBytes(16).toString('hex');
  const hash = crypto.pbkdf2Sync(password, salt, 10000, 64, 'sha512').toString('hex');
  return { salt, hash: salt + ':' + hash };
}

function verifyPassword(password, storedHash) {
  const [salt, hash] = storedHash.split(':');
  const test = crypto.pbkdf2Sync(password, salt, 10000, 64, 'sha512').toString('hex');
  return hash === test;
}

// ── JWT ───────────────────────────────────────────────────────────
function createJWT(payload, expiresInHours) {
  const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64url');
  const now = Math.floor(Date.now() / 1000);
  const body = Buffer.from(JSON.stringify({
    ...payload,
    iat: now,
    exp: now + (expiresInHours || 24) * 3600
  })).toString('base64url');
  const sig = crypto.createHmac('sha256', JWT_SECRET)
    .update(header + '.' + body).digest('base64url');
  return header + '.' + body + '.' + sig;
}

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

const s = v => v != null && v !== '' ? `'${String(v).replace(/'/g,"''")}'` : 'NULL';

// ── Handler ───────────────────────────────────────────────────────
exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 204, headers: cors, body: '' };
  if (event.httpMethod !== 'POST')
    return { statusCode: 405, headers: cors, body: JSON.stringify({ error: 'POST only' }) };
  if (!TOKEN)
    return { statusCode: 500, headers: cors, body: JSON.stringify({ error: 'DATABRICKS_TOKEN not set' }) };

  let p;
  try { p = JSON.parse(event.body); } catch(e) {
    return { statusCode: 400, headers: cors, body: JSON.stringify({ error: 'Invalid JSON' }) };
  }

  const { action, email, password, token } = p;

  try {
    // ── VERIFY TOKEN ──────────────────────────────────────────────
    if (action === 'verify') {
      if (!token) return { statusCode: 401, headers: cors, body: JSON.stringify({ error: 'No token' }) };
      const payload = verifyJWT(token);
      if (!payload) return { statusCode: 401, headers: cors, body: JSON.stringify({ error: 'Invalid or expired token' }) };
      return { statusCode: 200, headers: cors, body: JSON.stringify({ valid: true, user: payload }) };
    }

    if (!email || !password)
      return { statusCode: 400, headers: cors, body: JSON.stringify({ error: 'Email and password required' }) };

    const emailLower = email.trim().toLowerCase();

    // Look up user
    const users = await runQuery(
      `SELECT email, franchisee, role, name, password_hash, active
       FROM hive_metastore.default.franchisee_login
       WHERE LOWER(email) = ${s(emailLower)}`
    );

    if (!users.length)
      return { statusCode: 401, headers: cors, body: JSON.stringify({ error: 'Email not authorized. Contact your administrator.' }) };

    const user = users[0];

    if (user.active === 'false' || user.active === false)
      return { statusCode: 401, headers: cors, body: JSON.stringify({ error: 'Account deactivated. Contact your administrator.' }) };

    // ── SIGNUP (set password for first time) ─────────────────────
    if (action === 'signup') {
      if (user.password_hash)
        return { statusCode: 400, headers: cors, body: JSON.stringify({ error: 'Password already set. Use login instead.' }) };

      if (password.length < 8)
        return { statusCode: 400, headers: cors, body: JSON.stringify({ error: 'Password must be at least 8 characters.' }) };

      const { hash } = hashPassword(password);

      await runQuery(
        `UPDATE hive_metastore.default.franchisee_login
         SET password_hash = ${s(hash)}, last_login = CURRENT_TIMESTAMP()
         WHERE LOWER(email) = ${s(emailLower)}`
      );

      const jwt = createJWT({
        email: user.email,
        franchisee: user.franchisee,
        role: user.role || 'franchisee',
        name: user.name
      }, 72);

      return { statusCode: 200, headers: cors, body: JSON.stringify({
        success: true,
        token: jwt,
        user: { email: user.email, franchisee: user.franchisee, role: user.role, name: user.name }
      })};
    }

    // ── LOGIN ─────────────────────────────────────────────────────
    if (action === 'login') {
      if (!user.password_hash)
        return { statusCode: 400, headers: cors, body: JSON.stringify({ error: 'Password not set yet. Please sign up first.' }) };

      if (!verifyPassword(password, user.password_hash))
        return { statusCode: 401, headers: cors, body: JSON.stringify({ error: 'Incorrect password.' }) };

      // Update last_login
      runQuery(
        `UPDATE hive_metastore.default.franchisee_login
         SET last_login = CURRENT_TIMESTAMP()
         WHERE LOWER(email) = ${s(emailLower)}`
      ).catch(() => {}); // fire and forget

      const jwt = createJWT({
        email: user.email,
        franchisee: user.franchisee,
        role: user.role || 'franchisee',
        name: user.name
      }, 72);

      return { statusCode: 200, headers: cors, body: JSON.stringify({
        success: true,
        token: jwt,
        user: { email: user.email, franchisee: user.franchisee, role: user.role, name: user.name }
      })};
    }

    return { statusCode: 400, headers: cors, body: JSON.stringify({ error: 'Invalid action. Use login, signup, or verify.' }) };

  } catch(e) {
    console.error('auth error:', e);
    return { statusCode: 500, headers: cors, body: JSON.stringify({ error: e.message }) };
  }
};
