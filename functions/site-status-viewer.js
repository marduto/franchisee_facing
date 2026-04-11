// /.netlify/functions/site-status-viewer.js
// Serves kfc_sitestatusviewer with optional franchisee RLS filter.
//
// ENV vars required (same as sites.js):
//   DATABRICKS_HOST       e.g. https://adb-XXXXXXXX.azuredatabricks.net
//   DATABRICKS_TOKEN      Personal Access Token
//   DATABRICKS_WAREHOUSE_ID  SQL Warehouse ID  (falls back to HTTP_PATH tail)
//   DATABRICKS_HTTP_PATH     e.g. /sql/1.0/warehouses/<id>
//   APP_SECRET               Shared secret checked against x-app-secret header

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

const REQUIRED_ENV = ['DATABRICKS_HOST', 'DATABRICKS_TOKEN'];

exports.handler = async (event) => {
  const CORS = {
    'Access-Control-Allow-Origin':  '*',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers: CORS, body: '' };
  }

  // ── Auth check ────────────────────────────────────────────────────────────
  const user = getUser(event);
  if (!user) {
    return {
      statusCode: 401,
      headers: { ...CORS, 'Content-Type': 'application/json' },
      body: JSON.stringify({ error: 'Authentication required' }),
    };
  }

  // ── Env check ─────────────────────────────────────────────────────────────
  for (const v of REQUIRED_ENV) {
    if (!process.env[v]) {
      return {
        statusCode: 500,
        headers: { ...CORS, 'Content-Type': 'application/json' },
        body: JSON.stringify({ error: `Missing env var: ${v}` }),
      };
    }
  }

  const RAW_HOST   = process.env.DATABRICKS_HOST.replace(/\/$/, '');
  const HOST       = RAW_HOST.startsWith('http') ? RAW_HOST : `https://${RAW_HOST}`;
  const TOKEN      = process.env.DATABRICKS_TOKEN;
  const HTTP_PATH  = process.env.DATABRICKS_HTTP_PATH || '/sql/1.0/warehouses/1ad19be06aab1a65';
  const WAREHOUSE  = process.env.DATABRICKS_WAREHOUSE_ID || process.env.DATABRICKS_WAREHOUSE || HTTP_PATH.split('/').pop();

  // ── RLS: use franchisee from JWT, not query param ─────────────────────────
  const params      = event.queryStringParameters || {};
  const proposed_id = (params.proposed_id || '').trim().replace(/'/g, "''");

  // Franchisee filter — apply for any logged-in user that has a franchisee value
  // unless they are explicitly an admin. (user.franchisee is sourced from
  // franchisee_login.franchisee, which mirrors kfc_assetuniverse_2025.franchisee.)
  const role       = (user.role || '').toString().toLowerCase().trim();
  const isAdmin    = role === 'admin';
  const franchisee = (!isAdmin && user.franchisee)
    ? String(user.franchisee).replace(/'/g, "''")
    : '';

  console.log('[site-status-viewer] auth:', JSON.stringify({
    email: user.email || null,
    role: user.role || null,
    franchisee_jwt: user.franchisee || null,
    is_admin: isAdmin,
    filter_will_apply: !!franchisee,
  }));

  // ── Build SQL ─────────────────────────────────────────────────────────────
  // The view's franchisee column is open_fz (per the PySpark .select that
  // builds default.kfc_sitestatusviewer).
  let whereClause = 'WHERE 1=1';
  if (franchisee)  whereClause += ` AND UPPER(TRIM(open_fz)) = UPPER(TRIM('${franchisee}'))`;
  if (proposed_id) whereClause += ` AND proposed_id = '${proposed_id}'`;

  const SQL = `
    SELECT *
    FROM default.kfc_sitestatusviewer
    ${whereClause}
    ORDER BY proposed_ic_date DESC, distance ASC
    LIMIT 500
  `;

  // ── Execute via Databricks SQL Statements API ─────────────────────────────
  try {
    const execRes = await fetch(
      `${HOST}/api/2.0/sql/statements`,
      {
        method:  'POST',
        headers: {
          'Authorization':  `Bearer ${TOKEN}`,
          'Content-Type':   'application/json',
        },
        body: JSON.stringify({
          warehouse_id:  WAREHOUSE,
          statement:     SQL,
          wait_timeout:  '30s',
          on_wait_timeout: 'CANCEL',
        }),
      }
    );

    const json = await execRes.json();


    // Cold warehouse signal
    if (json.error_code === 'TEMPORARILY_UNAVAILABLE' ||
        (json.message || '').toLowerCase().includes('starting')) {
      return {
        statusCode: 503,
        headers: { ...CORS, 'Content-Type': 'application/json' },
        body: JSON.stringify({ error: 'WAREHOUSE_COLD' }),
      };
    }

    if (!execRes.ok || json.status?.state === 'FAILED') {
      const msg = json.message || json.error_code || 'Query failed';
      return {
        statusCode: 500,
        headers: { ...CORS, 'Content-Type': 'application/json' },
        body: JSON.stringify({ error: msg }),
      };
    }

    // ── Map column names to rows ───────────────────────────────────────────
    const cols = (json.manifest?.schema?.columns || []).map(c => c.name);
    const rows = (json.result?.data_array || []).map(row => {
      const obj = {};
      cols.forEach((col, i) => { obj[col] = row[i] ?? null; });
      return obj;
    });

    console.log('[site-status-viewer] result:', JSON.stringify({
      filter_column: franchisee ? 'open_fz' : null,
      filter_value: franchisee || null,
      rows_returned: rows.length,
      view_columns: cols,
    }));

    return {
      statusCode: 200,
      headers: { ...CORS, 'Content-Type': 'application/json' },
      body: JSON.stringify({ rows, count: rows.length }),
    };

  } catch (err) {
    return {
      statusCode: 500,
      headers: { ...CORS, 'Content-Type': 'application/json' },
      body: JSON.stringify({ error: err.message || 'Internal error' }),
    };
  }
};
