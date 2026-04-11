// bem-fetch.js — Netlify Function
// GET /?near_id=A-1234 — returns the latest BEM record for a site from kfc_bem_results
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

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: cors, body: '' };
  const user = getUser(event);
  if (!user) return { statusCode: 401, headers: cors, body: JSON.stringify({ error: 'Authentication required' }) };
  if (!TOKEN)
    return { statusCode: 500, headers: cors, body: JSON.stringify({ error: 'DATABRICKS_TOKEN not set' }) };

  const near_id = (event.queryStringParameters || {}).near_id;
  if (!near_id)
    return { statusCode: 400, headers: cors, body: JSON.stringify({ error: 'near_id required' }) };

  const COLS = `
        bem_wpra_weekly, bem_weekly_txn, bem_avg_ticket, bem_annual_revenue,
        bem_capex,
        bem_cap_land, bem_cap_building, bem_cap_fitout, bem_cap_shopfit,
        bem_cap_kitchen, bem_cap_hvac, bem_cap_electronics, bem_cap_proffees,
        bem_cap_kiosks, bem_cap_generator, bem_cap_solar, bem_cap_water,
        bem_cap_other, bem_cap_contribution, bem_cap_initialfee,
        bem_cap_minor_remodel, bem_cap_major_remodel,
        bem_depr_yr1, bem_nopat_yr1,
        bem_cos_pct, bem_labour_pct, bem_store_cost_pct, bem_rental_pct,
        bem_royalties_pct, bem_advertising_pct, bem_delcomm_pct,
        bem_taxrate_pct, bem_txn_growth_pct, bem_ticket_growth_pct,
        bem_inflation_pct, bem_rental_esc_pct,
        bem_wacc_pct, bem_hurdle_pct,
        bem_drivethru_pct, bem_delivery_pct, bem_instore_pct,
        bem_ebitda, bem_ebitda_margin, bem_npv AS bem_npv_5yr,
        bem_irr, bem_payback_yrs, bem_verdict,
        CAST(ic_date AS STRING) AS ic_date`;

  try {
    // Direct query — no schema inspection needed, table is stable
    let rows = await runQuery(`
      SELECT ${COLS}
      FROM hive_metastore.default.kfc_bem_results
      WHERE near_id = ${s(near_id)}
      ORDER BY modified_at DESC
      LIMIT 1`);

    // Fallback: match by store_name
    if (rows.length === 0) {
      rows = await runQuery(`
        SELECT ${COLS}
        FROM hive_metastore.default.kfc_bem_results
        WHERE store_name = (
          SELECT store_name FROM kfc_assetuniverse_2025
          WHERE near_id = ${s(near_id)} LIMIT 1
        )
        ORDER BY modified_at DESC
        LIMIT 1`);
    }

    const bem = rows.length > 0 ? rows[0] : null;
    return { statusCode: 200, headers: cors, body: JSON.stringify({ bem, found: bem !== null, near_id }) };
  } catch(e) {
    return { statusCode: 500, headers: cors, body: JSON.stringify({ error: e.message }) };
  }
};
