// bem-save.js — Netlify Function
// POST: writes BEM results to kfc_bem_results, keyed on (near_id, ic_date)
// GET:  diagnostics — checks table exists and env vars
//
// DDL (run once in Databricks):
// CREATE TABLE IF NOT EXISTS hive_metastore.default.kfc_bem_results (
//   bem_id STRING, near_id STRING, ic_date DATE,
//   store_name STRING, franchisee STRING, facilityty STRING, adm1_en STRING,
//   lat DOUBLE, lon DOUBLE,
//   bem_wpra_weekly DOUBLE, bem_weekly_txn DOUBLE, bem_avg_ticket DOUBLE,
//   bem_annual_revenue DOUBLE, bem_capex DOUBLE,
//   bem_cos_pct DOUBLE, bem_labour_pct DOUBLE, bem_store_cost_pct DOUBLE,
//   bem_rental_pct DOUBLE, bem_royalties_pct DOUBLE, bem_advertising_pct DOUBLE,
//   bem_delcomm_pct DOUBLE, bem_taxrate_pct DOUBLE,
//   bem_txn_growth_pct DOUBLE, bem_ticket_growth_pct DOUBLE,
//   bem_inflation_pct DOUBLE, bem_rental_esc_pct DOUBLE,
//   bem_wacc_pct DOUBLE, bem_hurdle_pct DOUBLE,
//   bem_drivethru_pct DOUBLE, bem_delivery_pct DOUBLE, bem_instore_pct DOUBLE,
//   bem_ebitda DOUBLE, bem_ebitda_margin DOUBLE, bem_npv DOUBLE,
//   bem_irr DOUBLE, bem_payback_yrs DOUBLE,
//   bem_verdict STRING, bem_submitted_by STRING,
//   bem_submitted_at TIMESTAMP, created_at TIMESTAMP, modified_at TIMESTAMP
// ) USING DELTA;
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
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
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

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: cors, body: '' };

  const user = getUser(event);
  if (!user) return { statusCode: 401, headers: cors, body: JSON.stringify({ error: 'Authentication required' }) };
  // GET — diagnostics
  if (event.httpMethod === 'GET') {
    if (!TOKEN) return { statusCode: 200, headers: cors, body: JSON.stringify({ ok: false, error: 'DATABRICKS_TOKEN not set' }) };
    try {
      const rows = await runQuery(`SHOW TABLES IN hive_metastore.default LIKE 'kfc_bem_results'`);
      return { statusCode: 200, headers: cors, body: JSON.stringify({ ok: true, table_exists: rows.length > 0, warehouse: WAREHOUSE }) };
    } catch(e) {
      return { statusCode: 200, headers: cors, body: JSON.stringify({ ok: false, error: e.message }) };
    }
  }

  if (event.httpMethod !== 'POST')
    return { statusCode: 405, headers: cors, body: JSON.stringify({ error: 'Method not allowed' }) };
  if (!TOKEN)
    return { statusCode: 500, headers: cors, body: JSON.stringify({ error: 'DATABRICKS_TOKEN not set' }) };

  let p;
  try { p = JSON.parse(event.body); }
  catch(e) { return { statusCode: 400, headers: cors, body: JSON.stringify({ error: 'Invalid JSON: ' + e.message }) }; }

  // RLS: franchisees can only save BEM for their own sites
  if (user.role === 'franchisee' && user.franchisee) {
    p.franchisee = user.franchisee;
  }

  if (!p.near_id && !p.store_name)
    return { statusCode: 400, headers: cors, body: JSON.stringify({ error: 'near_id or store_name required', received: Object.keys(p) }) };
  // ic_date optional — default to today if not provided
  if (!p.ic_date) p.ic_date = new Date().toISOString().slice(0, 10);

  try {
    const nearId    = p.near_id || 'NEW';
    const icDate    = String(p.ic_date).slice(0, 10);
    const today     = new Date().toISOString().slice(0, 10);
    const icPassed  = icDate < today; // IC date is in the past

    // If IC date has passed, generate a versioned ID so we INSERT a new record
    // rather than overwriting. Format: BEM-A-1234-20260331-v2, v3 etc.
    const baseId    = `BEM-${nearId}-${icDate.replace(/-/g,'')}`;
    const storeName = p.store_name || p.site_name || nearId;

    // Find all existing records for this near_id + ic_date to determine version
    const existing = await runQuery(`
      SELECT bem_id FROM hive_metastore.default.kfc_bem_results
      WHERE near_id = ${s(nearId)} AND CAST(ic_date AS STRING) LIKE '${icDate}%'
      ORDER BY created_at ASC`);

    // Overwrite if IC date not yet passed, else create new versioned record
    const shouldOverwrite = existing.length > 0 && !icPassed;
    const version = existing.length + 1;
    const bemId   = shouldOverwrite ? existing[0].bem_id : (existing.length === 0 ? baseId : `${baseId}-v${version}`);

    if (shouldOverwrite) {
      await runQuery(`
        UPDATE hive_metastore.default.kfc_bem_results SET
          store_name            = ${s(storeName)},
          franchisee            = ${s(p.franchisee || null)},
          facilityty            = ${s(p.facilityty || p.facility || null)},
          adm1_en               = ${s(p.adm1_en || null)},
          lat                   = ${n(p.lat)},
          lon                   = ${n(p.lon)},
          bem_wpra_weekly       = ${n(p.bem_wpra_weekly)},
          bem_weekly_txn        = ${n(p.bem_weekly_txn)},
          bem_avg_ticket        = ${n(p.bem_avg_ticket)},
          bem_annual_revenue    = ${n(p.bem_annual_revenue)},
          bem_capex             = ${n(p.bem_capex)},
          bem_cap_land          = ${n(p.bem_cap_land)},
          bem_cap_building      = ${n(p.bem_cap_building)},
          bem_cap_fitout        = ${n(p.bem_cap_fitout)},
          bem_cap_shopfit       = ${n(p.bem_cap_shopfit)},
          bem_cap_kitchen       = ${n(p.bem_cap_kitchen)},
          bem_cap_hvac          = ${n(p.bem_cap_hvac)},
          bem_cap_electronics   = ${n(p.bem_cap_electronics)},
          bem_cap_proffees      = ${n(p.bem_cap_proffees)},
          bem_cap_kiosks        = ${n(p.bem_cap_kiosks)},
          bem_cap_generator     = ${n(p.bem_cap_generator)},
          bem_cap_solar         = ${n(p.bem_cap_solar)},
          bem_cap_water         = ${n(p.bem_cap_water)},
          bem_cap_other         = ${n(p.bem_cap_other)},
          bem_cap_contribution  = ${n(p.bem_cap_contribution)},
          bem_cap_initialfee    = ${n(p.bem_cap_initialfee)},
          bem_cap_minor_remodel = ${n(p.bem_cap_minor_remodel)},
          bem_cap_major_remodel = ${n(p.bem_cap_major_remodel)},
          bem_depr_yr1          = ${n(p.bem_depr_yr1)},
          bem_nopat_yr1         = ${n(p.bem_nopat_yr1)},
          bem_cos_pct           = ${n(p.bem_cos_pct)},
          bem_labour_pct        = ${n(p.bem_labour_pct)},
          bem_store_cost_pct    = ${n(p.bem_store_cost_pct)},
          bem_rental_pct        = ${n(p.bem_rental_pct)},
          bem_royalties_pct     = ${n(p.bem_royalties_pct)},
          bem_advertising_pct   = ${n(p.bem_advertising_pct)},
          bem_delcomm_pct       = ${n(p.bem_delcomm_pct)},
          bem_taxrate_pct       = ${n(p.bem_taxrate_pct)},
          bem_txn_growth_pct    = ${n(p.bem_txn_growth_pct)},
          bem_ticket_growth_pct = ${n(p.bem_ticket_growth_pct)},
          bem_inflation_pct     = ${n(p.bem_inflation_pct)},
          bem_rental_esc_pct    = ${n(p.bem_rental_esc_pct)},
          bem_wacc_pct          = ${n(p.bem_wacc_pct)},
          bem_hurdle_pct        = ${n(p.bem_hurdle_pct)},
          bem_drivethru_pct     = ${n(p.bem_drivethru_pct)},
          bem_delivery_pct      = ${n(p.bem_delivery_pct)},
          bem_instore_pct       = ${n(p.bem_instore_pct)},
          bem_ebitda            = ${n(p.bem_ebitda)},
          bem_ebitda_margin     = ${n(p.bem_ebitda_margin)},
          bem_npv               = ${n(p.bem_npv_5yr)},
          bem_irr               = ${n(p.bem_irr)},
          bem_payback_yrs       = ${n(p.bem_payback_yrs)},
          bem_verdict           = ${s(p.bem_verdict)},
          bem_submitted_by      = ${s(p.bem_submitted_by || 'web-app')},
          bem_submitted_at      = CURRENT_TIMESTAMP(),
          modified_at           = CURRENT_TIMESTAMP()
        WHERE near_id = ${s(nearId)} AND CAST(ic_date AS STRING) LIKE '${icDate}%'`);
      return { statusCode: 200, headers: cors, body: JSON.stringify({ success: true, action: 'updated', bem_id: existing[0].bem_id }) };
    }

    await runQuery(`
      INSERT INTO hive_metastore.default.kfc_bem_results (
        bem_id, near_id, ic_date, store_name, franchisee, facilityty, adm1_en, lat, lon,
        bem_wpra_weekly, bem_weekly_txn, bem_avg_ticket, bem_annual_revenue, bem_capex,
        bem_cap_land, bem_cap_building, bem_cap_fitout, bem_cap_shopfit, bem_cap_kitchen,
        bem_cap_hvac, bem_cap_electronics, bem_cap_proffees, bem_cap_kiosks, bem_cap_generator,
        bem_cap_solar, bem_cap_water, bem_cap_other, bem_cap_contribution, bem_cap_initialfee,
        bem_cap_minor_remodel, bem_cap_major_remodel, bem_depr_yr1, bem_nopat_yr1,
        bem_cos_pct, bem_labour_pct, bem_store_cost_pct, bem_rental_pct,
        bem_royalties_pct, bem_advertising_pct, bem_delcomm_pct, bem_taxrate_pct,
        bem_txn_growth_pct, bem_ticket_growth_pct, bem_inflation_pct, bem_rental_esc_pct,
        bem_wacc_pct, bem_hurdle_pct, bem_drivethru_pct, bem_delivery_pct, bem_instore_pct,
        bem_ebitda, bem_ebitda_margin, bem_npv, bem_irr, bem_payback_yrs,
        bem_verdict, bem_submitted_by, bem_submitted_at, created_at, modified_at
      ) VALUES (
        ${s(bemId)}, ${s(nearId)}, TO_DATE(${s(icDate)}), ${s(storeName)},
        ${s(p.franchisee||null)}, ${s(p.facilityty||p.facility||null)}, ${s(p.adm1_en||null)},
        ${n(p.lat)}, ${n(p.lon)},
        ${n(p.bem_wpra_weekly)}, ${n(p.bem_weekly_txn)}, ${n(p.bem_avg_ticket)},
        ${n(p.bem_annual_revenue)}, ${n(p.bem_capex)},
        ${n(p.bem_cap_land)}, ${n(p.bem_cap_building)}, ${n(p.bem_cap_fitout)},
        ${n(p.bem_cap_shopfit)}, ${n(p.bem_cap_kitchen)}, ${n(p.bem_cap_hvac)},
        ${n(p.bem_cap_electronics)}, ${n(p.bem_cap_proffees)}, ${n(p.bem_cap_kiosks)},
        ${n(p.bem_cap_generator)}, ${n(p.bem_cap_solar)}, ${n(p.bem_cap_water)},
        ${n(p.bem_cap_other)}, ${n(p.bem_cap_contribution)}, ${n(p.bem_cap_initialfee)},
        ${n(p.bem_cap_minor_remodel)}, ${n(p.bem_cap_major_remodel)},
        ${n(p.bem_depr_yr1)}, ${n(p.bem_nopat_yr1)},
        ${n(p.bem_cos_pct)}, ${n(p.bem_labour_pct)}, ${n(p.bem_store_cost_pct)}, ${n(p.bem_rental_pct)},
        ${n(p.bem_royalties_pct)}, ${n(p.bem_advertising_pct)}, ${n(p.bem_delcomm_pct)}, ${n(p.bem_taxrate_pct)},
        ${n(p.bem_txn_growth_pct)}, ${n(p.bem_ticket_growth_pct)}, ${n(p.bem_inflation_pct)}, ${n(p.bem_rental_esc_pct)},
        ${n(p.bem_wacc_pct)}, ${n(p.bem_hurdle_pct)},
        ${n(p.bem_drivethru_pct)}, ${n(p.bem_delivery_pct)}, ${n(p.bem_instore_pct)},
        ${n(p.bem_ebitda)}, ${n(p.bem_ebitda_margin)}, ${n(p.bem_npv_5yr)},
        ${n(p.bem_irr)}, ${n(p.bem_payback_yrs)},
        ${s(p.bem_verdict)}, ${s(p.bem_submitted_by||'web-app')},
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
      )`);
    return { statusCode: 200, headers: cors, body: JSON.stringify({ success: true, action: 'inserted', bem_id: bemId }) };

  } catch(e) {
    console.error('BEM save error:', e);
    return { statusCode: 500, headers: cors, body: JSON.stringify({ error: e.message }) };
  }
};
