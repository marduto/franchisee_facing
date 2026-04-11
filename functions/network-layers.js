// network-layers.js — Netlify Function
// GET /?layer=ilp      → returns ILP wave site selections from default.gap_analysis
// GET /?layer=competitors → returns competitor locations from default.fast_food_africa
const https = require('https');

const HOST      = process.env.DATABRICKS_HOST         || 'adb-1077037977847740.0.azuredatabricks.net';
const HTTP_PATH = process.env.DATABRICKS_HTTP_PATH    || '/sql/1.0/warehouses/1ad19be06aab1a65';
const TOKEN     = process.env.DATABRICKS_TOKEN;
const WAREHOUSE = process.env.DATABRICKS_WAREHOUSE_ID || HTTP_PATH.split('/').pop();

const cors = {
  'Content-Type': 'application/json',
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type'
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
  const deadline = Date.now() + 28000;
  const submit = await httpsPost('/api/2.0/sql/statements', {
    statement: sql, warehouse_id: WAREHOUSE,
    wait_timeout: '0s', disposition: 'INLINE', format: 'JSON_ARRAY'
  });
  if (!submit.statement_id) throw new Error('No statement_id: ' + JSON.stringify(submit).slice(0,300));
  let result = submit;
  for (let i = 0; i < 20; i++) {
    const state = result.status?.state;
    if (state === 'SUCCEEDED') break;
    if (['FAILED','CANCELED','CLOSED'].includes(state))
      throw new Error(result.status?.error?.message || `Query ${state}`);
    if (Date.now() > deadline) throw new Error('WAREHOUSE_COLD: warehouse starting — retry in 30s');
    await sleep(3000);
    result = await httpsGet(`/api/2.0/sql/statements/${submit.statement_id}`);
  }
  if (result.status?.state !== 'SUCCEEDED') throw new Error('Query timed out');
  const cols = result.manifest?.schema?.columns?.map(c => c.name) || [];
  const rows = result.result?.data_array || [];
  return rows.map(row => { const o={}; cols.forEach((c,i)=>{ o[c]=row[i]??null; }); return o; });
}

const LAYER_QUERIES = {
  ilp: `
    SELECT
      poi_id, poi_lat, poi_lon,
      wave, assigned_facility_type, country,
      net_wpra_uplift_usd, net_wpra_uplift,
      predicted_wpra_total, site_score,
      wpra_tier, is_greenfield, cann_pct
    FROM hive_metastore.default.gap_analysis
    WHERE poi_lat IS NOT NULL AND poi_lon IS NOT NULL
    ORDER BY wave, net_wpra_uplift_usd DESC
    LIMIT 2000`,

  competitors: (params) => `
    SELECT
      poi_id, brand, name, lon, lat,
      country, admin1,
      pred_brand, category, pred_category,
      kfc_pred_facilitytype_code,
      scrape_date
    FROM hive_metastore.default.fast_food_africa
    WHERE lat IS NOT NULL AND lon IS NOT NULL
      ${params.minLat ? `AND lat  BETWEEN ${params.minLat} AND ${params.maxLat}` : ''}
      ${params.minLon ? `AND lon  BETWEEN ${params.minLon} AND ${params.maxLon}` : ''}
      ${params.country ? `AND LOWER(country) = LOWER('${params.country.replace(/'/g,"''")}')` : ''}
    ORDER BY pred_brand, name
    LIMIT 5000`
};

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: cors, body: '' };
  const secret = process.env.APP_SECRET;
  if (secret && event.headers['x-app-secret'] !== secret)
    return { statusCode: 401, headers: cors, body: JSON.stringify({ error: 'Unauthorized' }) };
  if (!TOKEN)
    return { statusCode: 500, headers: cors, body: JSON.stringify({ error: 'DATABRICKS_TOKEN not set' }) };

  const qp    = event.queryStringParameters || {};
  const layer = (qp.layer || '').toLowerCase();
  if (!LAYER_QUERIES[layer])
    return { statusCode: 400, headers: cors, body: JSON.stringify({ error: 'layer must be: ilp or competitors' }) };

  // Build spatial + country params for competitors
  const params = {
    minLat:  qp.min_lat  || null,
    maxLat:  qp.max_lat  || null,
    minLon:  qp.min_lon  || null,
    maxLon:  qp.max_lon  || null,
    country: qp.country  || null,
  };

  try {
    const query = typeof LAYER_QUERIES[layer] === 'function'
      ? LAYER_QUERIES[layer](params)
      : LAYER_QUERIES[layer];
    const records = await runQuery(query);
    return { statusCode: 200, headers: cors, body: JSON.stringify({ records, count: records.length, layer }) };
  } catch(e) {
    return { statusCode: 500, headers: cors, body: JSON.stringify({ error: e.message, layer }) };
  }
};
