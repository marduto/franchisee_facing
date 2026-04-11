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
      headers: {
        'Authorization':  `Bearer ${TOKEN}`,
        'Content-Type':   'application/json',
        'Content-Length': Buffer.byteLength(buf)
      }
    }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => { try { resolve(JSON.parse(data)); } catch(e) { reject(new Error('Parse: ' + data.slice(0,200))); } });
    });
    req.on('error', reject);
    req.write(buf); req.end();
  });
}

function httpsGet(path) {
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname: HOST, path, method: 'GET',
      headers: { 'Authorization': `Bearer ${TOKEN}` }
    }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => { try { resolve(JSON.parse(data)); } catch(e) { reject(new Error('Parse: ' + data.slice(0,200))); } });
    });
    req.on('error', reject);
    req.end();
  });
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function runQuery(sql) {
  const deadline = Date.now() + 22000; // bail 4s before Netlify's 26s hard limit
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
    // Back off slower during warehouse cold start (PENDING state)
    await sleep(state === 'PENDING' ? 5000 : 3000);
    result = await httpsGet(`/api/2.0/sql/statements/${submit.statement_id}`);
  }
  if (result.status?.state !== 'SUCCEEDED')
    throw new Error(`Timed out waiting for Databricks. Last state: ${result.status?.state}`);
  const cols = result.manifest?.schema?.columns?.map(c => c.name) || [];
  let rows = result.result?.data_array || [];

  // Fetch additional chunks if Databricks paginated the result
  let chunkIndex = result.result?.next_chunk_index;
  while (chunkIndex != null) {
    const chunk = await httpsGet(`/api/2.0/sql/statements/${submit.statement_id}/result/chunks/${chunkIndex}`);
    rows = rows.concat(chunk.data_array || []);
    chunkIndex = chunk.next_chunk_index;
  }

  return rows.map(row => { const o = {}; cols.forEach((col, i) => { o[col] = row[i] ?? null; }); return o; });
}

const SQL = `  SELECT near_id, store_name, map_label, lat, lon, status, franchisee, facilityty,
         store_type, ic_date, ic_ask, ic_category, ic_minutes, ic_pipeline_status,
         dev_manager, adm1_en, adm0_en, ytd_wpra,
         foh, boh, total_size, bem_address,
         bem_submitted_by, bem_submitted_at,
         status AS site_status, reg_overdue_vs_avg, app_overdue_vs_avg,
         design, shell_construction, fitout_construction, date_open,
         implied_shell_date, implied_fitout_date, implied_open_date,
         implied_final_plan_date, implied_council_date,
         shell_rag, fitout_rag, final_plan_rag, council_rag, timeline_rag_overall,
         implied_vs_planned_open_days, open_date_risk,
         bench_avg_shell_days, bench_avg_fitout_days, bench_avg_open_days,
         bench_avg_final_plan_days, bench_avg_council_days,
         days_until_planned_open
  FROM kfc_assetuniverse_2025
  WHERE lat IS NOT NULL AND lon IS NOT NULL`;

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: cors, body: '' };
  const user = getUser(event);
  if (!user) return { statusCode: 401, headers: cors, body: JSON.stringify({ error: 'Authentication required' }) };
  if (!TOKEN) return { statusCode: 500, headers: cors, body: JSON.stringify({ error: 'DATABRICKS_TOKEN not set' }) };
  try {
    const rows = await runQuery(SQL);
    const sites = rows.map(o => ({
      id:                 o.near_id,
      near_id:            o.near_id,
      store_name:         o.store_name,
      map_label:          o.map_label || o.store_name,
      lat:                parseFloat(o.lat),
      lon:                parseFloat(o.lon),
      status:             o.status,
      franchisee:         o.franchisee,
      facility:           o.facilityty,
      facilityty:         o.facilityty,
      ic_date:            o.ic_date,
      ic_ask:             o.ic_ask,
      ic_category:        o.ic_category,
      dev_manager:        o.dev_manager,
      adm1_en:            o.adm1_en,
      adm0_en:            o.adm0_en,
      ytd_wpra:           o.ytd_wpra,
      store_type:         o.store_type,
      ic_minutes:         o.ic_minutes,
      ic_pipeline_status: o.ic_pipeline_status,
      bem_address:        o.bem_address,
      bem_submitted_by:   o.bem_submitted_by,
      bem_submitted_at:   o.bem_submitted_at,
      foh:                parseFloat(o.foh)                || null,
      boh:                parseFloat(o.boh)                || null,
      design:             o.design,
      shell_construction:     o.shell_construction,
      fitout_construction:    o.fitout_construction,
      date_open:              o.date_open,
      implied_shell_date:        o.implied_shell_date,
      implied_fitout_date:       o.implied_fitout_date,
      implied_open_date:         o.implied_open_date,
      implied_final_plan_date:   o.implied_final_plan_date,
      implied_council_date:      o.implied_council_date,
      shell_rag:                 o.shell_rag,
      fitout_rag:                o.fitout_rag,
      final_plan_rag:            o.final_plan_rag,
      council_rag:               o.council_rag,
      timeline_rag_overall:      o.timeline_rag_overall,
      implied_vs_planned_open_days: parseInt(o.implied_vs_planned_open_days) || null,
      open_date_risk:            o.open_date_risk,
      days_until_planned_open:   parseInt(o.days_until_planned_open) || null,
      bench_avg_shell_days:      parseInt(o.bench_avg_shell_days) || null,
      bench_avg_fitout_days:     parseInt(o.bench_avg_fitout_days) || null,
      bench_avg_open_days:       parseInt(o.bench_avg_open_days) || null,
      bench_avg_final_plan_days: parseInt(o.bench_avg_final_plan_days) || null,
      bench_avg_council_days:    parseInt(o.bench_avg_council_days) || null,
      site_status:            o.site_status,
      reg_overdue_vs_avg:     parseFloat(o.reg_overdue_vs_avg) || null,
      app_overdue_vs_avg:     parseFloat(o.app_overdue_vs_avg) || null
    })).filter(s => s.lat && s.lon && !isNaN(s.lat) && !isNaN(s.lon));
    let filtered = sites;
    if (user.role === 'franchisee' && user.franchisee) {
      const fz = user.franchisee.toLowerCase();
      filtered = sites.filter(s => {
        if ((s.status || '').toLowerCase() === 'site identified') {
          return (s.franchisee || '').toLowerCase() === fz;
        }
        return true;
      });
    }
    return { statusCode: 200, headers: cors, body: JSON.stringify({ sites: filtered, count: filtered.length }) };
  } catch(e) {
    return { statusCode: 500, headers: cors, body: JSON.stringify({ error: e.message }) };
  }
};
