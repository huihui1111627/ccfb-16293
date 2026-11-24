'use strict';

const $ = (id) => document.getElementById(id);
const nf = new Intl.NumberFormat('zh-CN');

const STATE_LABELS = {
  idle: '空闲', sampling: '抽样中', preagg: '预聚合', passthrough: '透传',
  single: '单线程汇总', sorted: '有序流式', flushing: '刷写分区',
};
const DECISION_LABELS = {
  preagg: '预聚合', passthrough: '直接透传', single: '单线程', sorted: '有序流式',
};
const MODE_LABELS = {
  adaptive: '自适应', always_pre: '总是预聚合', never_pre: '从不预聚合',
};

let lastScenarioCount = 0;
let configPushTimer = null;

function fmtInt(v) { return nf.format(Math.round(Number(v) || 0)); }
function fmtFloat(v, digits = 2) {
  if (!Number.isFinite(v)) return '—';
  return Number(v).toLocaleString('zh-CN', { maximumFractionDigits: digits, minimumFractionDigits: digits });
}
function fmtBytes(v) {
  v = Number(v) || 0;
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return `${v.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}
function fmtMS(ms) {
  ms = Number(ms) || 0;
  if (ms < 1000) return `${fmtFloat(ms, 0)} ms`;
  return `${fmtFloat(ms / 1000, 2)} s`;
}
function pct(v, digits = 1) { return `${(Number(v) * 100).toFixed(digits)}%`; }

async function postJSON(url, body) {
  const res = await fetch(url, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body || {}),
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

function readConfigFromControls() {
  return {
    threads: Number($('threads').value),
    local_table_max: Number($('local').value),
    partitions: Number($('partitions').value),
    auto_partitions: $('autoPartitions').checked,
    high_unique_ratio: Number($('threshold').value),
    batch_rows: Number($('batchRows').value),
  };
}

function updateConfigLabels(c) {
  $('threadsVal').textContent = fmtInt(c.threads);
  $('localVal').textContent = fmtInt(c.local_table_max);
  $('partitionsVal').textContent = fmtInt(c.partitions);
  $('thresholdVal').textContent = Number(c.high_unique_ratio).toFixed(2);
  $('batchVal').textContent = fmtInt(c.batch_rows);
}

function syncControls(c) {
  updateConfigLabels(c);
  const mapping = [
    ['threads', c.threads], ['local', c.local_table_max], ['partitions', c.partitions],
    ['threshold', c.high_unique_ratio], ['batchRows', c.batch_rows],
  ];
  for (const [id, value] of mapping) {
    const el = $(id);
    if (document.activeElement !== el) el.value = value;
  }
  if (document.activeElement !== $('autoPartitions')) $('autoPartitions').checked = !!c.auto_partitions;
  $('effectiveVal').textContent = fmtInt(window.__lastState?.effective_partitions || c.partitions);
}

function scheduleConfigPush() {
  updateConfigLabels(readConfigFromControls());
  clearTimeout(configPushTimer);
  configPushTimer = setTimeout(async () => {
    try { await postJSON('/api/config', readConfigFromControls()); }
    catch (e) { $('errorBanner').textContent = `参数更新失败: ${e.message}`; $('errorBanner').classList.remove('hidden'); }
  }, 120);
}

function initControls() {
  for (const id of ['threads', 'local', 'partitions', 'threshold', 'batchRows']) {
    $(id).addEventListener('input', scheduleConfigPush);
  }
  $('autoPartitions').addEventListener('change', scheduleConfigPush);
  $('runBtn').addEventListener('click', async () => {
    try {
      await postJSON('/api/run', { scenario_id: $('scenario').value, mode: $('mode').value });
    } catch (e) { alert(e.message); }
  });
  $('benchBtn').addEventListener('click', async () => {
    try {
      await postJSON('/api/benchmark', { scenario_id: $('scenario').value });
    } catch (e) { alert(e.message); }
  });
  $('verifyBtn').addEventListener('click', async () => {
    try {
      await postJSON('/api/verify', { scenario_id: $('scenario').value, mode: 'adaptive' });
    } catch (e) { alert(e.message); }
  });
}

function renderScenarios(state) {
  const sel = $('scenario');
  if (state.scenarios && state.scenarios.length !== lastScenarioCount) {
    const old = sel.value;
    sel.innerHTML = '';
    for (const sc of state.scenarios) {
      const opt = document.createElement('option');
      opt.value = sc.id;
      opt.textContent = `${sc.name}（目标 ${fmtInt(sc.groups)} 组 / ${fmtInt(sc.rows)} 行）`;
      sel.appendChild(opt);
    }
    if (old) sel.value = old;
    lastScenarioCount = state.scenarios.length;
  }
}

function renderStatus(state) {
  const dot = $('statusDot');
  dot.className = 'dot ' + (state.running ? 'running' : (state.error ? 'error' : 'idle'));
  const kind = { run: '单次运行', benchmark: '三策略基准', verify: '正确性校验' }[state.job_kind] || '';
  $('statusText').textContent = state.running ? `${kind}运行中…` : (state.error ? '发生错误' : '空闲');
  $('statusSub').textContent = state.scenario_name ? `${state.scenario_name} · ${MODE_LABELS[state.mode] || state.mode || ''}` : '选择场景后启动';
  for (const id of ['runBtn', 'benchBtn', 'verifyBtn']) $(id).disabled = !!state.busy;
  $('effectiveVal').textContent = fmtInt(state.effective_partitions || state.config.partitions);

  const err = state.last_error || state.error;
  const eb = $('errorBanner');
  if (err) { eb.textContent = err; eb.classList.remove('hidden'); } else eb.classList.add('hidden');
}

function renderMetrics(state) {
  const workers = state.workers || [];
  const processed = workers.reduce((a, w) => a + Number(w.rows || 0), 0);
  const partitionGroups = (state.partitions || []).reduce((a, p) => a + p.groups, 0);
  const groups = state.result_groups || partitionGroups;
  $('groupCount').textContent = fmtInt(groups);
  $('groupHint').textContent = state.report?.sorted ? '有序流式输出' : `实际分区 ${fmtInt(state.effective_partitions)} 个`;
  $('rowCount').textContent = fmtInt(processed || state.report?.rows || 0);
  const secs = (state.elapsed_ms || 0) / 1000;
  $('throughput').textContent = secs > 0.05 ? `${fmtInt((processed || 0) / secs)} rows/s` : '统计中…';
  $('elapsed').textContent = fmtMS(state.elapsed_ms);
  $('partitionHint').textContent = state.phase ? `阶段：${state.phase}` : '—';
  const mem = state.memory || [];
  const peak = mem.reduce((m, p) => Math.max(m, p.heap_alloc), 0) || state.report?.peak_heap || 0;
  $('peakHeap').textContent = fmtBytes(peak);
  const firstTotal = mem[0]?.total_alloc || state.report?.start_total_alloc || 0;
  const lastTotal = mem[mem.length - 1]?.total_alloc || firstTotal;
  $('allocDelta').textContent = `累计分配 ${fmtBytes(state.report?.total_alloc_delta || (lastTotal - firstTotal))}`;
}

function renderWorkers(state) {
  const root = $('workers');
  const n = state.config.threads;
  const ws = (state.workers || []).slice(0, n);
  if (root.children.length !== ws.length) root.innerHTML = '';
  ws.forEach((w, i) => {
    let el = root.children[i];
    if (!el) {
      el = document.createElement('div');
      el.className = 'worker';
      el.innerHTML = `
        <div class="worker-head"><strong>线程 #${w.id}</strong><span class="badge"></span></div>
        <div class="worker-grid">
          <div class="stat"><span>本地表大小</span><b class="size"></b></div>
          <div class="stat"><span>命中率</span><b class="hit"></b></div>
          <div class="stat"><span>命中/查询</span><b class="lookups"></b></div>
          <div class="stat"><span>已处理行</span><b class="rows"></b></div>
          <div class="stat"><span>批次数</span><b class="batches"></b></div>
          <div class="stat"><span>触顶刷写</span><b class="flushes"></b></div>
        </div>
        <div class="hitbar"><i></i></div>`;
      root.appendChild(el);
    }
    const badge = el.querySelector('.badge');
    if (w.state === 'idle' && w.last_state && w.last_state !== 'idle') {
      badge.textContent = `上批 · ${STATE_LABELS[w.last_state] || w.last_state}`;
      badge.className = `badge ${w.last_state} last-state`;
    } else {
      badge.textContent = STATE_LABELS[w.state] || w.state;
      badge.className = `badge ${w.state}`;
    }
    const globalState = ['single', 'sorted'].includes(w.state) ||
      (w.state === 'idle' && ['single', 'sorted'].includes(w.last_state));
    el.querySelector('.size').textContent = globalState
      ? `${fmtInt(w.table_size)} 组`
      : `${fmtInt(w.table_size)} / ${fmtInt(state.config.local_table_max)}`;
    el.querySelector('.hit').textContent = w.lookups > 0 ? pct(w.hit_rate, 1) : '—';
    el.querySelector('.lookups').textContent = w.lookups > 0 ? `${fmtInt(w.hits)} / ${fmtInt(w.lookups)}` : '无本地查询';
    el.querySelector('.rows').textContent = fmtInt(w.rows);
    el.querySelector('.batches').textContent = fmtInt(w.batches);
    el.querySelector('.flushes').textContent = fmtInt(w.flushes);
    el.querySelector('.hitbar i').style.width = `${Math.min(100, w.hit_rate * 100)}%`;
  });
}

function renderBatches(state) {
  const body = $('batchRowsBody');
  const rows = (state.batches || []).slice().reverse().slice(0, 30);
  body.innerHTML = rows.map(b => `
    <tr>
      <td>${b.id}</td>
      <td>${fmtInt(b.rows)}</td>
      <td>${fmtInt(b.unique)} / ${fmtInt(b.sampled)}</td>
      <td>${pct(b.unique_ratio, 1)}</td>
      <td>${fmtInt(b.estimated_in_batch)}<div class="muted">全局 ${fmtInt(b.global_estimate)}</div></td>
      <td><span class="decision ${b.decision}">${DECISION_LABELS[b.decision] || b.decision}</span></td>
      <td>${b.reason || ''}</td>
      <td>${fmtFloat(b.duration_ms, 1)} ms</td>
    </tr>`).join('') || '<tr><td colspan="8" class="muted">尚无批次。</td></tr>';
}

function renderPartitions(state) {
  const ps = state.partitions || [];
  const bars = $('partitionBars');
  const maxBytes = Math.max(1, ...ps.map(p => p.bytes));
  bars.innerHTML = ps.slice(0, 80).map(p => `
    <div class="pbar-row">
      <span>P${p.id}</span>
      <div class="pbar-track"><div class="pbar-fill" style="width:${Math.max(0.5, p.bytes / maxBytes * 100)}%"></div></div>
      <span>${fmtBytes(p.bytes)}</span>
    </div>`).join('');

  const body = $('partitionBody');
  body.innerHTML = ps.map(p => `
    <tr>
      <td>${p.id}</td><td>${fmtInt(p.groups)}</td><td>${fmtInt(p.messages)}</td>
      <td>${fmtInt(p.source_rows)}</td><td>${fmtBytes(p.bytes)}</td><td>${pct(p.share, 2)}</td>
    </tr>`).join('') || '<tr><td colspan="6" class="muted">尚无分区。</td></tr>';
}

function drawMemoryChart(state) {
  const canvas = $('memChart');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  const W = Math.max(320, rect.width);
  const H = 280;
  if (canvas.width !== Math.round(W * dpr) || canvas.height !== H * dpr) {
    canvas.width = Math.round(W * dpr);
    canvas.height = H * dpr;
  }
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, W, H);
  const pad = { l: 72, r: 18, t: 18, b: 34 };
  const cw = W - pad.l - pad.r, ch = H - pad.t - pad.b;
  ctx.strokeStyle = 'rgba(255,255,255,.08)';
  ctx.fillStyle = '#8ea2c6';
  ctx.font = '11px -apple-system, sans-serif';
  ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i++) {
    const y = pad.t + ch * i / 4;
    ctx.beginPath(); ctx.moveTo(pad.l, y); ctx.lineTo(W - pad.r, y); ctx.stroke();
  }
  const mem = state.memory || [];
  if (mem.length < 2) {
    ctx.fillStyle = '#8ea2c6';
    ctx.fillText('运行后这里绘制 50ms 采样的内存曲线', pad.l + 12, pad.t + ch / 2);
    return;
  }
  const maxT = Math.max(1000, ...mem.map(p => p.elapsed_ms));
  const maxY = Math.max(1, ...mem.map(p => Math.max(p.heap_alloc, p.tracked_bytes || 0))) * 1.08;
  for (let i = 0; i <= 4; i++) {
    const y = pad.t + ch * i / 4;
    const val = maxY * (4 - i) / 4;
    ctx.fillText(fmtBytes(val), 8, y + 4);
  }
  ctx.fillText('0s', pad.l, H - 12);
  ctx.fillText(`${(maxT / 1000).toFixed(1)}s`, W - pad.r - 30, H - 12);

  function path(key, color) {
    ctx.beginPath();
    mem.forEach((p, i) => {
      const x = pad.l + cw * (p.elapsed_ms / maxT);
      const y = pad.t + ch * (1 - Math.max(0, p[key] || 0) / maxY);
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = color; ctx.lineWidth = 2.2; ctx.stroke();
  }
  path('heap_alloc', '#5aa7ff');
  path('tracked_bytes', '#b58cff');
}

function decisionSummary(d) {
  if (!d) return '—';
  return Object.entries(d).map(([k, v]) => `${DECISION_LABELS[k] || k} ${v}`).join(' · ');
}

function renderBenchmark(state) {
  const body = $('benchBody');
  const b = state.benchmark;
  if (!b || !b.results?.length) {
    if (state.busy && state.job_kind === 'benchmark') {
      body.innerHTML = '<tr><td colspan="10" class="muted">三种策略依次运行中，可观察实时批次/分区/内存…</td></tr>';
    }
    return;
  }
  body.innerHTML = b.results.map(r => `
    <tr>
      <td><span class="decision ${r.mode}">${MODE_LABELS[r.mode] || r.mode}</span>${r.sorted ? ' · 有序流' : ''}</td>
      <td><b>${fmtMS(r.duration_ms)}</b></td>
      <td>${fmtInt(r.rows_per_second)} rows/s</td>
      <td>${fmtInt(r.groups)}</td>
      <td>${fmtBytes(r.peak_heap)}</td>
      <td>${fmtBytes(r.peak_heap_delta)}</td>
      <td>${fmtBytes(r.total_alloc_delta)}</td>
      <td>${fmtBytes(r.heap_after_gc)}</td>
      <td>${fmtInt(r.effective_partitions)}</td>
      <td>${decisionSummary(r.decisions)}</td>
    </tr>`).join('');
}

function renderVerify(state) {
  const banner = $('verifyBanner');
  const v = state.verify;
  if (!v) { banner.classList.add('hidden'); return; }
  banner.classList.remove('hidden', 'error');
  if (v.ok) {
    banner.innerHTML = `✓ 校验通过：${v.scenario_id} 与单线程全量朴素实现逐组一致；${fmtInt(v.engine_groups)} 组、${fmtInt(v.materialized_rows)} 行，最大浮点相对误差 ${v.max_float_rel_err.toExponential(2)}。`;
  } else {
    banner.classList.add('error');
    banner.innerHTML = `✗ 校验失败：${fmtInt(v.mismatch_count)} 处不一致。<pre>${(v.mismatches || []).slice(0, 10).join('\n')}</pre>`;
  }
}

function renderTopGroups(state) {
  const ol = $('topGroups');
  const tops = state.report?.top_groups;
  if (!tops?.length) {
    ol.innerHTML = '<li class="muted">运行后显示。</li>';
    return;
  }
  ol.innerHTML = tops.map(t => `<li>${t.key}<span class="v">${fmtFloat(t.value, 3)} m³</span></li>`).join('');
}

async function tick() {
  try {
    const state = await (await fetch('/api/state')).json();
    window.__lastState = state;
    renderScenarios(state);
    syncControls(state.config);
    renderStatus(state);
    renderMetrics(state);
    renderWorkers(state);
    renderBatches(state);
    renderPartitions(state);
    drawMemoryChart(state);
    renderBenchmark(state);
    renderVerify(state);
    renderTopGroups(state);
  } catch (e) {
    $('statusText').textContent = `页面轮询失败: ${e.message}`;
  }
}

initControls();
tick();
setInterval(tick, 250);
window.addEventListener('resize', () => drawMemoryChart(window.__lastState || { memory: [] }));
