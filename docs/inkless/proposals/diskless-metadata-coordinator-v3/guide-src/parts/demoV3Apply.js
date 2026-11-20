/* ---------------------------------------------------------------- SQLite group apply */
function demoV3Apply() {
  const logEl = $('#ga-log');
  if (!logEl) return;
  const stopEl = $('#ga-stop'), v2Btn = $('#ga-v2'), v3Btn = $('#ga-v3');
  const sizes = [600, 1400, 900, 5200, 300, 1100, 1700, 500, 2300, 400, 900, 700];
  const CAP = 4096, TRIGGER = 1000;
  const colors = ['var(--cp0)', 'var(--cp3)', 'var(--cp1)', 'var(--cp5)', 'var(--cp4)', 'var(--cp7)'];
  let v3 = false;
  const groupsOf = () => {
    if (!v3) return sizes.map((_, i) => [i]);
    const groups = [];
    let cur = [], bytes = 0;
    sizes.forEach((sz, i) => {
      if (cur.length && bytes + sz > CAP) { groups.push(cur); cur = []; bytes = 0; }
      cur.push(i);
      bytes += sz;
    });
    if (cur.length) groups.push(cur);
    return groups;
  };
  const mib = kib => (kib / 1024).toFixed(1) + ' MiB';
  const render = () => {
    const stop = +stopEl.value;
    const end = stop < 0 ? sizes.length : stop;
    const groups = groupsOf();
    const groupOf = [];
    groups.forEach((g, k) => g.forEach(i => { groupOf[i] = k; }));
    logEl.innerHTML = '';
    sizes.forEach((sz, i) => {
      const cls = 'cell' + (i === stop ? ' lost' : i > stop && stop >= 0 ? ' unapplied' : '');
      logEl.append(h('div', { class: cls, title: `r${i + 1}: ${fmt(sz)} KiB`, style: { width: `${1.9 + sz / 700}rem`, background: v3 ? colors[groupOf[i] % colors.length] : 'var(--faint)' } }, `r${i + 1}`));
    });
    const applied = groups.map(g => g.filter(i => i < end)).filter(g => g.length);
    let wal = 0, peak = 0, ckpt = 0;
    for (const g of applied) {
      wal += g.reduce((sum, i) => sum + sizes[i], 0) / 4;
      peak = Math.max(peak, wal);
      if (wal >= TRIGGER) { ckpt++; wal = 0; }
    }
    $('#ga-legend').innerHTML = v3
      ? groups.map((g, k) => `<span><span class="dot" style="background:${colors[k % colors.length]}"></span>group ${k + 1}: r${g[0] + 1}${g.length > 1 ? '–r' + (g[g.length - 1] + 1) : ''}, ${mib(g.reduce((sum, i) => sum + sizes[i], 0))}</span>`).join('')
      : '<span><span class="dot" style="background:var(--faint)"></span>one transaction per record</span>';
    const partial = v3 && stop >= 0 && groups[groupOf[stop]][0] < stop;
    $('#ga-tx').innerHTML = `<h6>SQLite transactions ${chip(v3 ? 'good' : 'warn', fmt(applied.length))}</h6>` +
      `<dl class="kv"><dt>applied through</dt><dd>${end === 0 ? 'nothing' : 'r' + end}</dd>` +
      `<dt>stopping record</dt><dd>${stop < 0 ? 'none' : `r${stop + 1}${partial ? ', rolled back to its savepoint' : ''}`}</dd>` +
      `<dt>after the stop</dt><dd>${stop < 0 ? 'nothing' : `${sizes.length - stop} records unapplied; they fail with NOT_COORDINATOR after the leader resigns`}</dd>` +
      `<dt>largest group</dt><dd>${mib(Math.max(...groups.map(g => g.reduce((sum, i) => sum + sizes[i], 0))))}</dd></dl>`;
    $('#ga-ckpt').innerHTML = `<h6>Checkpoints ${chip(v3 || !ckpt ? 'good' : 'bad', v3 ? '0 on the apply thread' : `${ckpt} on the apply thread`)}</h6>` +
      `<dl class="kv"><dt>checkpoints</dt><dd>${ckpt}</dd><dt>runs on</dt><dd>${v3 ? 'the broker checkpoint thread, PASSIVE' : 'the thread whose commit crosses 1000 pages'}</dd>` +
      `<dt>WAL pages at peak</dt><dd>${fmt(Math.round(peak))}</dd></dl>`;
    show($('#ga-readout'), v3 ? 'good' : ckpt ? 'warn' : '',
      v3
        ? `Group apply covers ${end} record${end === 1 ? '' : 's'} in ${applied.length} transaction${applied.length === 1 ? '' : 's'}. The 4 MiB cap closes a group before the record that would cross it, and r4 gets a group of its own. ` +
          (partial ? `The stop at r${stop + 1} rolls back to its savepoint and commits its group through r${stop}, so the applied offset stays exact. ` : '') +
          (ckpt ? `The checkpoint thread, not the apply thread, runs the ${ckpt} checkpoint${ckpt === 1 ? '' : 's'}, and the WAL peaks at ${fmt(Math.round(peak))} pages.` : 'The WAL stays under the 1000-page trigger, so no checkpoint runs.')
        : `Per-record apply covers ${end} record${end === 1 ? '' : 's'} in ${end} transaction${end === 1 ? '' : 's'}, and ${ckpt} automatic checkpoint${ckpt === 1 ? ' stalls' : 's stall'} the thread that applies and replies.`);
  };
  stopEl.addEventListener('change', render);
  v2Btn.addEventListener('click', () => { v3 = false; setOn([v2Btn, v3Btn], v2Btn); render(); });
  v3Btn.addEventListener('click', () => { v3 = true; setOn([v2Btn, v3Btn], v3Btn); render(); });
  render();
}
