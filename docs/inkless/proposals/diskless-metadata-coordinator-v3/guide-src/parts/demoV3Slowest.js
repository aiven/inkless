/* ---------------------------------------------------------------- per-request completion */
function demoV3Slowest() {
  const nEl = $('#sp-n'), failedEl = $('#sp-failed'), inputs = $('#sp-inputs'), gantt = $('#sp-gantt');
  if (!nEl) return;
  const v2Btn = $('#sp-v2'), v3Btn = $('#sp-v3');
  const DEADLINE = 2000;
  let v3 = false;
  const values = Array.from({ length: 16 }, (_, i) => [20, 45, 120, 35][i % 4]);
  const buildInputs = () => {
    const n = +nEl.value;
    const oldFailed = failedEl.value;
    inputs.innerHTML = '';
    failedEl.innerHTML = '<option value="-1">none</option>';
    for (let i = 0; i < n; i++) {
      failedEl.append(h('option', { value: i }, `cp${i}`));
      const value = values[i];
      const output = h('span', { class: 'chip', id: `sp-l-${i}` }, value + ' ms');
      const range = h('input', { type: 'range', min: 5, max: 500, step: 5, value, 'aria-label': `cp${i} part latency` });
      range.addEventListener('input', () => { values[i] = +range.value; output.textContent = values[i] + ' ms'; render(); });
      inputs.append(h('label', { style: { minWidth: '180px' } }, `cp${i}`, range, output));
    }
    if ([...failedEl.options].some(o => o.value === oldFailed)) failedEl.value = oldFailed;
  };
  const render = () => {
    const n = +nEl.value, failed = +failedEl.value;
    const done = i => (failed === i ? DEADLINE : values[i]);
    const reqs = [
      { name: 'A', cps: [0, 1] },
      { name: 'B', cps: Array.from({ length: n }, (_, i) => i) },
      { name: 'C', cps: [2] },
    ];
    const fileDone = Math.max(...reqs[1].cps.map(done));
    const reqDone = r => (v3 ? Math.max(...r.cps.map(done)) : fileDone);
    const rows = Array.from({ length: n }, (_, i) => ({
      label: `cp${i}`,
      segs: [{ from: 0, to: done(i), color: failed === i ? 'var(--bad)' : cpColor(i), text: failed === i ? 'deadline' : values[i] + ' ms' }],
    }));
    for (const r of reqs) {
      const t = reqDone(r);
      rows.push({ label: `request ${r.name}`, segs: [{ from: 0, to: t, color: r.cps.includes(failed) ? 'var(--warn)' : 'var(--accent)', text: fmt(t) + ' ms', title: `request ${r.name}: ${r.cps.map(i => 'cp' + i).join(', ')}` }] });
    }
    drawGantt(gantt, rows, 2100, 500, ' ms');
    const clear = reqs.filter(r => !r.cps.includes(failed)).map(r => r.name);
    const names = list => list.length === 1 ? `request ${list[0]}` : `requests ${list.slice(0, -1).join(', ')} and ${list[list.length - 1]}`;
    if (!v3) {
      show($('#sp-readout'), failed >= 0 ? 'warn' : '',
        `Whole-file completion: every request completes with the file at <b>${fmt(fileDone)} ms</b>, the slowest of ${n} parts.` +
        (failed >= 0 && clear.length ? ` ${names(clear)} ${clear.length === 1 ? 'holds' : 'hold'} no batch for cp${failed} and still ${clear.length === 1 ? 'waits' : 'wait'} for its deadline.` : ''));
    } else {
      const [a, b, c] = reqs.map(reqDone);
      show($('#sp-readout'), failed >= 0 && clear.length ? 'good' : '',
        `Per-request completion: request A completes at <b>${fmt(a)} ms</b>, B at <b>${fmt(b)} ms</b>, and C at <b>${fmt(c)} ms</b>. ` +
        (failed >= 0
          ? `Requests with a batch for cp${failed} complete at the deadline, with <code>KAFKA_STORAGE_ERROR</code> for those batches. ${clear.length ? `${names(clear)} ${clear.length === 1 ? "doesn't" : "don't"} wait for it.` : ''} `
          : 'Each request waits only for the parts that hold its batches. ') +
        `The file still finishes at ${fmt(fileDone)} ms, which drives <code>finishedThrough</code> and cleanup.`);
    }
  };
  nEl.addEventListener('change', () => { buildInputs(); render(); });
  failedEl.addEventListener('change', render);
  v2Btn.addEventListener('click', () => { v3 = false; setOn([v2Btn, v3Btn], v2Btn); render(); });
  v3Btn.addEventListener('click', () => { v3 = true; setOn([v2Btn, v3Btn], v3Btn); render(); });
  buildInputs();
  render();
}
