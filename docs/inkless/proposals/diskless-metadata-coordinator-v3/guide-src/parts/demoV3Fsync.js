/* ---------------------------------------------------------------- fsync cost */
function demoV3Fsync() {
  const nEl = $('#fs-n');
  if (!nEl) return;
  const nnEl = $('#fs-nn'), rEl = $('#fs-r'), bEl = $('#fs-b'), msEl = $('#fs-ms'), emptyBtn = $('#fs-empty');
  const v2Btn = $('#fs-v2'), v3Btn = $('#fs-v3');
  let v3 = false;
  const num = x => (Number.isInteger(x) ? fmt(x) : x.toFixed(1));
  const render = () => {
    const N = +nnEl.value;
    const touched = Math.max(1, Math.min(N, Math.floor(+nEl.value) || 1));
    const r = +rEl.value, b = +bEl.value, ms = +msEl.value;
    const emptyOn = emptyBtn.classList.contains('on');
    const empty = emptyOn && touched < N;
    const parts = touched + (empty ? 1 : 0);
    const commitFlushes = parts * r;
    const fenceFlushes = !v3 && empty ? (parts - 1) * r : 0;
    const total = commitFlushes + fenceFlushes;
    const low = (N * r) / b, high = N * r;
    $('#fs-ms-label').textContent = ms + ' ms';
    $('#fs-count').innerHTML = `<h6>Log flushes per file ${chip(total > 12 ? 'bad' : total > 4 ? 'warn' : 'info', `${total} against PostgreSQL's 1`)}</h6>` +
      `<dl class="kv"><dt>parts</dt><dd>${touched} participant${touched === 1 ? '' : 's'}${empty ? ' + 1 empty tracking part' : emptyOn ? ' (every partition is a participant, so the tracking part holds batches)' : ''}</dd>` +
      `<dt>commit flushes</dt><dd>${parts} × ${r} = ${commitFlushes}</dd>` +
      `<dt>commit-time fence flushes</dt><dd>${v3 ? 'none in v3' : empty ? `${parts - 1} × ${r} = ${fenceFlushes}` : 'none: the tracking part holds batches'}</dd>` +
      `<dt>per broker per round</dt><dd>${num(low)} to ${high}, or ${num(low * 4)} to ${high * 4} per second</dd></dl>`;
    $('#fs-path').innerHTML = `<h6>Commit path ${chip('warn', 'two in a row')}</h6>` +
      `<p class="small" style="margin:0">Leader log flush on the runtime thread, then follower fetcher flushes before the high watermark advances.</p>` +
      `<dl class="kv" style="margin-top:0.4rem"><dt>illustrative one-flush time</dt><dd>${ms} ms</dd><dt>illustrative series</dt><dd>2 × ${ms} = ${2 * ms} ms</dd><dt>measured latency</dt><dd>open; the first benchmark runs at 3 brokers</dd></dl>`;
    show($('#fs-readout'), total > 12 ? 'warn' : '',
      v3
        ? `V3 keeps the commit fan-out: ${commitFlushes} flush${commitFlushes === 1 ? '' : 'es'} for this file${empty ? ', with no commit-time fences for the empty tracking part' : ''}. ` +
          (b === 3 ? `At 3 brokers, the smallest supported cluster, every broker hosts every coordinator replica, so each flushes at least ${num(low)} times per round. ` : `With ${b} brokers, replicas share the work, but the upper bound per broker stays ${high}. `) +
          'The default N stays 8 until the partition-count benchmark at 3 brokers with N of 1, 2, 4, and 8.'
        : `V2: ${total} flush${total === 1 ? '' : 'es'} for this file${fenceFlushes ? `, of which ${fenceFlushes} come from fencing every participant of the empty tracking part` : ''}, compared with one PostgreSQL WAL flush.`);
  };
  nnEl.addEventListener('change', () => {
    const N = +nnEl.value;
    nEl.max = N;
    nEl.value = Math.min(Math.max(1, Math.floor(+nEl.value) || 1), N);
    render();
  });
  [nEl, rEl, bEl, msEl].forEach(el => el.addEventListener('input', render));
  [rEl, bEl].forEach(el => el.addEventListener('change', render));
  emptyBtn.addEventListener('click', () => { emptyBtn.classList.toggle('on'); render(); });
  v2Btn.addEventListener('click', () => { v3 = false; setOn([v2Btn, v3Btn], v2Btn); render(); });
  v3Btn.addEventListener('click', () => { v3 = true; setOn([v2Btn, v3Btn], v3Btn); render(); });
  nEl.max = +nnEl.value;
  render();
}
