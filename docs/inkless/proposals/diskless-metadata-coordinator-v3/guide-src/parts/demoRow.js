/* ---------------------------------------------------------------- the broker row */
function demoRow() {
  const rowEl = $('#rw-row');
  if (!rowEl) return;
  const eEl = $('#rw-e'), sEl = $('#rw-s');
  let st;
  const reset = () => {
    st = { row: null, overlay: [], curE: 7, nextS: 0, lastSent: null, n: 0 };
    eEl.value = 7; sEl.value = 0;
    render();
    show($('#rw-readout'), '', 'Send a commit to start.');
  };
  const effective = () => {
    const base = st.row ? { ...st.row } : { e: null, last: null, result: null, fenced: null };
    for (const o of st.overlay) {
      if (o.kind === 'commit') { base.last = o.p; base.result = o.result; base.e = Math.max(base.e ?? o.p[0], o.p[0]); }
      if (o.kind === 'retire') base.e = Math.max(base.e ?? o.e, o.e);
    }
    return base;
  };
  const floorOf = r => maxPair(r.last, r.fenced);
  const render = () => {
    const r = st.row;
    rowEl.innerHTML = r
      ? `<dt>epoch</dt><dd>${r.e}</dd><dt>last commit</dt><dd>${pair(r.last)}</dd><dt>its result</dt><dd>${r.result || 'none'}</dd>` +
        `<dt>fenced through</dt><dd>${pair(r.fenced)}</dd><dt>floor</dt><dd><b>${pair(floorOf(r))}</b></dd>`
      : '<dt>row</dt><dd>none yet: broker 3 has never committed here</dd>';
    const ov = $('#rw-overlay');
    ov.innerHTML = st.overlay.length
      ? '<ol style="margin:0;padding-left:1.1rem">' + st.overlay.map(o => o.kind === 'commit'
        ? `<li>file committed <code>${pair(o.p)}</code>: ${o.result}</li>`
        : `<li>broker epoch retired: <code>retiredThrough</code> ${o.e - 1}</li>`).join('') + '</ol>'
      : '<span class="muted">Empty: everything is applied to SQLite.</span>';
    $('#rw-repl').disabled = !st.overlay.length;
  };
  const send = p => {
    st.lastSent = p;
    const r = effective();
    const pending = st.overlay.some(o => o.kind === 'commit' && cmpPair(o.p, p) === 0);
    const fl = floorOf(r);
    const out = $('#rw-readout');
    if (r.e != null && p[0] < r.e) {
      show(out, 'bad', `<b>Stale epoch.</b> <code>${pair(p)}</code> is from epoch ${p[0]}, and the row is at epoch ${r.e}. The leader rejects it. A broker never goes back to an old epoch, so this part comes from before a restart.`);
    } else if (r.last && cmpPair(p, r.last) === 0) {
      if (pending) show(out, 'warn', `<b>Retry of a pending commit.</b> <code>${pair(p)}</code> matches the last commit, which is still in the overlay. The leader appends nothing, waits for that entry to replicate, and replies with its result.`);
      else show(out, 'good', `<b>Retry.</b> <code>${pair(p)}</code> matches the last commit, which is applied. The leader replies at once with the stored result, <i>${r.result}</i>, and appends nothing. This works even after retention removed those batches.`);
    } else if (!fl || cmpPair(p, fl) > 0) {
      st.n++;
      const result = `clicks-1 offsets ${100 + st.n * 10}–${109 + st.n * 10}`;
      st.overlay.push({ kind: 'commit', p, result });
      const gap = r.last && r.last[0] === p[0] && p[1] > r.last[1] + 1;
      show(out, '', `<b>New commit.</b> <code>${pair(p)}</code> is above the floor ${pair(fl)}${gap ? '. The gap is fine: files that failed to upload used those sequences' : ''}. ` +
        `The leader checks the batches, appends a file-committed record, and keeps it in the overlay until replication confirms it.`);
    } else {
      show(out, 'bad', `<b>Stale sequence.</b> <code>${pair(p)}</code> is at or below the floor ${pair(fl)}. The leader rejects it and tells the broker the floor. This late part can never commit here.`);
    }
    render();
  };
  $('#rw-next').addEventListener('click', () => {
    const p = [st.curE, st.nextS++];
    eEl.value = p[0]; sEl.value = p[1];
    send(p);
  });
  $('#rw-retry').addEventListener('click', () => {
    if (!st.lastSent) { show($('#rw-readout'), 'warn', 'Nothing to retry yet. Press <b>Next file</b> first.'); return; }
    eEl.value = st.lastSent[0]; sEl.value = st.lastSent[1];
    send(st.lastSent);
  });
  $('#rw-old').addEventListener('click', () => {
    const r = effective();
    if (!r.last) { show($('#rw-readout'), 'warn', 'Commit a file first, so there\'s an older file to be late.'); return; }
    const p = r.last[1] > 0 ? [r.last[0], r.last[1] - 1] : [r.last[0] - 1, 9];
    eEl.value = p[0]; sEl.value = p[1];
    send(p);
  });
  $('#rw-restart').addEventListener('click', () => {
    st.curE++;
    st.nextS = 0;
    st.overlay.push({ kind: 'retire', e: st.curE });
    eEl.value = st.curE; sEl.value = 0;
    render();
    show($('#rw-readout'), '', `Broker 3 restarted and registered with epoch ${st.curE}. Once cp1's leader sees the new registration in its metadata image, it appends a broker-epoch-retired record with <code>retiredThrough</code> ${st.curE - 1}. When it applies, every replica rejects epoch ${st.curE - 1} and older from the same log position. Sequences start over in the new epoch.`);
  });
  $('#rw-send').addEventListener('click', () => send([Math.floor(+eEl.value), Math.floor(+sEl.value)]));
  $('#rw-repl').addEventListener('click', () => {
    const r = effective();
    st.row = { e: r.e, last: r.last, result: r.result, fenced: st.row ? st.row.fenced : null };
    const n = st.overlay.length;
    st.overlay = [];
    render();
    show($('#rw-readout'), 'good', `The high watermark passed ${n === 1 ? 'the pending entry' : `all ${n} pending entries`}. The leader applied ${n === 1 ? 'it' : 'them'} to SQLite in log order, updated the row, and replied.`);
  });
  $('#rw-reset').addEventListener('click', reset);
  reset();
}
