/* ---------------------------------------------------------------- fence and part race */
function demoRace() {
  const stateEl = $('#rc-state');
  if (!stateEl) return;
  let st;
  const reset = () => {
    st = { last: [7, 3], fenced: null, holds: [2, 3], log: [] };
    render();
    show($('#rc-readout'), '', 'Pick an order.');
    $('#rc-fence-first').disabled = false;
    $('#rc-part-first').disabled = false;
  };
  const floor = () => maxPair(st.last, st.fenced);
  const render = () => {
    stateEl.innerHTML = `<dl class="kv"><dt>last commit</dt><dd>${pair(st.last)}</dd><dt>fenced through</dt><dd>${pair(st.fenced)}</dd>` +
      `<dt>floor</dt><dd><b>${pair(floor())}</b></dd><dt>holds batches from</dt><dd>sequences ${st.holds.join(', ')}</dd></dl>`;
    $('#rc-log').innerHTML = st.log.length ? '<ol style="margin:0;padding-left:1.1rem">' + st.log.map(l => `<li>${l}</li>`).join('') + '</ol>' : '<span class="muted">Nothing yet.</span>';
  };
  const part = () => {
    const p = [7, 5];
    if (cmpPair(p, floor()) > 0) {
      st.last = p;
      st.holds = [...st.holds, 5].sort((a, b) => a - b);
      st.log.push(`Part <code>(7, 5)</code> is above the floor, so it commits. cp1 now holds batches from sequence 5.`);
      return true;
    }
    st.log.push(`Part <code>(7, 5)</code> is at or below the floor ${pair(floor())}. Rejected as a stale sequence: it can never commit here.`);
    return false;
  };
  const fence = () => {
    st.fenced = maxPair(st.fenced, [7, 6]);
    const held = st.holds.filter(s => s >= 0 && s <= 6);
    const released = [0, 1, 2, 3, 4, 5, 6].filter(s => !held.includes(s));
    st.log.push(`cp1 appends sequences-fenced for epoch 7, range [0, 6]. Fenced-through becomes <code>(7, 6)</code>. It answers: holds {${held.join(', ')}}, released {${released.join(', ')}}.`);
    return { held, released };
  };
  $('#rc-fence-first').addEventListener('click', () => {
    reset();
    const f = fence();
    const ok = part();
    render();
    show($('#rc-readout'), 'good', `The fence went first, so sequence 5 is released and the late part is stale. The tracking partition was told sequence 5 is released${ok ? '' : ', and that answer stays true forever'}. Referenced: {${f.held.join(', ')}}.`);
    $('#rc-fence-first').disabled = true; $('#rc-part-first').disabled = true;
  });
  $('#rc-part-first').addEventListener('click', () => {
    reset();
    part();
    const f = fence();
    render();
    show($('#rc-readout'), 'good', `The part went first and committed, so the fence's answer includes it: referenced {${f.held.join(', ')}}. The tracking partition waits for a release of sequence 5 before it deletes that object. Either order gives a true answer.`);
    $('#rc-fence-first').disabled = true; $('#rc-part-first').disabled = true;
  });
  $('#rc-reset').addEventListener('click', reset);
  reset();
}
