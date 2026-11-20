/* ---------------------------------------------------------------- replication guard */
function demoV3Rack() {
  const racksEl = $('#rk-racks');
  if (!racksEl) return;
  const rfEl = $('#rk-rf'), overrideBtn = $('#rk-override'), boxes = $('#rk-boxes');
  const brokers = ['b1', 'b2', 'b3', 'b4', 'b5', 'b6'];
  const layouts = {
    3: ['a', 'b', 'c', 'a', 'b', 'c'],
    late: ['a', 'b', 'a', 'c', 'b', 'c'],
    2: ['a', 'b', 'a', 'b', 'a', 'b'],
    1: ['a', 'a', 'a', 'a', 'a', 'a'],
    0: [null, null, null, null, null, null],
  };
  const current = [0, 1, 2];
  const target = new Set([0, 3, 1]);
  const labels = brokers.map((b, i) => {
    const cb = h('input', { type: 'checkbox', 'aria-label': `${b} in the target replica set` });
    cb.checked = target.has(i);
    cb.addEventListener('change', () => { if (cb.checked) target.add(i); else target.delete(i); render(); });
    const text = h('span', { class: 'mono' }, b);
    boxes.append(h('label', null, cb, text));
    return text;
  });
  const spread = (set, racks) => (racks[0] === null ? set.length : new Set(set.map(i => racks[i])).size);
  const describe = (set, racks) => set.map(i => brokers[i] + (racks[i] ? ` (${racks[i]})` : '')).join(', ');
  const render = () => {
    const racks = layouts[racksEl.value];
    labels.forEach((el, i) => { el.textContent = brokers[i] + (racks[i] ? ` (${racks[i]})` : ''); });
    const distinct = racks[0] === null ? brokers.length : new Set(racks).size;
    const maximal = Math.min(3, distinct);
    const t = [...target].sort((a, b) => a - b);
    const cur = spread(current, racks), tgt = spread(t, racks);
    let cls, verdict, why;
    if (t.length !== 3) {
      cls = 'bad'; verdict = 'INVALID_REPLICATION_FACTOR';
      why = `The target has ${t.length} replica${t.length === 1 ? '' : 's'}. The replica count is fixed at creation, so the controller rejects the change before it checks racks.`;
    } else if (tgt < cur && tgt < maximal) {
      cls = 'bad'; verdict = 'INVALID_REPLICA_ASSIGNMENT';
      why = `The target spans ${tgt} rack${tgt === 1 ? '' : 's'}, fewer than the current ${cur} and fewer than maximal spread, ${maximal}, so the controller rejects it.`;
    } else {
      cls = 'good'; verdict = 'accepted';
      why = tgt > cur ? `The target widens the spread from ${cur} to ${tgt} rack${tgt === 1 ? '' : 's'}.` : 'The target keeps the spread, so the move passes, including a move within a rack to decommission a broker.';
    }
    $('#rk-current').innerHTML = `<h6>Current replicas ${chip(cur < maximal ? 'warn' : 'info', `spans ${cur} of ${maximal}`)}</h6>` +
      `<p class="small" style="margin:0">${describe(current, racks)}. Maximal spread here is <code>min(3, ${distinct})</code> = ${maximal}.${racks[0] === null ? ' With no rack declared, spread counts distinct brokers.' : ''}</p>`;
    $('#rk-target').innerHTML = `<h6>Target ${chip(cls, verdict)}</h6><p class="small" style="margin:0">${t.length ? describe(t, racks) : 'no broker selected'}${t.length === 3 ? `, spanning ${tgt} rack${tgt === 1 ? '' : 's'}` : ''}.</p>`;
    const rf = +rfEl.value, override = overrideBtn.classList.contains('on');
    const startup = $('#rk-startup');
    if (rf >= 3) { startup.className = 'chip good'; startup.textContent = 'startup passes'; }
    else if (override) { startup.className = 'chip warn'; startup.textContent = 'starts with a warning; no clause 2 exception'; }
    else { startup.className = 'chip bad'; startup.textContent = 'startup fails: below 3'; }
    show($('#rk-readout'), cls === 'bad' ? 'bad' : cur < maximal ? 'warn' : 'good',
      why + (cur < maximal ? ` The current set is below maximal spread because rack c registered after creation, so <code>CoordinatorPartitionsBelowRackSpread</code> counts this partition until a reassignment restores it.` : '') +
      (maximal < 3 ? ` With fewer racks than the replication factor, creation still goes ahead and logs a warning.` : ''));
  };
  [racksEl, rfEl].forEach(el => el.addEventListener('change', render));
  overrideBtn.addEventListener('click', () => { overrideBtn.classList.toggle('on'); render(); });
  render();
}
