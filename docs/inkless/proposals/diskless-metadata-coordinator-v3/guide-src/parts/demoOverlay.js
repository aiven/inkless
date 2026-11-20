/* ---------------------------------------------------------------- the overlay */
function demoOverlay() {
  const gantt = $('#ov-gantt');
  if (!gantt) return;
  const COUNT = 8, R = 8, LINGER = 4, PER = 10;
  const arr = i => 2 * i;
  const btnWait = $('#ov-wait'), btnOv = $('#ov-overlay'), sel = $('#ov-fail');
  let mode = 'overlay';

  const batchesFor = () => {
    const out = [];
    let i = 0;
    while (i < COUNT) {
      const open = arr(i), close = open + LINGER, members = [];
      while (i < COUNT && arr(i) <= close) members.push(i++);
      out.push({ open, close, members });
    }
    return out;
  };

  const fillSelect = () => {
    sel.innerHTML = '<option value="-1">nothing</option>';
    if (mode === 'wait') {
      for (let i = 0; i < COUNT; i++) sel.append(h('option', { value: i }, `commit ${i}`));
    } else {
      batchesFor().forEach((b, k) => sel.append(h('option', { value: k }, `append ${k + 1} (commits ${b.members[0]}–${b.members[b.members.length - 1]})`)));
    }
  };

  const simulate = fail => {
    const rows = [];
    if (mode === 'wait') {
      let prevDone = 0, nextOff = 0;
      for (let i = 0; i < COUNT; i++) {
        const start = Math.max(arr(i), prevDone), done = start + R, failed = i === fail;
        rows.push({ i, wait: [arr(i), start], work: [start, done], offs: [nextOff, nextOff + PER - 1], failed, done,
          checked: i === 0 ? 'SQLite (empty)' : 'SQLite only, after the previous commit finished' });
        if (!failed) nextOff += PER;
        prevDone = done;
      }
      return { rows, appends: COUNT };
    }
    const batches = batchesFor();
    const failAt = fail >= 0 ? batches[fail].close + R : Infinity;
    const doneOf = [];
    let prevDone = 0, spec = 0, reverted = false;
    batches.forEach((b, k) => {
      b.dropped = k >= fail && fail >= 0 && b.open < failAt;
      if (!b.dropped) { b.done = Math.max(b.close + R, prevDone); prevDone = b.done; }
      doneOf[k] = b.dropped ? failAt : b.done;
    });
    for (let k = 0; k < batches.length; k++) {
      const b = batches[k];
      for (const i of b.members) {
        if (!b.dropped && fail >= 0 && k > fail && !reverted) {
          reverted = true;
          spec = PER * batches.slice(0, fail).reduce((a, x) => a + x.members.length, 0);
        }
        const appliedBefore = batches.filter((x, j) => j < k && !x.dropped && x.done <= arr(i)).reduce((a, x) => a + x.members.length, 0);
        const earlier = i - batches.filter((x, j) => j < k && x.dropped && failAt <= arr(i)).reduce((a, x) => a + x.members.length, 0);
        const inOverlay = Math.max(0, earlier - appliedBefore);
        rows.push({ i, wait: [arr(i), b.close], work: [b.close, doneOf[k]], offs: [spec, spec + PER - 1], failed: b.dropped, done: doneOf[k], batch: k,
          checked: `SQLite (${appliedBefore} commit${appliedBefore === 1 ? '' : 's'}) + overlay (${inOverlay})`, reused: reverted && !b.dropped });
        spec += PER;
      }
    }
    return { rows, appends: batches.length, batches, failAt };
  };

  const render = () => {
    const fail = +sel.value;
    const sim = simulate(fail);
    const maxT = Math.ceil(Math.max(...sim.rows.map(r => r.done)) / 8) * 8;
    drawGantt(gantt, sim.rows.map(r => ({
      label: `commit ${r.i}`,
      segs: [
        { from: r.wait[0], to: r.wait[1], color: 'var(--line)', ink: 'var(--muted)', text: r.wait[1] > r.wait[0] ? (mode === 'wait' ? 'waits' : 'linger') : '' },
        { from: r.work[0], to: r.work[1], color: r.failed ? 'var(--bad)' : 'var(--accent)', text: r.failed ? 'failed' : 'replicating' },
      ],
    })), maxT, 8, ' ms');
    const tb = $('#ov-table');
    tb.innerHTML = '';
    for (const r of sim.rows) {
      tb.append(h('tr', null,
        h('td', null, `${r.i} (arrives at ${r.wait[0]} ms)`),
        h('td', { class: 'mono', style: r.failed ? { textDecoration: 'line-through', color: 'var(--bad)' } : null }, `${r.offs[0]}–${r.offs[1]}`),
        h('td', { class: 'small' }, r.checked),
        h('td', { html: r.failed ? `${chip('bad', 'retriable error')} at ${r.done} ms` : `${chip('good', 'committed')} reply at ${r.done} ms` })));
    }
    const last = Math.max(...sim.rows.filter(r => !r.failed).map(r => r.done), 0);
    const failedRows = sim.rows.filter(r => r.failed);
    let text;
    if (mode === 'wait') {
      text = `Each commit waits for the previous one to replicate before it can even be checked. The last reply comes at <b>${last} ms</b>, after ${sim.appends} appends and ${sim.appends} disk syncs.`;
      if (failedRows.length) text += ` Commit ${fail} failed, and only commit ${fail} is affected: the next commit was checked after the failure, so it took the freed offsets.`;
    } else {
      text = `The leader checks each commit against SQLite plus the overlay, so commits don't wait for each other. ${sim.appends} appends carry all ${COUNT} commits. The last reply comes at <b>${last || '–'} ms</b> instead of 64 ms.`;
      if (failedRows.length) {
        const ids = failedRows.map(r => r.i);
        text += ` Append ${fail + 1} failed after its round trip, at ${sim.failAt} ms. Commits ${ids[0]}–${ids[ids.length - 1]} were checked against it, so the leader drops all of them and they get a retriable error.`;
        const reused = sim.rows.filter(r => r.reused);
        if (reused.length) text += ` Commits ${reused.map(r => r.i).join(' and ')} arrived after the failure, so the leader checks them against the reverted state and they take the freed offsets. That's safe: no dropped offset was ever applied or acknowledged.`;
      }
    }
    show($('#ov-readout'), failedRows.length ? 'warn' : '', text);
  };

  btnWait.addEventListener('click', () => { mode = 'wait'; setOn([btnWait, btnOv], btnWait); fillSelect(); render(); });
  btnOv.addEventListener('click', () => { mode = 'overlay'; setOn([btnWait, btnOv], btnOv); fillSelect(); render(); });
  sel.addEventListener('change', render);
  fillSelect();
  render();
}
