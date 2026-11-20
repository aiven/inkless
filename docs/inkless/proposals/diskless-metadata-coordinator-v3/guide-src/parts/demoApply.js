/* ---------------------------------------------------------------- log, high watermark, SQLite */
function demoApply() {
  const host = $('#ap-replicas');
  if (!host) return;
  const names = ['Leader', 'Follower 1', 'Follower 2'];
  const parts = ['orders-0', 'clicks-1', 'logs-2', 'orders-1'];
  const isrBtn = $('#ap-f2isr');
  let st;
  let changed = new Set();

  const recs = (a, b) => (a === b ? `record ${a}` : `records ${a}–${b}`);
  const isr = () => (st.f2out ? [0, 1] : [0, 1, 2]);
  const hw = () => Math.min(...isr().map(i => st.leo[i]));
  const sync = () => {
    st.knownHw[0] = hw();
    for (let i = 0; i < 3; i++) st.applied[i] = st.wiped[i] ? 0 : Math.min(st.leo[i], st.knownHw[i]);
  };
  const reset = () => {
    st = { leo: [0, 0, 0], knownHw: [0, 0, 0], applied: [0, 0, 0], wiped: [false, false, false], f2out: false };
    isrBtn.classList.remove('on');
    isrBtn.textContent = 'Follower 2 leaves the in-sync set';
    changed = new Set();
    render();
    show($('#ap-readout'), '', 'The log is empty. Press <b>Leader appends a commit</b>.');
  };
  const fetch = i => {
    const from = st.leo[i], to = st.leo[0];
    const hwBefore = st.knownHw[0], knewBefore = st.knownHw[i];
    let text = '';
    if (st.wiped[i]) {
      const replay = Math.min(st.leo[i], st.knownHw[i]);
      text += `${names[i]} restarted, found no database, and rebuilt it by replaying ${replay ? recs(0, replay - 1) : 'nothing'} from its own log, with no help from the leader. `;
      st.wiped[i] = false;
    }
    st.leo[i] = to;
    sync();
    st.knownHw[i] = st.knownHw[0];
    sync();
    const hwAfter = st.knownHw[0];
    const inIsr = isr().includes(i);
    text += to > from ? `${names[i]} copied ${recs(from, to - 1)}. ` : `${names[i]} had nothing new to copy. `;
    let cls = '';
    if (hwAfter > hwBefore) {
      cls = 'good';
      text += `That moved the high watermark from ${hwBefore} to <b>${hwAfter}</b>, so the leader applied ${recs(hwBefore, hwAfter - 1)} and replied to those commits. ${names[i]} learned the new high watermark in this fetch's response and applied up to it too. `;
    } else if (st.knownHw[i] > knewBefore) {
      cls = 'good';
      text += `It learned the newer high watermark, <b>${hwAfter}</b>, from the response and applied up to it. `;
    } else if (to > hwAfter) {
      cls = 'warn';
      const lag = isr().filter(r => st.leo[r] === hwAfter && r !== 0).map(r => names[r].toLowerCase());
      text += inIsr
        ? `Nothing is applied yet: the high watermark stays at <b>${hwAfter}</b> because ${lag.join(' and ')} still ends at offset ${hwAfter}. The high watermark is the lowest log end among in-sync replicas. `
        : `It's out of the in-sync set, so the high watermark doesn't wait for it. `;
    }
    const stale = [1, 2].filter(r => r !== i && !st.wiped[r] && st.knownHw[r] < hwAfter && st.leo[r] > st.knownHw[r]);
    for (const r of stale) text += `${names[r]} still knows high watermark ${st.knownHw[r]}, so it applies the rest on its next fetch. `;
    changed = new Set([0, i]);
    render();
    show($('#ap-readout'), cls, text);
  };
  const render = () => {
    host.innerHTML = '';
    for (let i = 0; i < 3; i++) {
      const inIsr = isr().includes(i);
      const cells = [];
      for (let k = 0; k < st.leo[i]; k++) {
        cells.push(h('span', { class: 'cell' + (k >= st.applied[i] ? ' unapplied' : ''), style: { background: PART_COLORS[k % 4] }, title: `record ${k}: commit for ${parts[k % 4]}` }, k));
      }
      const counts = {};
      for (let k = 0; k < st.applied[i]; k++) counts[parts[k % 4]] = (counts[parts[k % 4]] || 0) + 10;
      const rows = st.wiped[i]
        ? '<dt>tables</dt><dd class="bad-text">database file deleted</dd>'
        : Object.keys(counts).length
          ? Object.entries(counts).map(([p, n]) => `<dt>${p}</dt><dd>next offset ${n}</dd>`).join('')
          : '<dt>tables</dt><dd>empty</dd>';
      host.append(h('div', { class: 'panel' + (changed.has(i) ? ' changed' : '') },
        h('h6', null, names[i], h('span', { html: (i === 0 ? chip('info', 'leader') + ' ' : '') + (inIsr ? chip('good', 'in sync') : chip('warn', 'out of sync')) })),
        cells.length ? h('div', { class: 'log' }, cells) : h('div', { class: 'small muted' }, 'Log empty'),
        h('div', { class: 'small muted', style: { margin: '0.4rem 0' } }, `log end ${st.leo[i]} · knows high watermark ${st.knownHw[i]}`),
        h('div', { class: 'small', style: { fontWeight: 650 } }, 'SQLite'),
        h('dl', { class: 'kv', html: `<dt>applied offset</dt><dd>${st.applied[i] ? st.applied[i] - 1 : 'none'}</dd>${rows}` })));
    }
  };

  $('#ap-append').addEventListener('click', () => {
    st.leo[0]++;
    sync();
    changed = new Set([0]);
    render();
    const off = st.leo[0] - 1;
    show($('#ap-readout'), 'warn',
      `The leader appended record ${off}, a commit for ${parts[off % 4]}. The high watermark is still ${st.knownHw[0]}, so nobody applies record ${off} yet, ` +
      `and the broker that sent this commit is still waiting for its answer. Let the followers fetch.`);
  });
  $('#ap-f1').addEventListener('click', () => fetch(1));
  $('#ap-f2').addEventListener('click', () => fetch(2));
  isrBtn.addEventListener('click', () => {
    if (!st.f2out) {
      st.f2out = true;
      isrBtn.classList.add('on');
      isrBtn.textContent = 'Follower 2 catches up and rejoins';
      sync();
      changed = new Set([0, 2]);
      render();
      show($('#ap-readout'), '',
        `The leader removed follower 2 from the in-sync set, for example because it stopped fetching. The high watermark no longer waits for it: it's now ${st.knownHw[0]}. ` +
        `With <code>min.insync.replicas=2</code>, the leader and follower 1 are enough to accept commits.`);
    } else {
      st.leo[2] = st.leo[0];
      st.f2out = false;
      isrBtn.classList.remove('on');
      isrBtn.textContent = 'Follower 2 leaves the in-sync set';
      sync();
      st.knownHw[2] = st.knownHw[0];
      sync();
      changed = new Set([0, 2]);
      render();
      show($('#ap-readout'), 'good', 'Follower 2 fetched everything it missed, so the leader added it back to the in-sync set. From now on, the high watermark waits for it again.');
    }
  });
  $('#ap-wipe').addEventListener('click', () => {
    if (st.wiped[1]) { show($('#ap-readout'), 'warn', 'Follower 1\'s database is already gone. Press <b>Follower 1 fetches</b> to restart it.'); return; }
    if (!st.applied[1]) { show($('#ap-readout'), 'warn', 'Follower 1 hasn\'t applied anything yet, so there\'s nothing to wipe. Append a few commits and let both followers fetch first.'); return; }
    st.wiped[1] = true;
    sync();
    changed = new Set([1]);
    render();
    show($('#ap-readout'), 'bad',
      `Follower 1's SQLite file is gone: its tables are empty. Its log still holds ${recs(0, st.leo[1] - 1)}, and the log is the truth. ` +
      `Press <b>Follower 1 fetches</b> to restart it. It rebuilds the database from its own log before it fetches anything new. The snapshot chapter shows how snapshots keep this replay short.`);
  });
  $('#ap-reset').addEventListener('click', reset);
  reset();
}
