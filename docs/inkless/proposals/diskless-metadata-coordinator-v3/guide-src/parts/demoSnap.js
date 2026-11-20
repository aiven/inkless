/* ---------------------------------------------------------------- snapshots */
function demoSnap() {
  const view = $('#sn-view');
  if (!view) return;
  const outBtn = $('#sn-f1out');
  const names = ['Leader', 'Follower 1', 'Follower 2'];
  let st;
  const reset = () => {
    st = { applied: 1000, logStart: 0, snaps: [[], [], []], reported: [null, null, null], f1out: false };
    outBtn.classList.remove('on');
    outBtn.textContent = 'Follower 1 leaves the in-sync set';
    update('Every replica has applied 1,000 records. Nobody has a snapshot yet, so the log start stays at 0.');
  };
  const inSync = i => !(i === 1 && st.f1out);
  const evaluate = () => {
    const leaderOlder = st.snaps[0].length === 2 ? st.snaps[0][0] : null;
    const reports = [1, 2].filter(inSync).map(i => st.reported[i]).filter(x => x != null);
    const best = reports.length ? Math.max(...reports) : null;
    const before = st.logStart;
    if (leaderOlder != null && best != null) st.logStart = Math.max(st.logStart, Math.min(leaderOlder, best));
    return { leaderOlder, best, before };
  };
  const update = msg => {
    const ev = evaluate();
    const scale = Math.max(st.applied, 1000);
    view.innerHTML = '';
    for (let i = 0; i < 3; i++) {
      const track = h('div', { class: 'bar-track' },
        h('div', { class: 'bar-seg', style: { left: '0', width: (st.logStart / scale) * 100 + '%', background: 'repeating-linear-gradient(45deg, var(--line) 0 6px, transparent 6px 12px)', color: 'var(--muted)' } }, st.logStart ? 'deleted' : ''),
        h('div', { class: 'bar-seg', style: { left: (st.logStart / scale) * 100 + '%', width: ((st.applied - st.logStart) / scale) * 100 + '%', background: 'color-mix(in srgb, var(--accent) 30%, transparent)', color: 'var(--ink)' } }, 'log'),
        ...st.snaps[i].map(s => h('div', { class: 'bar-seg', title: 'snapshot at ' + fmt(s), style: { left: `calc(${(s / scale) * 100}% - 2px)`, width: '4px', background: 'var(--cp2)' } })));
      const note = st.snaps[i].length ? 'snapshots at ' + st.snaps[i].map(fmt).join(' and ') : 'no snapshot';
      const rep = i === 0 ? '' : st.reported[i] == null ? ' · nothing reported' : inSync(i) ? ` · reported ${fmt(st.reported[i])}` : ' · out of sync, report ignored';
      view.append(h('div', { class: 'bar-row', style: { gridTemplateColumns: '6.5rem 1fr' } },
        h('span', { class: 'small', style: { fontWeight: 650 } }, names[i]),
        h('div', null, track, h('div', { class: 'small muted' }, note + rep))));
    }
    view.append(h('div', { class: 'small muted', style: { marginTop: '0.4rem' } }, `Applied: ${fmt(st.applied)} records. Log start: ${fmt(st.logStart)}. Pink marks are snapshots.`));
    let why;
    if (ev.leaderOlder == null) why = `The leader has ${st.snaps[0].length ? 'only one snapshot' : 'no snapshot'}. It needs two, because it uses its older one.`;
    else if (ev.best == null) why = `The leader's older snapshot is at ${fmt(ev.leaderOlder)}, but no in-sync follower has reported a snapshot.`;
    else why = `The leader's older snapshot is at ${fmt(ev.leaderOlder)}. The best in-sync follower report is ${fmt(ev.best)}. The log start may move to the lower of the two, ${fmt(Math.min(ev.leaderOlder, ev.best))}${Math.min(ev.leaderOlder, ev.best) < ev.before ? ', but it never moves back' : ''}.`;
    const movedText = st.logStart > ev.before ? ` <b class="good">The leader moved the log start from ${fmt(ev.before)} to ${fmt(st.logStart)}</b>, and Kafka deletes the old segments.` : '';
    show($('#sn-readout'), st.logStart > ev.before ? 'good' : '', `${msg ? msg + ' ' : ''}${why}${movedText}`);
  };
  const snap = i => {
    const list = st.snaps[i];
    if (list.length && list[list.length - 1] === st.applied) { update(`${names[i]} already has a snapshot at ${fmt(st.applied)}. Append more records first.`); return; }
    list.push(st.applied);
    if (list.length > 2) list.shift();
    if (i > 0) st.reported[i] = st.applied;
    update(`${names[i]} copied its SQLite at offset ${fmt(st.applied)}${i > 0 ? ' and reported it to the leader' : ''}. It keeps ${list.length === 2 ? 'this one and one older' : 'this one'}.`);
  };
  $('#sn-append').addEventListener('click', () => { st.applied += 1000; update('Every replica applied 1,000 more records.'); });
  $('#sn-L').addEventListener('click', () => snap(0));
  $('#sn-F1').addEventListener('click', () => snap(1));
  $('#sn-F2').addEventListener('click', () => snap(2));
  outBtn.addEventListener('click', () => {
    st.f1out = !st.f1out;
    outBtn.classList.toggle('on', st.f1out);
    outBtn.textContent = st.f1out ? 'Follower 1 rejoins the in-sync set' : 'Follower 1 leaves the in-sync set';
    update(st.f1out
      ? 'Follower 1 dropped out of the in-sync set. Its report no longer counts, because it may have lost its files meanwhile.'
      : 'Follower 1 caught up and rejoined. It reports its newest snapshot again.');
  });
  $('#sn-reset').addEventListener('click', reset);
  reset();
}
