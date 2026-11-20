/* ---------------------------------------------------------------- finish reports */
function demoV3Empty() {
  const host = $('#et-parts');
  if (!host) return;
  const btns = { v2: $('#et-v2'), v3: $('#et-v3'), v3x: $('#et-v3x') };
  let mode = 'v2';
  const init = () => ({
    cps: [
      { state: 'waiting', note: 'participant part for (7, 41)' },
      { state: 'waiting', note: 'participant part for (7, 41)' },
      { state: 'waiting', note: 'empty tracking part' },
    ],
    outcome: '',
  });
  const send = { title: 'The file sends three parts', text: 'The broker uploads the file and sends parts to cp0, cp1, and cp2. Cp2 tracks the file but holds no batches, so its part is empty.' };
  const recorded = { title: 'The empty tracking part records', text: 'Cp2 records the part, and its own entry releases in the same apply. That release triggers no fence. The entries for cp0 and cp1 are pending: they wait for a finish report that covers sequence 41.',
    run: s => { s.cps[2].state = 'recorded'; s.cps[2].note = 'own entry released; cp0 and cp1 pending'; s.cps[0].state = 'pending'; s.cps[0].note = 'part in flight; no fence'; s.cps[1].state = 'pending'; s.cps[1].note = 'part in flight; no fence'; } };
  const steps = {
    v2: [
      send,
      { title: 'The empty tracking part applies', text: 'cp2 applies the empty part and releases its own state at once. It hasn\'t heard from cp0 or cp1, and every partition it has heard from is released, so the obvious rule fences both.',
        run: s => { s.cps[2].state = 'released'; s.cps[2].note = 'released in the empty-part apply; fences sent'; s.cps[0].state = 'fence sent'; s.cps[0].note = 'waiting for the participant fence'; s.cps[1].state = 'fence sent'; s.cps[1].note = 'waiting for the participant fence'; } },
      { title: 'cp0 wins the race; cp1 changes leader', text: 'Cp0\'s part arrives before the fence and commits, so the fence answers referenced. The file still paid one fence request and an fsynced participant-state record. Cp1 is in a leader change: its part waits for the new leader, while the fence from cp2 reaches the queue first.',
        run: s => { s.cps[0].state = 'committed'; s.cps[0].note = 'part committed first; fence answers referenced'; s.cps[1].state = 'failover'; s.cps[1].note = 'part waits behind leader recovery'; } },
      { title: 'The queued cp1 part arrives', text: 'The participant part reaches cp1 after the fence. Its pair is at the floor, so cp1 returns a non-retriable stale-sequence error. The produce fails instead of waiting through the failover.',
        run: s => { s.cps[1].state = 'stale'; s.cps[1].note = 'part rejected at (7, 41)'; s.outcome = 'bad'; } },
    ],
    v3: [
      send,
      recorded,
      { title: 'cp0 commits; cp1 changes leader', text: 'Cp0\'s part commits. Cp1\'s leader changes, so its part gets <code>NOT_COORDINATOR</code>, and the broker retries it at the new leader. Nothing at cp2 has fenced cp1.',
        run: s => { s.cps[0].state = 'committed'; s.cps[0].note = 'recorded file-committed: answered'; s.cps[1].state = 'failover'; s.cps[1].note = 'NOT_COORDINATOR; the broker retries'; } },
      { title: 'The retry commits within the deadline', text: 'The new cp1 leader commits the part with the same pair. Every part is finished, so the file is finished, and the produce request succeeds.',
        run: s => { s.cps[1].state = 'committed'; s.cps[1].note = 'committed at the new leader: answered'; s.outcome = 'good'; } },
      { title: 'The next tracking part carries the finish report', text: 'The broker\'s next tracking part to cp2 carries <code>finishedThrough</code> 41 with no exception ranges. Cp2 moves cp0 and cp1 from pending to held. It sends no fence and waits for their release reports.',
        run: s => { s.cps[2].state = 'held'; s.cps[2].note = 'finishedThrough 41, no exceptions: cp0 and cp1 held'; s.cps[0].note = 'held: owes a release report'; s.cps[1].note = 'held: owes a release report'; } },
      { title: 'Retention releases the object', text: 'Later, retention removes the batches at cp0 and cp1. Each sends a release report, cp2 moves both entries to released, and it queues the object for deletion.',
        run: s => { s.cps[0].state = 'released'; s.cps[0].note = 'release report accepted'; s.cps[1].state = 'released'; s.cps[1].note = 'release report accepted'; s.cps[2].state = 'queued'; s.cps[2].note = 'every entry released: object queued'; } },
    ],
    v3x: [
      send,
      recorded,
      { title: 'cp0 commits; cp1 stays unavailable', text: 'Cp0\'s part commits. Cp1 has no leader, so every attempt gets <code>NOT_COORDINATOR</code>. The broker keeps retrying until the commit deadline.',
        run: s => { s.cps[0].state = 'committed'; s.cps[0].note = 'recorded file-committed: answered'; s.cps[1].state = 'retrying'; s.cps[1].note = 'NOT_COORDINATOR on every attempt'; } },
      { title: 'The commit deadline passes', text: 'Cp1\'s part is finished without a leader answer, so it is an exception. The request gets <code>KAFKA_STORAGE_ERROR</code> for cp1\'s batches and its own results for cp0\'s batches. The producer retries the failed partitions.',
        run: s => { s.cps[1].state = 'exception'; s.cps[1].note = 'finished at the deadline; outcome unknown'; s.outcome = 'warn'; } },
      { title: 'The finish report lists the exception', text: 'The next tracking part carries <code>finishedThrough</code> 41 with the exception range [41, 41] for cp1. Cp2 moves cp0 to held and cp1 to fencing, and it sends cp1 one range fence. The broker finished the file, so no live part can still arrive.',
        run: s => { s.cps[2].state = 'fencing'; s.cps[2].note = 'cp0 held; range fence [41, 41] to cp1'; s.cps[1].state = 'fence sent'; s.cps[1].note = 'range fence up to (7, 41)'; } },
      { title: 'The fence answers', text: 'Cp1\'s new leader applies the fence and raises the broker\'s floor to (7, 41), so the old part can never commit there. The part never committed, so the answer is "released," and cp1\'s entry is released. Had it committed, the answer would be "referenced," and cp1 would release later.',
        run: s => { s.cps[1].state = 'released'; s.cps[1].note = 'floor (7, 41); answer: released'; s.cps[2].state = 'held'; s.cps[2].note = 'cp0 held; cp1 released'; s.outcome = 'good'; } },
    ],
  };
  const stateClass = st => st === 'stale' ? 'bad'
    : ['committed', 'released', 'recorded', 'held', 'queued'].includes(st) ? 'good'
    : ['fence sent', 'failover', 'retrying', 'exception', 'fencing'].includes(st) ? 'warn' : 'info';
  const panel = (c, i, changed) => h('div', { class: 'panel' + (changed ? ' changed' : '') },
    h('h6', null, h('span', { html: cpChip(i) }), h('span', { class: 'chip ' + stateClass(c.state) }, c.state)),
    h('div', { class: 'small' }, c.note));
  const readout = s => {
    if (mode === 'v2') return s.outcome === 'bad' ? 'Obvious rule: the delayed cp1 part gets stale-sequence, so the produce fails although nothing was wrong with it.' : 'Obvious rule: step through the empty tracking part and the failover.';
    if (mode === 'v3') return s.outcome === 'good' ? 'Finish reports: the produce succeeded, and cp2 sent no fence. A named test checks exactly this.' : 'Finish reports: the tracking partition waits for a finish report before it decides anything.';
    return s.outcome === 'good' ? 'Finish reports: the only fence went to the participant that the broker reported as an exception, after the file finished. It answered released.'
      : s.outcome === 'warn' ? 'Finish reports: cp1\'s batches failed at the deadline, as any unconfirmed part does. A fence comes only after the finish report.'
      : 'Finish reports: the tracking partition waits for a finish report before it decides anything.';
  };
  const ctl = stepper({
    prev: $('#et-prev'), next: $('#et-next'), reset: $('#et-reset'),
    init, steps: () => steps[mode],
    render: (s, prev, step, i, n) => {
      $('#et-count').textContent = `Step ${i + 1} of ${n}`;
      $('#et-narr').innerHTML = `<h6>${step.title} ${mode === 'v2' ? chip('bad', 'obvious rule') : chip('good', 'the design')}</h6><p>${step.text}</p>`;
      host.innerHTML = '';
      s.cps.forEach((c, k) => host.append(panel(c, k, prev && JSON.stringify(prev.cps[k]) !== JSON.stringify(c))));
      show($('#et-readout'), s.outcome, readout(s));
    },
  });
  for (const [key, btn] of Object.entries(btns)) {
    btn.addEventListener('click', () => { mode = key; setOn(Object.values(btns), btn); ctl.go(0); });
  }
}
