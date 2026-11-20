/* ---------------------------------------------------------------- release entry states */
function demoV3Entry() {
  const v2El = $('#re-v2');
  if (!v2El) return;
  const v3El = $('#re-v3');
  const labels = {
    track: 'tracking part records', answered: 'finish report: answered', exception: 'finish report: exception', lease: 'lease ends',
    ref: 'fence answers "referenced"', rel: 'fence answers "released"', report: 'cp1 release report',
  };
  let s;
  const reset = () => {
    s = {
      v2: { state: 'none', note: 'No entry yet: the tracking part hasn\'t recorded.', dropped: false, overwritten: false },
      v3: { state: 'none', note: 'No entry yet: the tracking part hasn\'t recorded.', fences: 0 },
      history: [],
    };
  };
  const stepV2 = (m, ev) => {
    switch (ev) {
      case 'track':
        if (m.state === 'none') { m.state = 'waiting'; m.note = m.dropped ? 'The entry waits for a release that cp1 already sent and never resends.' : 'The entry waits for cp1.'; }
        else m.note = 'Duplicate: no change.';
        break;
      case 'answered': case 'exception':
        m.note = 'The naive tracker ignores finish reports, so this event does nothing.';
        break;
      case 'lease':
        m.note = m.state === 'released' ? 'No change.' : 'The lease end sends a fence to cp1.';
        break;
      case 'ref':
        if (m.state === 'released') m.overwritten = true;
        m.state = 'referenced';
        m.note = m.overwritten ? '"Referenced" overwrote "released." Cp1 already reported and doesn\'t report again.' : 'Cp1 holds batches.';
        break;
      case 'rel':
        m.state = 'released'; m.note = 'The fence answered released.';
        break;
      case 'report':
        if (m.state === 'none') { m.dropped = true; m.note = 'No entry exists, so the naive tracker drops the report. Cp1 appends release-accepted and never resends.'; }
        else if (m.state === 'released') m.note = 'Duplicate: no change.';
        else { m.state = 'released'; m.note = 'Release report recorded.'; }
        break;
    }
  };
  const stepV3 = (m, ev) => {
    const st = m.state;
    if (ev === 'track') {
      if (st === 'none') { m.state = 'pending'; m.note = 'The participant set is recorded. Cp1 waits for a finish report.'; }
      else if (st === 'released') m.note = 'The tracking part merges with the stored entry, and cp1 stays released.';
      else m.note = 'Duplicate: no change.';
      return;
    }
    if (st === 'released') { m.note = 'Released is terminal: no change.'; return; }
    switch (ev) {
      case 'answered':
        if (st === 'pending') { m.state = 'held'; m.note = 'Cp1 answered, so it holds batches and owes a release report. No fence.'; }
        else m.note = st === 'none' ? 'No participant set is recorded yet, so the report changes nothing here.' : 'No change.';
        break;
      case 'exception':
        if (st === 'pending') { m.state = 'fencing'; m.fences++; m.note = 'Cp1 is an exception, so the tracking partition sends a range fence.'; }
        else if (st === 'held') m.note = 'Held never goes back. The tracking partition skips the fence.';
        else m.note = st === 'none' ? 'No participant set is recorded yet, so the report changes nothing here.' : 'No change: a fence is already out.';
        break;
      case 'lease':
        if (st === 'pending' || st === 'none') { m.state = 'fencing'; m.fences++; m.note = st === 'none' ? 'The lease end fences the unrecorded sequence at every partition.' : 'The lease end sends a range fence to cp1.'; }
        else if (st === 'held') m.note = 'Held stays held. The tracking partition skips the fence.';
        else m.note = 'No change: a fence is already out.';
        break;
      case 'ref':
        m.state = 'held'; m.note = 'The fence found cp1 holding batches. Cp1 owes a release report.';
        break;
      case 'rel':
        m.state = 'released'; m.note = 'The fence answered released.';
        break;
      case 'report':
        m.state = 'released';
        m.note = st === 'none' ? 'The sequence is above the highest recorded one, so the tracking partition stores an early released entry.' : 'Release report recorded.';
        break;
    }
  };
  const stateChip = st => chip(st === 'released' ? 'good' : st === 'held' || st === 'referenced' ? 'info' : st === 'fencing' ? 'warn' : '', st === 'none' ? 'no entry' : st);
  const render = () => {
    const v2Leak = s.v2.state !== 'released' && (s.v2.dropped || s.v2.overwritten);
    v2El.innerHTML = `<h6>Naive tracker ${stateChip(s.v2.state)}</h6><p class="small" style="margin:0 0 0.4rem">${s.v2.note}</p>` +
      `<dl class="kv"><dt>blocks deletion</dt><dd>${s.v2.state === 'released' ? 'no' : 'yes'}</dd><dt>leak</dt><dd>${v2Leak ? 'yes' : 'no'}</dd></dl>`;
    v3El.innerHTML = `<h6>The design ${stateChip(s.v3.state)}</h6><p class="small" style="margin:0 0 0.4rem">${s.v3.note}</p>` +
      `<dl class="kv"><dt>blocks deletion</dt><dd>${s.v3.state === 'released' ? 'no' : 'yes'}</dd><dt>fences sent to cp1</dt><dd>${s.v3.fences}</dd></dl>`;
    $('#re-history').textContent = s.history.length ? 'Events so far: ' + s.history.map((e, i) => `${i + 1}. ${labels[e]}`).join(', ') + '.' : 'No events yet.';
    const v3Text = {
      none: 'No entry yet. Send the cp1 release report first to try the early-release case.',
      pending: 'Cp1 is pending until a finish report, a fence answer, or a release report arrives. <code>TrackingEntryOldestAgeMs</code> for pending shows an entry that never clears.',
      fencing: 'A fence is out to cp1. <code>TrackingEntryOldestAgeMs</code> for fencing grows until it answers.',
      held: 'Cp1 holds batches and owes a release report. The tracking partition sends it no further fence.',
      released: 'Cp1 is released, so it no longer blocks deletion, and no later event can change that.',
    }[s.v3.state];
    show($('#re-readout'), v2Leak ? 'bad' : s.v3.state === 'released' ? 'good' : s.v3.state === 'fencing' ? 'warn' : '',
      (v2Leak ? `<b class="bad">The naive tracker leaks this object:</b> ${s.v2.dropped ? 'the early report was dropped, so the entry waits forever.' : '"referenced" overwrote "released," and cp1 never reports again.'} ` : '') + v3Text);
  };
  const apply = ev => { s.history.push(ev); stepV2(s.v2, ev); stepV3(s.v3, ev); render(); };
  for (const ev of Object.keys(labels)) $('#re-' + ev).addEventListener('click', () => apply(ev));
  const scenario = evs => { reset(); evs.forEach(ev => { s.history.push(ev); stepV2(s.v2, ev); stepV3(s.v3, ev); }); render(); };
  $('#re-sc-early').addEventListener('click', () => scenario(['report', 'track', 'answered']));
  $('#re-sc-late').addEventListener('click', () => scenario(['track', 'exception', 'report', 'ref']));
  $('#re-reset').addEventListener('click', () => { reset(); render(); });
  reset();
  render();
}
