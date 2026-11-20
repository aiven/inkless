/* ---------------------------------------------------------------- owner approach stepper */
function demoV1() {
  const cpsHost = $('#v1-cps');
  if (!cpsHost) return;
  const KEY = 'k-9f2c';
  const init = () => ({
    storage: 'none',
    cps: [
      { role: 'participant', holds: 'nothing yet', records: [], notes: [] },
      { role: 'not a participant', holds: '', records: [], notes: [] },
      { role: 'participant and owner', holds: 'nothing yet', records: [], notes: [] },
      { role: 'participant', holds: 'nothing yet', records: [], notes: [] },
    ],
  });
  const steps = [
    { title: 'Close the file and pick the owner', clock: false,
      text: `The broker closes a file with batches for cp0, cp2, and cp3, and generates a random key, <code>${KEY}</code>. It hashes the key to pick one participant as the preferred owner: cp2.` },
    { title: 'Store the intent before the upload', clock: false, wait: true,
      text: 'The broker sends an intent to cp2: the key, the participants, and the owner. cp2 appends it and waits for replication and a disk sync on every replica. The upload, and so every producer in this file, waits for this step.',
      run: s => { s.cps[2].records.push('intent registered'); s.cps[2].notes = ['owns ' + KEY + ': waiting for cp0, cp2, cp3']; } },
    { title: 'Upload the object', clock: false,
      text: 'Only now does the broker upload the object.',
      run: s => { s.storage = 'uploaded'; } },
    { title: 'Send the commit parts', clock: true,
      text: 'The broker sends one part each to cp0, cp2, and cp3. Each part carries its first-send time, 12:00:00 on the broker\'s clock. Each leader checks the age on <i>its own</i> clock: the part must be younger than the maximum commit age, 5 minutes.',
      run: s => { for (const i of [0, 2, 3]) { s.cps[i].records.push('file committed'); s.cps[i].holds = 'batches in ' + KEY; } } },
    { title: 'Retention at cp0 releases the object', clock: true,
      text: 'At 12:01, retention removes cp0\'s batches. cp0 holds nothing in the object, so it reports a release to the owner, and appends an owner-accepted record once the owner accepts it. The part\'s first-send time is younger than the maximum commit age, so cp0 keeps a tombstone for the key, so a delayed retry can\'t add the batches back. The tombstone lasts twice the maximum commit age from its creation, until 12:11.',
      run: s => { s.cps[0].records.push('retention applied', 'owner accepted'); s.cps[0].holds = 'nothing'; s.cps[0].notes = ['tombstone for ' + KEY + ' until 12:11']; s.cps[2].records.push('participant state recorded: cp0 released'); s.cps[2].notes = ['owns ' + KEY + ': waiting for cp2, cp3']; } },
    { title: 'cp2 releases its own batches', clock: false,
      text: '<code>DeleteRecords</code> removes cp2\'s batches. The owner records its own release.',
      run: s => { s.cps[2].records.push('records deleted'); s.cps[2].holds = 'nothing'; s.cps[2].notes = ['owns ' + KEY + ': waiting for cp3']; } },
    { title: 'Five minutes pass: fence the silent participant', clock: true,
      text: 'cp3 never reported, so it\'s silent: it might hold batches, or its part might never have arrived. Once the maximum commit age has passed on the owner\'s clock, the owner fences cp3. cp3 holds a commit for the key, so it appends a file-fenced record with the answer referenced, which leaves its commit in place. The owner records cp3 as referenced and waits for its release report without fencing it again.',
      run: s => { s.cps[3].records.push('file fenced: referenced, owner cp2'); s.cps[2].records.push('participant state recorded: cp3 referenced'); } },
    { title: 'cp3 releases, and the owner marks the object', clock: true,
      text: 'Retention removes cp3\'s batches, cp3 reports a release, and it appends an owner-accepted record. By now the part\'s first-send time is older than the maximum commit age, so no retry can pass the age check, and cp3 deletes its row for the object instead of keeping a tombstone. Every participant is released and the intent is older than the maximum commit age, so the owner marks the object for deletion.',
      run: s => { s.cps[3].records.push('retention applied', 'owner accepted'); s.cps[3].holds = 'nothing'; s.cps[3].notes = ['row deleted, no tombstone']; s.cps[2].records.push('participant state recorded: cp3 released', 'file delete marked'); s.cps[2].notes = ['deletion queue: ' + KEY]; } },
    { title: 'FileCleaner deletes the object', clock: false,
      text: 'After the grace period, <code>FileCleaner</code> deletes the object from storage, and the owner records that the file is deleted.',
      run: s => { s.storage = 'deleted'; s.cps[2].records.push('file deleted'); s.cps[2].notes = []; } },
    { title: '12:11: cp0\'s tombstone expires', clock: true,
      text: 'Tombstones and fences expire once they are older than twice the maximum commit age, through a keys-expired record, so SQLite doesn\'t grow forever. cp0\'s tombstone dates from 12:01, so it goes at 12:11. From now on, cp0 remembers nothing about the key. A late part from 12:00 is stopped only by the age check, and that check compares two different clocks. Chapter 15 shows where that goes wrong.',
      run: s => { s.cps[0].records.push('keys expired'); s.cps[0].notes = []; } },
  ];
  stepper({
    prev: $('#v1-prev'), next: $('#v1-next'), reset: $('#v1-reset'),
    init, steps: () => steps,
    render: (s, prev, step, i, n) => {
      $('#v1-count').textContent = `Step ${i + 1} of ${n}`;
      $('#v1-narr').innerHTML = `<h6>${step.title} ${step.clock ? chip('warn', 'uses a clock') : ''} ${step.wait ? chip('bad', 'produce waits') : ''}</h6><p>${step.text}</p>`;
      cpsHost.innerHTML = '';
      s.cps.forEach((c, k) => {
        const changed = prev && JSON.stringify(prev.cps[k]) !== JSON.stringify(c);
        cpsHost.append(h('div', { class: 'panel' + (changed ? ' changed' : '') + (k === 1 ? ' down' : ''), html:
          `<h6>${cpChip(k)}<span class="small muted">${c.role}</span></h6>` +
          (c.holds ? `<div class="small">Holds: ${c.holds}</div>` : '') +
          (c.notes.length ? `<div class="small warn-text">${c.notes.join('<br>')}</div>` : '') +
          `<div class="small muted" style="margin-top:0.3rem">Records</div>` +
          (c.records.length ? `<ol class="small mono" style="margin:0;padding-left:1.1rem">${c.records.map(r => `<li>${r}</li>`).join('')}</ol>` : '<div class="small muted">none</div>') }));
      });
      const clocks = steps.slice(0, i + 1).filter(x => x.clock).length;
      show($('#v1-readout'), s.storage === 'deleted' ? 'good' : '',
        `Object storage: <b>${s.storage === 'none' ? 'nothing uploaded' : s.storage === 'uploaded' ? KEY + ' stored' : KEY + ' deleted'}</b>. ` +
        `Steps so far that depend on a clock: <b>${clocks}</b>.`);
    },
  });
}
