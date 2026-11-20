/* ---------------------------------------------------------------- image edge cases */
function demoV3Image() {
  const sel = $('#ie-case');
  if (!sel) return;
  const xEl = $('#ie-x'), track = $('#ie-track');
  const MIN = +xEl.min, MAX = +xEl.max;
  const pos = o => ((o - MIN) / (MAX - MIN)) * 100;
  const ok = (title, text) => ({ cls: 'good', title, text });
  const bad = (title, text) => ({ cls: 'bad', title, text });
  const cases = {
    broker: {
      start: 400,
      events: [[120, 'Broker 4 registers at epoch 120 and commits here.'], [300, 'Broker 4 is unregistered.'], [480, 'Broker 4 re-registers at epoch 480.']],
      at: x => x < 120 ? {
        v2: bad('live broker retired', 'The image shows no registration for broker 4, so the naive rule retires broker 4 with no epoch attached. Broker 4 is rejected for good, although it is live at epoch 120.'),
        v3: ok('nothing appended', `<code>retiredThrough</code> would be ${x}, below epoch 120 that the row already holds, so the leader appends nothing. Commits at epoch 120 proceed.`),
      } : x < 300 ? {
        v2: ok('no change', 'The image shows broker 4 at epoch 120, the epoch the row holds. Nothing needs retiring.'),
        v3: ok('no change', 'The image shows broker 4 at epoch 120, the epoch the row holds. Nothing needs retiring.'),
      } : x < 480 ? {
        v2: bad('rejected forever', 'The image shows no registration, so the naive rule retires broker 4 with no epoch attached. When broker 4 returns at epoch 480, it is rejected as a stale epoch, and it stays rejected.'),
        v3: ok(`retiredThrough ${x}`, `The image shows no registration, so <code>retiredThrough</code> is the image offset, ${x}. A late part at epoch 120 gets stale-epoch. Epoch 480 is above ${x}, so the returning broker commits.`),
      } : {
        v2: ok('row moves to 480', 'The image shows epoch 480, so the naive rule moves the row to the new epoch, and commits at 480 pass. It fails only when it acted on an image from the unregistered window.'),
        v3: ok('retiredThrough 479', 'The image shows epoch 480, so <code>retiredThrough</code> is 479. Epoch 120 gets stale-epoch, epoch 480 commits, and the tracking partition ends the epoch-120 leases.'),
      },
    },
    lag: {
      start: 500,
      events: [[520, 'Broker 7 registers at epoch 520 and commits here at once.']],
      at: x => x < 520 ? {
        v2: bad('live broker retired', 'The image has no registration for broker 7, so the naive rule retires it. Broker 7 is live and has already committed here, but it is rejected for good.'),
        v3: ok('nothing appended', `<code>retiredThrough</code> would be ${x}, below epoch 520 in the row, so the leader appends nothing. Commits continue before and after the image catches up.`),
      } : {
        v2: ok('no change', 'The image shows broker 7 at epoch 520. Nothing needs retiring.'),
        v3: ok('no change', 'The image shows broker 7 at epoch 520. Nothing needs retiring.'),
      },
    },
    topic: {
      start: 560,
      events: [[590, 'Topic orders is created.'], [610, 'The old coordinator leader appends partition-created with metadataOffset 610.'], [650, 'Topic orders is deleted.']],
      at: x => x < 590 ? {
        v2: bad('live rows purged', 'The image doesn\'t contain the topic, so the naive rule appends topic-deleted. The coordinator purges the partition\'s batches and queues its objects while the topic is live. The delete is irreversible.'),
        v3: ok('skipped', `The image is at ${x}, below <code>metadataOffset</code> 610, so the leader skips the topic until the next metadata update. Absence below the creation offset proves nothing.`),
      } : x < 650 ? {
        v2: ok('topic present', 'The image contains the topic, so nothing is deleted.'),
        v3: ok('topic present', 'The image contains the topic, so nothing is deleted.'),
      } : {
        v2: ok('deleted', 'The image is past the deletion, and the naive rule appends topic-deleted, which is correct here.'),
        v3: ok('deleted', `The image is at ${x}, at or past <code>metadataOffset</code> 610, and the topic is absent. Topic IDs are never reused, so the absence proves the deletion, and the leader appends topic-deleted.`),
      },
    },
  };
  const panel = (label, r) => `<h6>${label} ${chip(r.cls, r.title)}</h6><p class="small" style="margin:0">${r.text}</p>`;
  const render = () => {
    const c = cases[sel.value], x = +xEl.value;
    $('#ie-x-label').textContent = 'offset ' + x;
    track.innerHTML = `<div class="bar-seg" style="left:0;width:${pos(x)}%;background:var(--accent)">image at ${x}</div>` +
      c.events.map(([o]) => `<div class="bar-seg" style="left:calc(${pos(o)}% - 1px);width:3px;background:var(--ink)" title="offset ${o}"></div>`).join('');
    $('#ie-events').innerHTML = c.events.map(([o, text]) =>
      `<div>${chip(o <= x ? 'good' : 'info', o <= x ? 'in image' : 'ahead of image')} <span class="mono">${o}</span> ${text}</div>`).join('');
    const r = c.at(x);
    $('#ie-v2').innerHTML = panel('Naive rule: absent means gone', r.v2);
    $('#ie-v3').innerHTML = panel('The design: compare offsets', r.v3);
    show($('#ie-readout'), r.v2.cls === 'bad' ? 'warn' : '',
      r.v2.cls === 'bad'
        ? 'The naive rule reads absence from a lagging image as retirement or deletion. The design compares the image offset with the offset the row carries, so it waits instead.'
        : 'Here the image is far enough along, and both rules agree. Drag the offset into the gap before the next record to see them differ.');
  };
  sel.addEventListener('change', () => { xEl.value = cases[sel.value].start; render(); });
  xEl.addEventListener('input', render);
  xEl.value = cases[sel.value].start;
  render();
}
