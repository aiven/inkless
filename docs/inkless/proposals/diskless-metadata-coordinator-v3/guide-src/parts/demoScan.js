/* ---------------------------------------------------------------- scanner or books */
function demoScan() {
  const tabs = $('#sc-tabs');
  if (!tabs) return;
  const cases = [
    { name: 'A plain orphan',
      scan: ['good', 'Works', 'The scanner lists the bucket, finds the old object, asks every coordinator partition, and nobody knows the key. It deletes the object. Correct.'],
      books: ['good', 'Works', 'The records named the key before the upload. Once they show that nothing can reference it, the coordinator deletes it. Correct.'] },
    { name: 'A coordinator answers from old state',
      scan: ['bad', 'Data loss', 'One coordinator partition answers from an old state: for example, a replica restored from an old copy, or a bug in a lookup. It says "unknown" for an object it does reference. The scanner deletes live data.'],
      books: ['good', 'Leak at worst', 'Old state can only be missing records. A missing record never puts a key in the deletion queue, so the worst outcome is an object that stays in storage longer.'] },
    { name: 'Two clusters share a bucket prefix',
      scan: ['bad', 'Data loss', 'Cluster A\'s scanner lists cluster B\'s objects too. Nobody in cluster A knows them, so the scanner deletes cluster B\'s live data.'],
      books: ['good', 'Works', 'The coordinator deletes only keys its own records name. Every key starts with the cluster ID, so cluster B\'s objects are never in cluster A\'s books.'] },
    { name: 'A clock runs 10 minutes ahead',
      scan: ['bad', 'Data loss', 'The scanner skips objects younger than its age threshold, to leave room for uploads that haven\'t committed yet. With its clock ahead, a fresh upload looks old. The scanner deletes it, and then the commit arrives and points into a deleted object.'],
      books: ['warn', 'Depends on how bookkeeping uses clocks', 'Bookkeeping that expires its records on a timer breaks the same way, as the rejected-alternatives chapter shows. This design uses clocks only to decide when, never whether, as chapter 27 explains.'] },
    { name: 'Cost',
      scan: ['warn', 'Follows live objects', 'At 40 uploads per second and seven days of retention, about 24 million objects are live. An hourly pass makes about 24,000 list requests and about 190 million key lookups across 8 coordinator partitions, because the scanner doesn\'t know which partitions an object belongs to.'],
      books: ['good', 'Follows the upload rate', 'The coordinator never lists the bucket. Its work grows with the number of objects created and deleted, not with the number that exist.'] },
  ];
  const btns = cases.map((c, i) => {
    const b = h('button', null, c.name);
    b.addEventListener('click', () => pick(i));
    tabs.append(b);
    return b;
  });
  const panel = (title, [cls, verdict, text]) => `<h6>${title} ${chip(cls, verdict)}</h6><p class="small" style="margin:0">${text}</p>`;
  const pick = i => {
    setOn(btns, btns[i]);
    $('#sc-scanner').innerHTML = panel('Deleting scanner', cases[i].scan);
    $('#sc-books').innerHTML = panel('Log-driven bookkeeping', cases[i].books);
  };
  pick(0);
}
