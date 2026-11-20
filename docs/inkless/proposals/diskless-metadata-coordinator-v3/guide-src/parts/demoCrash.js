/* ---------------------------------------------------------------- crash points */
function demoCrash() {
  const host = $('#cr-points');
  if (!host) return;
  const points = [
    { label: 'Before the upload', obj: false, cp0: ['muted', 'Never heard of it.'], cp1: ['muted', 'Never heard of it.'], cls: 'good',
      text: 'Nothing reached storage, so nothing is left behind. The producers get an error and retry.' },
    { label: 'After the upload, before any commit', obj: true, cp0: ['muted', 'Never heard of it.'], cp1: ['muted', 'Never heard of it.'], cls: 'bad', tag: 'Orphan',
      text: '<b>The orphan.</b> The object is in storage, and no coordinator partition knows its name. Nothing ever points into it, so it would be safe to delete, but nobody knows it exists.' },
    { label: 'After the cp0 part, before the cp1 part', obj: true, cp0: ['good', 'Holds batches in the object.'], cp1: ['muted', 'Never heard of it.'], cls: 'warn', tag: 'Partly referenced',
      text: '<b>Partly referenced.</b> cp0 holds batches in the object. cp1 never heard of it. When cp0 later removes its batches, cp0 alone can\'t tell whether anyone else still needs the object.' },
    { label: 'The cp1 part times out', obj: true, cp0: ['good', 'Holds batches in the object.'], cp1: ['warn', 'The request may still be in its queue.'], cls: 'warn', tag: 'Unknown outcome',
      text: '<b>The unknown outcome.</b> The broker gave up on cp1 and told those producers it failed. But the request may still be waiting at cp1\'s leader, and it can commit later. Until something rules that out, nobody can say the object is unreferenced.' },
    { label: 'Both parts commit, the reply is lost', obj: true, cp0: ['good', 'Holds batches in the object.'], cp1: ['good', 'Holds batches in the object.'], cls: '', tag: 'Fully referenced',
      text: 'Both parts committed, so the object is fully referenced. The producers didn\'t hear back and retry. Idempotent producers get "already committed." The object is deleted later, once both cp0 and cp1 removed their batches: the shared-object case.' },
  ];
  const buttons = points.map((p, i) => {
    const b = h('button', null, p.label);
    b.addEventListener('click', () => pick(i));
    host.append(b);
    return b;
  });
  const pick = i => {
    const p = points[i];
    setOn(buttons, buttons[i]);
    $('#cr-storage').innerHTML = `<h6>Object storage ${p.tag ? chip(p.cls || 'info', p.tag) : ''}</h6>` +
      (p.obj ? `<div class="objbox" style="width: 140px"><span style="width:50%;background:var(--cp0)"></span><span style="width:50%;background:var(--cp1)"></span></div><div class="small mono" style="margin-top:0.3rem">a71f…e0</div>` : '<div class="small muted">Empty</div>');
    $('#cr-cp0').innerHTML = `<h6>${cpChip(0)}</h6><div class="small ${p.cp0[0] === 'muted' ? 'muted' : p.cp0[0] + '-text'}">${p.cp0[1]}</div>`;
    $('#cr-cp1').innerHTML = `<h6>${cpChip(1)}</h6><div class="small ${p.cp1[0] === 'muted' ? 'muted' : p.cp1[0] + '-text'}">${p.cp1[1]}</div>`;
    show($('#cr-readout'), p.cls, p.text);
  };
  pick(1);
}
