/* ---------------------------------------------------------------- the barrier */
function demoBarrier() {
  const box = $('#br-svg');
  if (!box) return;
  const offBtn = $('#br-off'), onBtn = $('#br-on'), resetSel = $('#br-reset');
  let barrier = true;
  const init = () => ({
    active: null, result: '',
    consumer: ['Committed offset: 1,400', 'About to fetch orders-0'],
    b1: ['Thinks it leads cp0', 'orders-0 ends at 1,000 (its high watermark)'],
    b2: ['Leads cp0 since the election', 'orders-0 committed up to 1,500'],
  });
  const common = [
    { title: 'A leader change that broker 1 missed', text: 'Broker 1 led cp0 and lost contact with the controller and its followers. The controller elected broker 2, which kept committing. Broker 1 hasn\'t heard about it, so it still answers reads from its SQLite.' },
    { title: 'The consumer resumes at offset 1,400', text: 'The consumer\'s fetch reaches a broker whose metadata still names broker 1 as cp0\'s leader, so that broker calls <code>findBatches</code> on broker 1. Offset 1,400 is above broker 1\'s high watermark.',
      run: s => { s.active = 'b1'; s.b1 = ['Asked for orders-0 at 1,400', 'Its high watermark is 1,000: above it']; } },
  ];
  const without = () => [
    { title: 'Broker 1 answers OFFSET_OUT_OF_RANGE', text: 'Broker 1 answers from its own state: offset 1,400 doesn\'t exist. The answer is false. Offset 1,400 exists on broker 2.',
      run: s => { s.active = 'consumer'; s.consumer = ['Got OFFSET_OUT_OF_RANGE', `Applies auto.offset.reset=${resetSel.value}`]; } },
    resetSel.value === 'earliest'
      ? { title: 'The consumer rewinds to the start', text: 'With <code>earliest</code>, the consumer moves to the first offset and reads 1,400 records again. Applications see duplicates.',
        run: s => { s.consumer = ['Position: 0', 'Re-reads offsets 0 to 1,399']; s.result = 'bad'; } }
      : { title: 'The consumer jumps to the end', text: 'With <code>latest</code>, the consumer asks for the latest offset. That lookup reaches broker 2, which answers 1,500. The consumer never reads offsets 1,400 to 1,499. If the lookup reached broker 1 instead, it would get 1,000 and re-read records.',
        run: s => { s.active = 'b2'; s.consumer = ['Position: 1,500', 'Offsets 1,400 to 1,499 are skipped']; s.result = 'bad'; } },
  ];
  const withB = () => [
    { title: 'Broker 1 appends a barrier', text: 'Before it answers "out of range" above its high watermark, broker 1 appends an empty barrier record and waits for it to commit. Its followers now fetch from broker 2, so the barrier can\'t commit.',
      run: s => { s.b1 = ['Appended a barrier record', 'Waiting for it to commit']; } },
    { title: '500 ms pass', text: 'The barrier didn\'t commit in time. Broker 1 answers <code>OFFSET_NOT_AVAILABLE</code>, which is retriable and makes no claim about the offset.',
      run: s => { s.active = 'consumer'; s.b1 = ['Barrier never committed', 'Answered OFFSET_NOT_AVAILABLE']; s.consumer = ['Got a retriable error', 'Refreshes metadata and retries']; } },
    { title: 'The consumer retries at the real leader', text: 'The refreshed metadata names broker 2. Broker 2 has offset 1,400, so the consumer continues exactly where it stopped. A current leader that is asked about an offset past its end commits its barrier in one round trip and then answers truthfully.',
      run: s => { s.active = 'b2'; s.b2 = ['Asked for orders-0 at 1,400', 'Found it: serves offsets 1,400 onward']; s.consumer = ['Position: 1,400', 'Nothing skipped, nothing re-read']; s.result = 'good'; } },
  ];
  const panel = (name, lines, on, cls) =>
    `<div class="panel${on ? ' changed' : ''}${cls ? ' ' + cls : ''}"><h6>${name}</h6>${lines.map(l => `<div class="small">${l}</div>`).join('')}</div>`;
  const ctl = stepper({
    prev: $('#br-prev'), next: $('#br-next'),
    init, steps: () => common.concat(barrier ? withB() : without()),
    render: (s, prev, step, i, n) => {
      $('#br-count').textContent = `Step ${i + 1} of ${n}`;
      $('#br-narr').innerHTML = `<h6>${step.title}</h6><p>${step.text}</p>`;
      box.innerHTML = `<div class="grid3">${panel('Consumer', s.consumer, s.active === 'consumer')}` +
        `${panel('Broker 1 ' + chip('warn', 'deposed'), s.b1, s.active === 'b1', 'down')}` +
        `${panel('Broker 2 ' + chip('good', 'leader'), s.b2, s.active === 'b2')}</div>`;
      const ro = $('#br-readout');
      if (s.result === 'bad') show(ro, 'bad', 'One false answer from a deposed leader moved the consumer to the wrong position.');
      else if (s.result === 'good') show(ro, 'good', 'The barrier turned a false answer into a retry. The consumer lost nothing and read nothing twice.');
      else show(ro, '', barrier ? 'Barrier on. Step through to see what broker 1 does.' : 'Barrier off. Step through to see what broker 1 does.');
    },
  });
  offBtn.addEventListener('click', () => { barrier = false; setOn([offBtn, onBtn], offBtn); ctl.go(0); });
  onBtn.addEventListener('click', () => { barrier = true; setOn([offBtn, onBtn], onBtn); ctl.go(0); });
  resetSel.addEventListener('change', () => ctl.go(ctl.index));
}
