/* ---------------------------------------------------------------- status board */
function demoV3Status() {
  const table = $('#sb-table'), statusEl = $('#sb-status'), reviewEl = $('#sb-review');
  if (!table) return;
  const rows = [
    { ticket: '01', review: 'Fable', sources: ['fable'], severity: 'Critical', group: 'decided', status: 'decided', text: 'Finish reports: fence only exceptions, missed sequences, and lease ends, and only after the broker finished the file.' },
    { ticket: '02', review: 'Fable', sources: ['fable'], severity: 'Warning', group: 'decided', status: 'decided', text: 'Keepalives at half the idle timeout, an immediate switch after a fallback, and scoped lease releases.' },
    { ticket: '03', review: 'Fable', sources: ['fable'], severity: 'Warning', group: 'decided', status: 'decided', text: 'retiredThrough from the image offset, a metadataOffset gate on topic-deleted, and COORDINATOR_LOAD_IN_PROGRESS for unreconciled partitions.' },
    { ticket: '04', review: 'Council and Fable', sources: ['council', 'fable'], severity: 'Warning', group: 'decided', status: 'decided', text: 'Each produce request completes when the parts that hold its batches finish. Story 83 holds per request.' },
    { ticket: '05', review: 'Fable', sources: ['fable'], severity: 'Warning', group: 'decided', status: 'decided', text: 'Released is terminal, early reports are stored, entries move forward through four states, reports are batched, and gauges show stuck entries.' },
    { ticket: '06', review: 'Council and Fable', sources: ['council', 'fable'], severity: 'Warning', group: 'decided', status: 'decided', text: 'One batched fence request in flight per participant, and finishedThrough over the whole broker epoch.' },
    { ticket: '07', review: 'Council · research', sources: ['council'], research: true, severity: 'Warning', group: 'research', status: 'research closed', text: 'A same-upload footer is feasible, and readers ignore trailing bytes. It can\'t hold offsets or commit outcomes.' },
    { ticket: '08', review: 'Council · research', sources: ['council'], research: true, severity: 'Warning', group: 'research', status: 'research closed', text: 'S3 versioning, GCS soft delete, and Azure blob soft delete keep deletes recoverable. Object-write credentials can\'t check the setting.' },
    { ticket: '09', review: 'Council', sources: ['council'], severity: 'Critical', group: 'future', status: 'future work', text: 'A footer of partition runs and an operator-triggered recovery, recorded as future work. Clause 2 takes the guard exception.' },
    { ticket: '10', review: 'Council', sources: ['council'], severity: 'Critical', group: 'future', status: 'future work', text: 'A soft-delete requirement, probe, restore, and deletion-rate cap, recorded as future work. Six rows stay accepted risks.' },
    { ticket: '11', review: 'Council', sources: ['council'], severity: 'Warning', group: 'decided', status: 'decided', text: 'Minimum replication factor 3 with an internal override, no narrowing of rack spread on reassignment, and a spread gauge.' },
    { ticket: '12', review: 'Fable · research', sources: ['fable'], research: true, severity: 'Suggestion', group: 'research', status: 'research closed', text: 'One file costs up to N × R log flushes, and a background PASSIVE checkpoint is feasible.' },
    { ticket: '13', review: 'Fable', sources: ['fable'], severity: 'Suggestion', group: 'decided', status: 'decided', text: 'PASSIVE checkpoints on a broker-wide thread, one transaction per high-watermark advance, and a follower-applier thread.' },
    { ticket: '14', review: 'Fable', sources: ['fable'], severity: 'Suggestion', group: 'decided', status: 'decided', text: 'The default N stays 8, 3 brokers is the smallest cluster and the first benchmark, and the linger defaults to adaptive.' },
    { ticket: '15', review: 'Council', sources: ['council'], severity: 'Warning', group: 'open', status: 'open · in progress', text: 'Session notes so far: existing broker in-flight limits, exponential backoff on retried RPCs, and a read pool off the shard queue.' },
    { ticket: '16', review: 'Council', sources: ['council'], severity: 'Warning', group: 'blocked', status: 'waiting on 15', text: 'Workloads, scales, and thresholds. The partial-failure benchmark judges per-request latency.' },
    { ticket: '17', review: 'Council and Fable', sources: ['council', 'fable'], severity: 'Warning', group: 'blocked', status: 'waiting on 19', text: 'A bounded model that reproduces v2 defects, checks invariants, and drops tails at every fsync point.' },
    { ticket: '18', review: 'Council and Fable', sources: ['council', 'fable'], severity: 'Warning', group: 'blocked', status: 'waiting on 15, 16, 17, 19', text: 'Rewrite failure modes, risks, metrics, tests, stories, and Terms once the other tickets close.' },
    { ticket: '19', review: 'Council · from ticket 11', sources: ['council'], severity: 'Warning', group: 'open', status: 'open', text: 'The empty restart: its trigger, how user partitions restart, and how the empty partition acts as participant and tracking partition.' },
  ];
  const statusClass = { decided: 'good', research: 'good', future: 'info', open: 'warn', blocked: '' };
  const matchesReview = (row, filter) => filter === 'all' ||
    (filter === 'research' && row.research) ||
    (filter === 'both' && row.sources.length === 2) ||
    (filter !== 'research' && filter !== 'both' && row.sources.includes(filter));
  const render = () => {
    const status = statusEl.value, review = reviewEl.value;
    const visible = rows.filter(row => (status === 'all' || row.group === status) && matchesReview(row, review));
    table.innerHTML = '';
    visible.forEach(row => table.append(h('tr', null,
      h('td', { class: 'mono' }, row.ticket),
      h('td', null, row.review),
      h('td', { html: chip(row.severity === 'Critical' ? 'bad' : row.severity === 'Warning' ? 'warn' : 'info', row.severity) }),
      h('td', { html: chip(statusClass[row.group], row.status) }),
      h('td', { class: 'small' }, row.text))));
    $('#sb-count').textContent = `${visible.length} of ${rows.length} tickets`;
    show($('#sb-readout'), visible.length ? '' : 'warn',
      visible.length ? `Showing ${visible.length} ticket${visible.length === 1 ? '' : 's'}.` : 'No tickets match both filters.');
  };
  statusEl.addEventListener('change', render);
  reviewEl.addEventListener('change', render);
  render();
}
