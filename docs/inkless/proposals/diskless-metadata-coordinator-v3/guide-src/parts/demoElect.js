/* ---------------------------------------------------------------- election after a crash */
function demoElect() {
  const firstEl = $('#el-first');
  if (!firstEl) return;
  const diskEl = $('#el-disk');
  const render = () => {
    const n = +firstEl.value, wiped = diskEl.value === 'wiped';
    let up, v2;
    if (n === 1) {
      up = wiped
        ? ['bad', 'Data lost', 'Broker 1 is the last known leader, and upstream Kafka elects it as soon as it returns, even with <code>unclean.leader.election.enable=false</code>. Its log is empty. When brokers 2 and 3 return, they truncate to match it. cp0\'s batch index, leases, and deletion queues are gone, while object storage still holds every object.']
        : ['good', 'Recovers', 'Broker 1 is the last known leader, so upstream elects it. Its disk survived, so nothing is lost.'];
    } else {
      up = ['warn', 'Waits for broker 1', (n === 2
        ? 'Broker 2 shut down uncleanly, so upstream removes it from the ELR when it registers. '
        : 'Broker 3 was never in the ELR. ') +
        'cp0 stays offline until broker 1, the last known leader, returns. Then broker 1 leads' +
        (wiped ? `, and broker ${n} copies the log from it.` : '.')];
    }
    if (wiped) {
      v2 = ['good', 'Not elected', `The log directory that held broker ${n}'s replica has a new ID, so the controller removes broker ${n} from the ELR and from the last-known-leader list. ` +
        (n === 1 ? 'Broker 2 is still in the ELR and leads when it returns. Broker 1 copies the log from it.'
          : n === 2 ? 'Broker 1 leads when it returns. Broker 2 copies the log from it.'
            : 'Broker 1 or broker 2 leads when it returns.')];
    } else if (n === 3) {
      v2 = ['', 'Waits', 'Broker 3\'s disk is fine, but it dropped out of the in-sync set before the others and may miss the newest records. It isn\'t in the ELR, so cp0 waits for broker 1 or broker 2.'];
    } else {
      v2 = ['good', 'Leads at once', `An unclean shutdown keeps broker ${n} in the ELR, because with <code>flush.messages=1</code> its disk holds every acknowledged record. Its directory ID is unchanged, so it leads as soon as it returns.`];
    }
    const panel = (title, [cls, verdict, text]) => `<h6>${title} ${cls ? chip(cls, verdict) : chip('', verdict)}</h6><p class="small" style="margin:0">${text}</p>`;
    $('#el-up').innerHTML = panel('Upstream Kafka', up);
    $('#el-v2').innerHTML = panel('This design', v2);
  };
  firstEl.addEventListener('change', render);
  diskEl.addEventListener('change', render);
  render();
}
