/* ---------------------------------------------------------------- admin guards */
function demoGuard() {
  const sel = $('#gd-action');
  if (!sel) return;
  const actions = [
    ['Delete the topic (DeleteTopics)',
      ['bad', 'Allowed', 'A principal with <code>DELETE</code> permission can delete it, as it can delete <code>__consumer_offsets</code>. Every diskless partition loses its batch index.'],
      ['good', 'INVALID_TOPIC_EXCEPTION', 'Rejected after authorization. Other topics in the same request are still deleted.']],
    ['Produce with client ID __admin_client',
      ['bad', 'Allowed', 'Any client can use this client ID to write to an internal topic. A foreign record would be applied on every replica and take an offset the leader\'s overlay doesn\'t expect.'],
      ['good', 'INVALID_TOPIC_EXCEPTION', 'Only appends from the coordinator itself pass.']],
    ['Create the topic from a client, or by auto-create',
      ['bad', 'Allowed', 'A client could create the topic first, with broker defaults and the wrong partition count.'],
      ['good', 'INVALID_REQUEST', 'Only the controller creates it, with the fixed configs.']],
    ['Add partitions (CreatePartitions)',
      ['bad', 'Allowed', 'A new partition count changes <code>slot % N</code>, so user partitions would map to a different coordinator partition that has none of their state.'],
      ['good', 'INVALID_PARTITIONS', '<code>N</code> is fixed after creation.']],
    ['Unclean leader election (ElectLeaders UNCLEAN)',
      ['bad', 'Allowed', 'This admin path never reads <code>unclean.leader.election.enable</code>, so it can elect a replica that misses acknowledged records.'],
      ['good', 'INVALID_REQUEST', 'Rejected for this topic.']],
    ['Delete records (DeleteRecords)',
      ['good', 'INVALID_TOPIC_EXCEPTION', 'Already rejected for internal topics.'],
      ['good', 'INVALID_TOPIC_EXCEPTION', 'Same. Only the snapshot rule moves the log start.']],
    ['Reassign to a different replica count',
      ['bad', 'Allowed', 'Fewer replicas means fewer copies of every acknowledged record.'],
      ['good', 'INVALID_REPLICATION_FACTOR', 'A reassignment that keeps the replica count still works.']],
    ['Set flush.messages=10000',
      ['bad', 'Allowed', 'A correlated power loss would lose acknowledged records, as the durability chapter shows.'],
      ['good', 'INVALID_CONFIG', 'Setting it to its required value, 1, passes.']],
    ['Lower min.insync.replicas to 1',
      ['bad', 'Allowed', 'Records are acknowledged on one replica, and the change clears the partition\'s eligible leader replicas.'],
      ['good', 'INVALID_CONFIG', 'Fixed at its value from creation.']],
    ['Set retention.ms to 7 days',
      ['bad', 'Allowed', 'Kafka would delete old segments on its own and move the log start past the snapshots.'],
      ['good', 'INVALID_CONFIG', 'Must stay <code>-1</code>.']],
    ['Change segment.bytes',
      ['good', 'Allowed', 'Harmless.'],
      ['good', 'Allowed', 'Configs without a safety role stay alterable.']],
    ['Preferred leader election',
      ['good', 'Allowed', 'Moves leadership to an in-sync replica.'],
      ['good', 'Allowed', 'Keeps every acknowledged record, so it stays allowed.']],
  ];
  actions.forEach(([name], i) => sel.append(h('option', { value: i }, name)));
  const panel = (title, [cls, verdict, text]) => `<h6>${title} ${chip(cls, verdict)}</h6><p class="small" style="margin:0">${text}</p>`;
  const render = () => {
    const a = actions[+sel.value];
    $('#gd-up').innerHTML = panel('Upstream internal topic', a[1]);
    $('#gd-v2').innerHTML = panel('This design', a[2]);
  };
  sel.addEventListener('change', render);
  render();
}
