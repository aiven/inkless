## Decision statement

To allow producers to write to the same topic partition, a new concept of _topic partition leader per zone_ is needed.

The Batch Coordinator ensures the ordering between produce request batches across brokers in different zones. 

## Rationale

To reduce the cross-AZ traffic to zero, we need to ensure that all produce requests are coordinated within the same zone.

## Expected Outcome

A batch coordinator is introduced to serve batch offsets requests and ensure ordering between produce request batches across topic partition leaders placed on brokers in different zones.

## Implications of the decision

Each produce request will have to account for the _block buffering time_: the time it takes to fill the batch buffer across topic partitions on the same broker;
and the _offset coordination time_: the time it takes to coordinate the batch offsets across brokers in different zones.

To be able to avoid cross-AZ costs, the producers have to share their zone placement (e.g. on a new config field or encode it on client-id).
Otherwise, the requests will be targeted to any of the leaders, potentially in another AZ.

## Rejected Alternatives

### Allow cross-AZ produce requests and avoid batch offset coordination

By allowing Producers from the different zones to write to the same topic partition leader, as it is today, there will be no need for batch coordinator.
Instead, a potential cross-AZ cost will be incurred.

If the traffic is evenly distributed across zones, the cross-AZ traffic will be 2/3 of the total traffic.
Half of this traffic will be charged to the client-side (customer), and the other half to the broker-side (provider).
In a BYOC context, all the incoming traffic will be charged to the customer.

> [!NOTE]
> These costs will be incurred by the proposed alternative if producers are not configured to provide zone location.

Produce latency in this case will only be the block buffering time.

Batch coordinate metadata still needs to be shared across zones, to allow for leader rebalancing, recovery, etc; but not for ordering.

The cost of this alternative is the cross-AZ traffic, which is not acceptable at the moment:
- 1/6 of all cross-AZ traffic is potentially due to produce requests, so worst case scenario is 1/6 of the total cross-AZ traffic.
- 2/3 of the 1/6 is the average case scenario, where throughput from 2 out of 3 zones is produced from zones different than the leader.

Using the small/large cluster numbers, the cost of this alternative is:

// TODO: Add the cost calculation here

## Status

Status: In Discussion
