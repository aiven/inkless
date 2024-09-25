# Known Challenges

This is the current list of known challenges for Inkless.

## New type of Topic

We need to create a new type of topic, in order to have Inkless topics working alongside traditional topics.
What would we need? can we piggyback on TS infrastructure?

## Replication

We need to understand how to effectively deactivate replication factor for the new type of topics. Is setting replication factor to 1 enough? Should this setting be ignored for the new type of topics?

## How to deal with Object Storage writing

Where do we need to "inject" ourselves to enable Inkless writing on an Object Storage as primary storage. Ordering of batches? How to persist metadata in the metadata store?

## What is the right batch size for our Object Storage writing

We need to calculate the right size to minimize costs and latency (which probably are at odds)

## Data format of our Object Storage data

How does the data look like? Which metadata (topic, partition...) do we need to store and how? 

## How to deal with metadata?

How can we record where is the data stored? How can we tweak partition ownership given that brokers should be stateless? How do we spread load of consumers and producers? Which type of metadata?

## What's our take on partition ownership?

Given brokers are stateless and Kafka has the concept of leader of a partition, how do we reconcile these two concepts? How can we guarantee ordering within partition?

## How to reduce inter-AZ costs?

Intra-AZ traffic is usually free, while inter-AZ traffic is expensive. How to have clusters that span multiple AZs but reduce inter-AZ traffic to a minimum? Is setting independent clusters with same object storage enough? How do we deal with data linearization?

## How to deal with the active segment?

How can we make use of the active segment as a cache for fresh data, but at the same time, limiting the disk to the acive segment
