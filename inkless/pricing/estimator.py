#!/usr/bin/env python

import argparse
import csv
import json
import math
import pathlib
from bs4 import BeautifulSoup

HRS_TO_SECS = 3600
SECS_TO_MONTH = 1.0/(60 * 60 * 24 * 30)
MONTHLY_HOURS = 30 * 24
BYTES_TO_GB = 1e-9


def _combine_fns(fns):
    def combined(*args):
        return sum([fn(*args) for fn in fns])
    return combined


def _redundant_storage(fn, mult):
    def _compute(brokers_per_cluster, bytes_per_cluster):
        return fn(brokers_per_cluster, mult * bytes_per_cluster)
    return _compute


def _redundant_writes(fn, mult):
    def _compute(broker_count, request_size, write_iops, read_iops, delete_iops):
        return fn(broker_count, request_size, mult * write_iops, read_iops, delete_iops)
    return _compute


def _summarize_objects(found):
    if len(found) == 0:
        return ""
    first = found[0][1]
    common = {k: first[k] for k in first}
    for sku, f in found:
        for k in f:
            if k in common and common[k] != f[k]:
                del common[k]

    difference = [{k: f[k] for k in f if k not in common} for sku, f in found]
    return str(difference)


def _sig_figs(x, n):
    # From https://stackoverflow.com/questions/3410976/how-to-round-a-number-to-significant-figures-in-python
    return x if x == 0 else round(x, -int(math.floor(math.log10(abs(x)))) + (n - 1))


def _simulate(
        brokers_per_cluster,
        replication_factor,
        hot_set_retention_hours,
        hot_set_bytes_per_block,
        hot_set_consumer_count,
        archive_retention_hours,
        archive_bytes_per_block,
        archive_consumer_count,
        reduced_durability_fn,
        fixed_instance_price_fn,
        variable_instance_price_fn,
        network_total_fn,
        network_capacity_fn,
        network_price_fn,
        hot_set_storage_price_fn,
        hot_set_io_price_fn,
        archive_storage_price_fn,
        archive_io_price_fn
):
    follower_count = replication_factor - 1
    reduced_durability = reduced_durability_fn()

    # Network resource utilization
    # EBS is assumed to be on a different budget and uncapped
    traditional_transfers = sum([
        follower_count,  # replication out
        follower_count,  # replication in
    ])
    inkless_transfers = sum([
        replication_factor if reduced_durability else 1,  # hot set write
        follower_count,  # hot set read
        1 * archive_consumer_count,  # archive read
        2 * hot_set_consumer_count,  # cache lookups
        2 * archive_consumer_count,  # cache lookups
    ])
    transfer_count = sum([
        1,  # client in
        hot_set_consumer_count,  # client out
        archive_consumer_count,  # client out
        1,  # archive writes
        network_total_fn(traditional_transfers, inkless_transfers)
    ])

    network_capacity_bytes_per_sec_per_broker = network_capacity_fn()

    # Assume all transfers are symmetric, and budget double the network capacity.
    producer_bytes_per_sec_per_broker = 2 * network_capacity_bytes_per_sec_per_broker / transfer_count
    # TODO: remove hardcoded total producer throughput
    producer_bytes_per_sec_per_broker = 10_000_000 / brokers_per_cluster

    bytes_per_sec_per_cluster = producer_bytes_per_sec_per_broker * brokers_per_cluster
    bytes_per_hour_per_cluster = HRS_TO_SECS * bytes_per_sec_per_cluster

    fixed_instance_cost_per_month = fixed_instance_price_fn(
        3  # 3 controllers and 3 reserved brokers colocated
    )

    variable_instance_cost_per_month = variable_instance_price_fn(
        brokers_per_cluster
    )

    replication_inbound_bytes_per_sec_per_cluster = bytes_per_sec_per_cluster * follower_count
    replication_outbound_bytes_per_sec_per_cluster = replication_inbound_bytes_per_sec_per_cluster

    network_io_cost_per_month = network_price_fn(
        replication_outbound_bytes_per_sec_per_cluster,
        replication_inbound_bytes_per_sec_per_cluster
    )

    # Hot Set size & IO
    hot_set_bytes_per_cluster = hot_set_retention_hours * bytes_per_hour_per_cluster
    hot_set_block_writes_per_sec_per_cluster = bytes_per_sec_per_cluster / hot_set_bytes_per_block
    hot_set_block_reads_per_sec_per_cluster = follower_count * hot_set_block_writes_per_sec_per_cluster
    hot_set_block_deletes_per_sec_per_cluster = hot_set_block_writes_per_sec_per_cluster

    produce_latency = hot_set_bytes_per_block / producer_bytes_per_sec_per_broker

    hot_set_storage_cost_per_month = hot_set_storage_price_fn(
        brokers_per_cluster,
        replication_factor,
        hot_set_bytes_per_cluster
    )

    hot_set_io_cost_per_month = hot_set_io_price_fn(
        brokers_per_cluster,
        replication_factor,
        hot_set_bytes_per_block,
        hot_set_block_writes_per_sec_per_cluster,
        hot_set_block_reads_per_sec_per_cluster,
        hot_set_block_deletes_per_sec_per_cluster
    )

    # Archive size & IO
    archive_bytes_per_cluster = archive_retention_hours * bytes_per_hour_per_cluster
    archive_bytes_per_sec_per_cluster = bytes_per_sec_per_cluster if archive_retention_hours > 0 else 0
    archive_block_writes_per_sec_per_cluster = archive_bytes_per_sec_per_cluster / archive_bytes_per_block
    archive_block_reads_per_sec_per_cluster = archive_consumer_count * archive_block_writes_per_sec_per_cluster
    archive_block_deletes_per_sec_per_cluster = archive_block_writes_per_sec_per_cluster

    archive_storage_cost_per_month = archive_storage_price_fn(
        brokers_per_cluster,
        replication_factor,
        archive_bytes_per_cluster
    )

    archive_io_cost_per_month = archive_io_price_fn(
        brokers_per_cluster,
        replication_factor,
        archive_bytes_per_block,
        archive_block_writes_per_sec_per_cluster,
        archive_block_reads_per_sec_per_cluster,
        archive_block_deletes_per_sec_per_cluster
    )

    total_cost_per_month = sum([
        fixed_instance_cost_per_month,
        variable_instance_cost_per_month,
        network_io_cost_per_month,
        hot_set_storage_cost_per_month,
        hot_set_io_cost_per_month,
        archive_storage_cost_per_month,
        archive_io_cost_per_month
    ])

    # (month sec-1) * ($ month-1) / (byte sec-1) * (gb byte-1)
    total_cost_per_gb = SECS_TO_MONTH * total_cost_per_month / (bytes_per_sec_per_cluster * BYTES_TO_GB)

    return (
        producer_bytes_per_sec_per_broker,
        produce_latency,
        fixed_instance_cost_per_month,
        variable_instance_cost_per_month,
        network_io_cost_per_month,
        hot_set_storage_cost_per_month,
        hot_set_io_cost_per_month,
        archive_storage_cost_per_month,
        archive_io_cost_per_month,
        total_cost_per_month,
        total_cost_per_gb
    )


# Indicates that the hardware configuration specified is not valid for the given workload,
# and some published limit would be exceeded.
class InvalidHardware(RuntimeError):
    pass


class Estimator:
    def __init__(self, region, show_errors):
        base_path = pathlib.Path("published/aws/")
        self.region = region
        self.show_errors = show_errors
        with open(base_path.joinpath(region).joinpath("ec2.json")) as ec2_file:
            self.ec2 = json.load(ec2_file)
        with open(base_path.joinpath(region).joinpath("s3.json")) as s3_file:
            self.s3 = json.load(s3_file)
        with open(base_path.joinpath("instance-types.html")) as instance_types_file:
            self.network = self._parse_network_specs(BeautifulSoup(instance_types_file, 'html.parser'))

        self.currencies = ["USD"]
        # This is an utter hack because there doesn't appear to be a programmatic association here.
        self.region_prefix = {
            "us-east-1": "",
            "eu-central-1": "EUC1-",
        }[region]
        self.s3_price_tiers = {
            "Standard": ("Requests-Tier1", "Requests-Tier2"),
            "Intelligent-Tiering": ("Requests-INT-Tier1", "Requests-INT-Tier2"),
            "Intelligent-Tiering Frequent Access": ("Requests-INT-Tier1", "Requests-INT-Tier2"),
            "Intelligent-Tiering Infrequent Access": ("Requests-INT-Tier1", "Requests-INT-Tier2"),
            "Intelligent-Tiering Archive Instant Access": ("Requests-INT-Tier1", "Requests-INT-Tier2"),
            "IntelligentTieringArchiveAccess": ("Requests-INT-Tier1", "Requests-INT-Tier2"),
            "IntelligentTieringDeepArchiveAccess": ("Requests-INT-Tier1", "Requests-INT-Tier2"),
            "Standard - Infrequent Access": ("Requests-SIA-Tier1", "Requests-SIA-Tier2"),
            "One Zone - Infrequent Access": ("Requests-ZIA-Tier1", "Requests-ZIA-Tier2"),
            "Express One Zone": ("Requests-XZ-Tier1", "Requests-XZ-Tier2"),
            "Amazon Glacier": ("Requests-GLACIER-Tier1", "Requests-GLACIER-Tier2"),
            "Amazon Glacier Staging": ("Requests-GLACIER-Tier1", "Requests-GLACIER-Tier2"),
            "Glacier Instant Retrieval": ("Requests-GIR-Tier1", "Requests-GIR-Tier2"),
            "Reduced Redundancy": ("Requests-Tier1", "Requests-Tier2"),
        }

    def _parse_network_specs(self, network_file):

        def parse_baseline_bandwidth(string):
            if "Low" in string or "Moderate" in string:
                return 1000
            baseline_burst_bandwidth = string.split(" / ")
            if len(baseline_burst_bandwidth) == 1:
                maximum = baseline_burst_bandwidth[0].split(" ")
                if len(maximum) == 2 and maximum[1] == "Gigabit":
                    return float(maximum[0]) * 1e9 / 8
            elif len(baseline_burst_bandwidth) == 2:
                # Take the baseline as the maximum, because we're looking for steady-state behavior.
                return float(baseline_burst_bandwidth[0]) * 1e9 / 8
            raise RuntimeError("Unknown baseline burst column value " + string)
        network_table = network_file.find(id="gp_network").find_next_sibling(class_="table-container").div.table.find_all("tr")
        network = {}
        for row in network_table:
            if len(row.find_all("td")) != 9:
                continue
            first_col = row.find("td")
            instance_type = first_col.next_element.string.replace(" ", "")
            bandwidth = first_col.next_sibling.next_sibling.next_element.string
            network[instance_type] = parse_baseline_bandwidth(bandwidth)
        return network

    def ebs_storage_types(self):
        out = set()
        for sku, product in self.find_products(self.ec2, "Storage", {
            "volumeApiName": None
        }):
            out.add(product["volumeApiName"])
        return out

    def s3_storage_classes(self):
        out = set()
        for sku, product in self.find_products(self.s3, "Storage", {
            "volumeType": None
        }):
            volume_type = product["volumeType"]
            if "ObjectOverhead" not in volume_type:
                out.add(volume_type)
        out.remove("Tags")
        out.remove("Glacier Deep Archive")  # Missing Tier1 request pricing (?????)
        return out

    def find_products(self, data, family, criteria):
        def _match(product):
            if "productFamily" in product:
                if family is not None and product["productFamily"] != family:
                    return False
            else:
                if family is not None:
                    return False
            attributes = product["attributes"]
            for attribute in criteria:
                if attribute not in attributes:
                    return False
                desired_attribute = criteria[attribute]
                if desired_attribute is None:
                    pass
                elif isinstance(desired_attribute, str):
                    # Exact match
                    if desired_attribute != attributes[attribute]:
                        return False
                elif callable(desired_attribute):
                    if not desired_attribute(attributes[attribute]):
                        return False
                else:
                    raise RuntimeError("Unknown criteria " + str(desired_attribute))
            return True

        products = data["products"]
        return [(sku, products[sku]["attributes"]) for sku in products if _match(products[sku])]

    def find_product(self, data, family, criteria):
        found = self.find_products(data, family, criteria)
        if len(found) != 1:
            raise RuntimeError("Criteria " + str(criteria) + " matches products: " + _summarize_objects(found))
        return found[0]

    def _run_price_fns(
            self,
            values,
            reduced_durability_fn,
            fixed_instance_price_fn,
            variable_instance_price_fn,
            network_total_fn,
            network_capacity_fn,
            network_price_fn,
            hot_set_storage_price_fn,
            hot_set_io_price_fn,
            archive_storage_price_fn,
            archive_io_price_fn
    ):
        row = [""] * 8  # Preallocate array
        row.extend(values)
        for brokers_per_cluster in [6]:
            row[0] = str(brokers_per_cluster)
            for replication_factor in [3]:
                row[1] = str(replication_factor)
                for hot_set_retention_hours in [1e0]:  # ~1hr, ~1wk
                    row[2] = str(hot_set_retention_hours)
                    for hot_set_bytes_per_block in [1e4, 1e5, 5e5, 1e6, 2e6, 4e6, 8e6, 64e6]:
                        row[3] = str(hot_set_bytes_per_block)
                        for hot_set_consumer_count in [3]:
                            row[4] = str(hot_set_consumer_count)
                            for archive_retention_hours in [0]:  # ~1 month, ~1 year, ~10 year
                                row[5] = str(archive_retention_hours)
                                for archive_bytes_per_block in [1e9]:
                                    row[6] = str(archive_bytes_per_block)
                                    for archive_consumer_count in [0]:
                                        row[7] = str(archive_consumer_count)
                                        try:
                                            res = _simulate(
                                                brokers_per_cluster=brokers_per_cluster,
                                                replication_factor=replication_factor,
                                                hot_set_retention_hours=hot_set_retention_hours,
                                                hot_set_bytes_per_block=hot_set_bytes_per_block,
                                                hot_set_consumer_count=hot_set_consumer_count,
                                                archive_retention_hours=archive_retention_hours,
                                                archive_bytes_per_block=archive_bytes_per_block,
                                                archive_consumer_count=archive_consumer_count,
                                                reduced_durability_fn=reduced_durability_fn,
                                                fixed_instance_price_fn=fixed_instance_price_fn,
                                                variable_instance_price_fn=variable_instance_price_fn,
                                                network_total_fn=network_total_fn,
                                                network_capacity_fn=network_capacity_fn,
                                                network_price_fn=network_price_fn,
                                                hot_set_storage_price_fn=hot_set_storage_price_fn,
                                                hot_set_io_price_fn=hot_set_io_price_fn,
                                                archive_storage_price_fn=archive_storage_price_fn,
                                                archive_io_price_fn=archive_io_price_fn
                                            )
                                            print("\t".join(row) + "\t" + "\t".join(str(x) for x in res))
                                        except InvalidHardware as e:
                                            if self.show_errors:
                                                print("\t".join(row) + "\t" + str(e))

    def run(self):
        headers = [
            "brokers_per_cluster",
            "replication_factor",
            "hot_set_retention_hours",
            "hot_set_bytes_per_block",
            "hot_set_consumer_count",
            "archive_retention_hours",
            "archive_bytes_per_block",
            "archive_consumer_count",
            "solution_type",
            "instance_type",
            "hot_set_type",
            "archive_type",
            "producer_bytes_per_sec_per_broker",
            "produce_latency_seconds",
            "fixed_instance_cost_per_month",
            "variable_instance_cost_per_month",
            "network_io_cost_per_month",
            "hot_set_storage_cost_per_month",
            "hot_set_io_cost_per_month",
            "archive_storage_cost_per_month",
            "archive_io_cost_per_month",
            "total_cost_per_month",
            "total_cost_per_gb"
        ]
        print("\t".join(headers))
        archive_type = "Intelligent-Tiering"
        archive_storage_price_fn = self.s3_storage_price(archive_type)
        archive_io_price_fn = self.s3_iops_price(archive_type)
        for instance_type in ["m7i.xlarge"]:
            instance_price_fn = self.ec2_instance_price(instance_type)
            fixed_instance_price_fn = self.ec2_instance_price("m7i.large")
            network_capacity_fn = self.ec2_network_capacity(instance_type)
            network_price_fn = self.ec2_network_price()
            for storage_type in ["standard", "sc1", "st1", "gp2", "gp3", "io1", "io2"]:
                self._run_price_fns(
                    ["traditional", instance_type, "ebs-standard" if storage_type == "standard" else storage_type, archive_type],
                    reduced_durability_fn=self.ebs_durability(storage_type),
                    fixed_instance_price_fn=fixed_instance_price_fn,
                    variable_instance_price_fn=instance_price_fn,
                    network_total_fn=lambda traditional, inkless: traditional,
                    network_capacity_fn=network_capacity_fn,
                    network_price_fn=network_price_fn,
                    hot_set_storage_price_fn=self.ebs_storage_price(storage_type),
                    hot_set_io_price_fn=self.ebs_iops_price(storage_type),
                    archive_storage_price_fn=archive_storage_price_fn,
                    archive_io_price_fn=archive_io_price_fn
                )

            for storage_type in ["standard", "sc1", "st1", "gp2", "gp3", "io1", "io2"]:
                self._run_price_fns(
                    ["unoptimized", instance_type, "ebs-standard" if storage_type == "standard" else storage_type, archive_type],
                    reduced_durability_fn=self.ebs_durability(storage_type),
                    fixed_instance_price_fn=fixed_instance_price_fn,
                    variable_instance_price_fn=instance_price_fn,
                    network_total_fn=lambda traditional, inkless: traditional,
                    network_capacity_fn=network_capacity_fn,
                    # Assume all replication data is cross-az, and 2/3 of producer data is cross-az.
                    network_price_fn=lambda outbound, inbound: network_price_fn(outbound * 5/3, inbound * 5/3),
                    hot_set_storage_price_fn=self.ebs_storage_price(storage_type),
                    hot_set_io_price_fn=self.ebs_iops_price(storage_type),
                    archive_storage_price_fn=archive_storage_price_fn,
                    archive_io_price_fn=archive_io_price_fn
                )

            for s3_class in ["Standard", "Intelligent-Tiering"]:
                self._run_price_fns(
                    ["inkless", instance_type, "s3-standard" if s3_class == "Standard" else s3_class, archive_type],
                    reduced_durability_fn=self.s3_durability(s3_class),
                    fixed_instance_price_fn=fixed_instance_price_fn,
                    variable_instance_price_fn=instance_price_fn,
                    network_total_fn=lambda traditional, inkless: inkless,
                    network_capacity_fn=network_capacity_fn,
                    network_price_fn=lambda outbound, inbound: 0,  # no cross AZ.
                    hot_set_storage_price_fn=self.s3_storage_price(s3_class),
                    hot_set_io_price_fn=self.s3_iops_price(s3_class),
                    archive_storage_price_fn=archive_storage_price_fn,
                    archive_io_price_fn=archive_io_price_fn
                )

    def ec2_instance_price(self, instance_type):
        sku, product = self.find_product(self.ec2, "Compute Instance", {
            "instanceType": instance_type,
            "usagetype": self.region_prefix + "DedicatedUsage:" + instance_type,
            "operatingSystem": "Linux",
            "preInstalledSw": "NA"
        })
        terms = self.ec2["terms"]["OnDemand"][sku]
        fns = []
        for term_code in terms:
            price_dimensions = terms[term_code]["priceDimensions"]
            for rate_code in price_dimensions:
                fns.append(self._ec2_instance_price(price_dimensions[rate_code]))
        if len(fns) == 0:
            raise RuntimeError("Missing instance price calculation for " + instance_type)
        return _combine_fns(fns)

    def _ec2_instance_price(self, rate):
        return lambda instance_count: instance_count * self.calculate_cost(rate, ["Hrs"], MONTHLY_HOURS)

    def ec2_network_capacity(self, instance_type):
        if instance_type not in self.network:
            raise RuntimeError("Instance type has no defined network bandwidth " + instance_type)
        max_bytes_per_sec = self.network[instance_type]

        def _compute():
            return max_bytes_per_sec
        return _compute

    def ec2_network_price(self):
        transfer_sku, transfer_product = self.find_product(self.ec2, "Data Transfer", {
            "transferType": "IntraRegion"
        })

        terms = self.ec2["terms"]["OnDemand"][transfer_sku]
        fns = []
        for term_code in terms:
            price_dimensions = terms[term_code]["priceDimensions"]
            for rate_code in price_dimensions:
                fns.append(self._ec2_network_price(price_dimensions[rate_code]))
        if len(fns) == 0:
            raise RuntimeError("Missing transfer price calculation")
        return _combine_fns(fns)

    def _ec2_network_price(self, rate):
        def _compute(outbound_bytes_per_sec, inbound_bytes_per_sec):
            return self.calculate_cost(rate, ["GB"], (inbound_bytes_per_sec + outbound_bytes_per_sec) * BYTES_TO_GB / SECS_TO_MONTH)
        return _compute

    def ebs_durability(self, storage_type):
        def _compute():
            return True
        return _compute

    def ebs_storage_price(self, storage_type):
        fns = []
        sku, product = self.find_product(self.ec2, "Storage", {
            "usagetype": lambda x: "EBS:VolumeUsage" in x,
            "volumeApiName": storage_type
        })
        max_size_string = product["maxVolumeSize"]
        if "TiB" in max_size_string:
            multiplier = 1024 * 1024 * 1024 * 1024
        else:
            raise RuntimeError("Failed to parse max volume size from " + max_size_string)
        space_array = max_size_string.split(" ")
        max_size = int(space_array[0]) * multiplier

        terms = self.ec2["terms"]["OnDemand"][sku]
        for term_code in terms:
            price_dimensions = terms[term_code]["priceDimensions"]
            if len(price_dimensions) != 1:
                raise RuntimeError("Too many price dimensions for " + sku)
            for rate_code in price_dimensions:
                fns.append(self._ebs_storage_price(max_size, price_dimensions[rate_code]))
        if len(fns) == 0:
            raise RuntimeError("Missing storage price calculation for " + storage_type)
        return _combine_fns(fns)

    def _ebs_storage_price(self, max_size, rate):
        def _compute(number_of_brokers, redundancy, total_size):
            bytes_per_broker = redundancy * total_size / number_of_brokers
            # Split the volumes if they become too large
            number_of_volumes = math.ceil(bytes_per_broker / max_size) * number_of_brokers
            volume_size = bytes_per_broker / number_of_volumes
            return number_of_volumes * self.calculate_cost(rate, ["GB-Mo", "GB-month"], volume_size * BYTES_TO_GB)
        return _compute

    def ebs_iops_price(self, storage_type):
        fns = []
        sku, product = self.find_product(self.ec2, "Storage", {
            "usagetype": lambda x: "EBS:VolumeUsage" in x,
            "volumeApiName": storage_type
        })
        max_iops_string = product["maxIopsvolume"]
        try:
            # Just a plain number
            limit_maximum_iops = int(max_iops_string)
        except ValueError:
            hyphen_array = max_iops_string.split("-")
            try:
                # A range low-high
                limit_maximum_iops = int(hyphen_array[-1])
            except ValueError:
                try:
                    limit_maximum_iops = int(hyphen_array[0])
                except ValueError:
                    raise RuntimeError("Cannot parse iops limit from " + max_iops_string)
        max_throughput_string = product["maxThroughputvolume"]
        if "MiB" in max_throughput_string or "MB" in max_throughput_string:
            # There is a note that when talking about throughput, MB is actually MiB
            multiplier = 1024 * 1024
        else:
            raise RuntimeError("Cannot parse max throughput from " + max_throughput_string)
        space_array = max_throughput_string.split(" ")
        try:
            # If there is a range, parse the upper limit first
            limit_maximum_throughput = int(space_array[2]) * multiplier
        except IndexError or ValueError:
            try:
                limit_maximum_throughput = int(space_array[0]) * multiplier
            except ValueError:
                raise RuntimeError("Cannot parse max throughput from " + max_throughput_string)
        # SSDs are benchmarked for 16K blocks, while HDDs are benchmarked with 1MiB blocks.
        limit_block_size = 16384 if "io" in storage_type or "gp" in storage_type else 1024 * 1024
        for sku, product in self.find_products(self.ec2, "System Operation", {
            "usagetype": lambda x: "EBS:VolumeP-IOPS" in x,
            "volumeApiName": storage_type
        }):
            terms = self.ec2["terms"]["OnDemand"][sku]
            for term_code in terms:
                price_dimensions = terms[term_code]["priceDimensions"]
                if len(price_dimensions) != 1:
                    raise RuntimeError("Too many price dimensions for " + sku)
                for rate_code in price_dimensions:
                    rate = price_dimensions[rate_code]
                    rate_maximum_iops = None
                    rate_minimum_iops = 0
                    # They have minimum_iops and maximum fields in the price dimensions, but don't use them for iops
                    if "description" in rate:
                        if "IOPS-month provisioned (io2)" in rate["description"]:
                            rate_maximum_iops = 32000
                        elif "32001-64000 IOPS" in rate["description"]:
                            rate_minimum_iops = 32000
                            rate_maximum_iops = 64000
                        elif "over 64000 IOPS" in rate["description"]:
                            rate_minimum_iops = 64000
                        elif storage_type == "gp3":
                            rate_minimum_iops = 3000
                        elif storage_type == "io1":
                            pass
                        else:
                            raise RuntimeError("Unknown description " + str(rate) + " " + storage_type)
                    fns.append(self._ebs_iops_price(limit_maximum_iops, limit_block_size, limit_maximum_throughput,
                                                    rate_minimum_iops, rate_maximum_iops, rate))
        if len(fns) == 0:
            return lambda broker_count, redundancy, request_size, write_iops, read_iops, delete_iops: 0
        return _combine_fns(fns)

    def _ebs_iops_price(self, maximum_iops, block_size, max_throughput, minimum, maximum, rate):
        def _compute(broker_count, redundancy, request_size, write_iops, read_iops, delete_iops):
            # If reading above or below a block worth of data, round it up to the nearest block
            # This imposes a throughput penalty on very small reads, as if they caused a much larger read to happen
            block_count = math.ceil(1.0 * request_size / block_size)
            total_iops_per_broker = (read_iops + redundancy * write_iops + redundancy * delete_iops) / broker_count * block_count
            number_of_volumes = math.ceil(total_iops_per_broker / maximum_iops)
            per_volume_iops = total_iops_per_broker / number_of_volumes
            total_throughput = per_volume_iops * block_size
            if total_throughput > max_throughput:
                raise InvalidHardware("Necessary throughput %d exceeds maximum throughput %d" % (total_throughput, max_throughput))
            # TODO: does billing happen for the application IOPS or disk IOPS? Assuming disk
            billing_iops = max(0, per_volume_iops - minimum)
            if maximum:
                billing_iops = min(maximum, billing_iops)
            return broker_count * number_of_volumes * self.calculate_cost(rate, ["IOPS-Mo"], billing_iops)
        return _compute

    def s3_durability(self, s3_class):
        def _compute():
            return s3_class == "Reduced Redundancy" or s3_class == "Express One Zone"
        return _compute

    def s3_storage_price(self, s3_class):
        sku, product = self.find_product(self.s3, "Storage", {
            "volumeType": s3_class
        })
        reduced_durability = self.s3_durability(s3_class)()
        terms = self.s3["terms"]["OnDemand"][sku]
        fns = []
        for term_code in terms:
            price_dimensions = terms[term_code]["priceDimensions"]
            for rate_code in price_dimensions:
                fns.append(self._s3_storage_price(reduced_durability, price_dimensions[rate_code]))
        if len(fns) == 0:
            raise RuntimeError("Missing storage price calculation for " + s3_class)
        return _combine_fns(fns)

    def _s3_storage_price(self, reduced_durability, rate):
        def _compute(broker_count, redundancy, bytes_total):
            duplication = redundancy if reduced_durability else 1
            return self.calculate_cost(rate, ["Gigabyte Month", "GB-Mo"], duplication * bytes_total * BYTES_TO_GB)
        return _compute

    def s3_iops_price(self, s3_class):
        def usage_type_to_rate(usage_type):
            sku, product = self.find_product(self.s3, None, {
                "usagetype": usage_type,
                "operation": "PutObject" if usage_type == "Requests-GLACIER-Tier1" else ""
            })
            terms = self.s3["terms"]["OnDemand"][sku]
            for term_code in terms:
                price_dimensions = terms[term_code]["priceDimensions"]
                if len(price_dimensions) != 1:
                    raise RuntimeError("Too many price dimensions for " + usage_type)
                for rate_code in price_dimensions:
                    return price_dimensions[rate_code]

        if s3_class not in self.s3_price_tiers:
            raise RuntimeError("Missing API price usage type for " + s3_class)
        tier_1_usage_type, tier_2_usage_type = self.s3_price_tiers[s3_class]
        tier_1_rate = usage_type_to_rate(self.region_prefix + tier_1_usage_type)
        tier_2_rate = usage_type_to_rate(self.region_prefix + tier_2_usage_type)
        reduced_durability = self.s3_durability(s3_class)()

        def _compute(broker_count, redundancy, request_size, write_iops, read_iops, delete_iops):
            duplication = redundancy if reduced_durability else 1
            tier_1_requests_per_month = duplication * write_iops / SECS_TO_MONTH
            tier_2_requests_per_month = (read_iops + (duplication * delete_iops)) / SECS_TO_MONTH
            return self.calculate_cost(tier_1_rate, ["Requests"], tier_1_requests_per_month) + \
                self.calculate_cost(tier_2_rate, ["Requests"], tier_2_requests_per_month)
        return _compute

    def calculate_cost(self, rate, expected_units, value):
        begin = int(rate["beginRange"])
        billed_value = max(0, value - begin)
        if rate["endRange"] != "Inf":
            end = int(rate["endRange"])
            billed_value = min(billed_value, end)
        if rate["unit"] not in expected_units:
            raise RuntimeError("Unknown rate unit " + str(rate) + " not one of " + str(expected_units))
        ppu = rate["pricePerUnit"]
        for currency in self.currencies:
            if currency in ppu:
                marginal_cost = float(ppu[currency])
                return marginal_cost * billed_value
        raise RuntimeError("Unknown currency: " + ppu)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("region")
    parser.add_argument("--show-errors", action="store_true", default=False)
    parsed = parser.parse_args()
    estimator = Estimator(parsed.region, parsed.show_errors)
    estimator.run()


if __name__ == "__main__":
    main()
