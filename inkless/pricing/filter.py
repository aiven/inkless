#!/usr/bin/env python

import argparse
import pyarrow as pa
import pyarrow.parquet as pq

# Filter a single CUR2.0 parquet file to exclude rows which are irrelevant to the cost analysis.
# Transform the dataset to extract cost allocation tags to queryable columns.
# This step may be applied to each file in a CUR2.0 dataset independently, allowing for incremental processing.


_COST_ALLOCATION_TAGS = [
    'user_name',
    'user_aiven_charge_tier',
    'user_aiven_cloud_name',
    'user_aiven_project',
    'user_aiven_project_id',
    'user_aiven_service_architecture',
    'user_aiven_service_fedora_version',
    'user_aiven_service_id',
    'user_aiven_service_name',
    'user_aiven_service_type',
    'user_aiven_storage_use',
    'user_aiven_system_name',
    'user_aiven_tenant_id',
    'user_ci_infra',
    'user_env_name',
    'user_env_type',
    'user_pipeline_build_number',
    'user_pipeline_test_types',
    'user_rdunklau_test_backup',
    'user_system_name'
]


def main():
    parser = argparse.ArgumentParser(
        prog='filter.py',
        description='Filters a parquet file to only contain benchmarking data')
    parser.add_argument('input_file')
    parser.add_argument('output_file')
    args = parser.parse_args()

    # Read input file into data frame
    table = pq.read_table(args.input_file)
    schema = table.schema
    df = table.to_pandas()
    # Normalize all resource tags into individual columns
    for tag in _COST_ALLOCATION_TAGS:
        df[tag] = df['resource_tags'].apply(lambda o: next(iter([v for k, v in o if k == tag]), None))
        schema = schema.append(pa.field(tag, pa.string(), nullable=True))
    df = df[df['resource_tags'].apply(lambda o: ('user_aiven_project', 'jeqo-kafka-benchmarking') in o)]
    # Write data frame to output file
    table = pa.Table.from_pandas(df, schema)
    pq.write_table(table, args.output_file)


if __name__ == '__main__':
    main()
