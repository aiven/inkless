#!/usr/bin/env python

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import matplotlib.pyplot as plt
import pyarrow.compute as pc
import sys
import argparse
import dateutil.parser

# From a CUR2.0 dataset that has been filtered already, extract a summary report of the system costs incurred.


def extract(start, end, show_timeseries, show_breakdown):
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 400)
    pd.set_option('display.max_colwidth', 400)

    earliest_condition = pc.field('line_item_usage_start_date') >= pa.scalar(start, type=pa.timestamp('ns'))
    latest_condition = pc.field('line_item_usage_end_date') <= pa.scalar(end, type=pa.timestamp('ns'))

    if start is not None and end is not None:
        input_filter = (earliest_condition & latest_condition)
    elif start is not None:
        input_filter = earliest_condition
    elif end is not None:
        input_filter = latest_condition
    else:
        input_filter = None

    # Read the Parquet file into an Arrow Table

    table = pq.ParquetDataset('filtered/', filters=input_filter).read()

    # Convert the Arrow Table to a Pandas DataFrame
    df = table.to_pandas()

    df['user_aiven_service_type'] = df['user_aiven_service_type'].fillna("None")
    df['user_name'] = df['user_name'].fillna("None")

    def all_identical(x):
        unique = x.unique()
        if unique.size != 1:
            raise RuntimeError('Series contains multiple distinct values: ' + unique)
        return unique[0]

    df['identity_line_item_id'] = df['identity_line_item_id'].astype(str)

    if df.empty:
        raise RuntimeError('No input data')
    pt = pd.DataFrame.from_records(df.pivot_table(
        index=[
            'identity_time_interval',
            'line_item_usage_type',
            'line_item_operation',
            'line_item_line_item_description',
            'user_aiven_service_type',
        ], columns=[
        ], aggfunc={
            'line_item_unblended_cost': 'sum',
            'line_item_unblended_rate': all_identical,
            'line_item_usage_amount': 'sum',
        }).to_records())
    pt.to_csv(sys.stdout, sep='\t')

    if pt.empty:
        raise RuntimeError('No data after first pivot')
    if show_timeseries:
        time_series = pt.pivot_table(
            index=[
                'identity_time_interval',
            ], columns=[
                'line_item_usage_type',
                'line_item_line_item_description',
            ], aggfunc={
                'line_item_unblended_cost': 'sum',
            })
        time_series.plot.bar(stacked=True)

    if show_breakdown:
        per_service_type = pt.pivot_table(
            index=[
                'user_aiven_service_type',
            ], columns=[
                'line_item_usage_type',
                'line_item_line_item_description',
            ], aggfunc={
                'line_item_unblended_cost': 'max',
            })
        per_service_type.plot.bar(stacked=True)
    if show_timeseries or show_breakdown:
        plt.show(block=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=dateutil.parser.parse)
    parser.add_argument("--end", type=dateutil.parser.parse)
    parser.add_argument("--show-timeseries", action="store_true", default=False)
    parser.add_argument("--show-breakdown", action="store_true", default=False)
    parsed = parser.parse_args()
    extract(parsed.start, parsed.end, parsed.show_timeseries, parsed.show_breakdown)


if __name__ == '__main__':
    main()
