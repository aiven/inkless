
# Pricing Model & Cost Profiling

This directory contains all pricing & cost related tooling for Inkless.

## Prerequisites

* AWS credentials for AivenDeveloperAccess to the pgaas account
* Python 3, see [requirements.txt](requirements.txt)

## Getting Started

Configure your 2FA device, and give it your latest auth code.

```shell
AUTH_DEVICE_ARN="your-auth-device-arn"
AUTH_CODE="your-auth-device-code"
```

Refresh your AWS CLI credentials with your given 2FA device.

```shell
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN; \
export $(printf "AWS_ACCESS_KEY_ID=%s AWS_SECRET_ACCESS_KEY=%s AWS_SESSION_TOKEN=%s" \
$(aws sts assume-role \ 
    --role-arn arn:aws:iam::450367589208:role/AivenDeveloperAccess \
    --role-session-name "$USER Download cost profiling reports" \
    --query "Credentials.[AccessKeyId,SecretAccessKey,SessionToken]" \
    --output text \
    --serial-number "$AUTH_DEVICE_ARN" \
     --token-code "$AUTH_CODE"))
```

Run the build

```shell
make
```

This should produce two TSV files in the `output/` directory.

## Analysis

These TSV files may be uploaded to Google Sheets for further analysis and comparison.
This can be done in an existing sheet through these steps:

1. Select a blank sheet, or one which should be overwritten
2. File > Import
3. Upload > Browse
4. Select a relevant TSV file
5. Select Import Location "Replace Current Sheet"
6. Confirm "Import Data"

Pivot tables are good for summarizing the data into a more usable form, or performing ad-hoc queries.
If data contains an analysis mistake or doesn't cover a relevant case:

1. Edit the source files `filter.py`, `estimator.py` or `extract.py`
2. Rerun `make`
3. Re-import the data

Data will be recomputed from cached state.
Rerunning the analysis like this does not require active credentials.
All derived computations in the spreadsheet should update automatically after the import.

## Advanced Usage

See the `--help` output of the various commands for analysis other than the example configured in make.

## Clearing Caches

By default, the `make` target pulls data once and caches it.
In order to receive new data, you should force-update when necessary.
This requires the CLI to have appropriate credentials.

```shell
make -B metadata/pgaas
make
```

You can also clear all cached state to reclaim disk space or when switching branches.

```shell
make clean
```

## Data Source

The cost data processed by these scripts comes from [AWS Cost Usage Reports](https://docs.aws.amazon.com/cur/latest/userguide/what-is-data-exports.html).
There are many more columns exposed by AWS that are hidden in these analysis scripts.
Data is exported one or more times daily by an export configured in the pgaas account, and has hourly granularity.
For tests taking place in other AWS accounts, additional data exports must be configured in the AWS console.

After executing a test run, one must wait several hours before the data is available for processing.
Once the data has been exported, it is preserved in the bucket for async analysis at a later time.
There is already historical data present for earlier test runs, though it may be tagged slightly differently.

The only tags available in cost reporting are Cost Allocation Tags.
Tags must be individually opted-in to cost reporting, so tags relevant to the cost analysis should either coincide with an existing opted-in tag, or a new tag opted-in. 

