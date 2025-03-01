
.PHONY: all
all: output/estimate.tsv output/testrun.tsv

aws_pricing_url = https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/

published_files = published/aws/network-specs.tsv published/aws/instance-types.html published/aws/services.json
published/aws/network-specs.tsv:
	@echo "Go to https://docs.aws.amazon.com/ec2/latest/instancetypes/gp.html#gp_network and copy the table without headers into $@"
	exit 1
published/aws/instance-types.html:
	mkdir -p $(@D)
	curl "https://docs.aws.amazon.com/ec2/latest/instancetypes/gp.html" > $@
published/aws/services.json:
	mkdir -p $(@D)
	curl "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/index.json" > $@

published_files += published/aws/us-east-1/ec2.json published/aws/us-east-1/s3.json
published/aws/us-east-1/ec2.json:
	mkdir -p $(@D)
	curl "$(aws_pricing_url)AmazonEC2/current/us-east-1/index.json" > $@
published/aws/us-east-1/s3.json:
	mkdir -p $(@D)
	curl "$(aws_pricing_url)AmazonS3/current/us-east-1/index.json" > $@

published_files += published/aws/eu-central-1/ec2.json published/aws/eu-central-1/s3.json
published/aws/eu-central-1/ec2.json:
	mkdir -p $(@D)
	curl "$(aws_pricing_url)AmazonEC2/current/eu-central-1/index.json" > $@
published/aws/eu-central-1/s3.json:
	mkdir -p $(@D)
	curl "$(aws_pricing_url)AmazonS3/current/eu-central-1/index.json" > $@

.PHONY: published
published: $(published_files)

measured/pgaas:
	# This will only run once automatically. To receive further updates run "make -B measured/pgaas"
	# Also note that this command needs credentials. Re-authenticate on the AWS CLI if this command fails.
	aws s3 sync s3://aiven-test-cur-export/ measured/

.PHONY: filtered
filtered: measured/pgaas
	@./filter.sh

.PHONY: clean
clean:
	rm -rf published measured filtered output

output/estimate.tsv: estimator.py | published
	mkdir -p $(@D)
	./estimator.py eu-central-1 > $@

one_week_ago=$(shell date -v -1w)

output/testrun.tsv: extract.py | filtered
	mkdir -p $(@D)
	# Show only 1 week of data. To customize this, see ./extract.py --help and run the command yourself.
	./extract.py --start "${one_week_ago}" > $@
