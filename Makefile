.PHONY: compose
compose:
	docker compose -f docker-compose.yml up

.PHONY: bucket
bucket:
	AWS_ACCESS_KEY_ID='123' AWS_SECRET_KEY='xyz' AWS_SECRET_ACCESS_KEY='abc' AWS_BUCKET_NAME='inkless' aws --endpoint-url http://127.0.0.1:4566 s3api create-bucket --bucket inkless --region us-east-1
