.PHONY: compose
compose:
	docker compose -f docker-compose.yml up

.PHONY: bucket
bucket:
	aws --endpoint-url http://127.0.0.1:4566 s3api create-bucket --bucket inkless --region us-east-1
