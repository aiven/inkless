.PHONY: localstack
localstack:
	docker compose -f localstack.yml up

.PHONY: bucket
bucket:
	AWS_ACCESS_KEY_ID='123' AWS_SECRET_KEY='xyz' AWS_SECRET_ACCESS_KEY='abc' AWS_BUCKET_NAME='inkless' aws --endpoint-url http://127.0.0.1:4566 s3api create-bucket --bucket inkless --region us-east-1


VERSION := 4.0.0-inkless-SNAPSHOT

core/build/distributions/kafka_2.13-$(VERSION).tgz:
	./gradlew releaseTarGz

.PHONY: build_release
build_release: core/build/distributions/kafka_2.13-$(VERSION).tgz

.PHONY: docker_build_prep
docker_build_prep:
	cd docker && \
	  [ -d .venv ] || python3 -m venv .venv && \
	  source .venv/bin/activate && \
	  pip install -r requirements.txt

# download prometheus jmx exporter
docker/extra/prometheus-jmx-javaagent/jmx_prometheus_javaagent.jar:
	curl -o docker/extra/prometheus-jmx-javaagent/jmx_prometheus_javaagent.jar \
	  https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar

.PHONY: docker_build
docker_build: build_release docker_build_prep
	cp -R core/build/distributions docker/resources/.
	# use existing docker tooling to build image
	cd docker && \
	  .venv/bin/python3 docker_build_test.py -b kafka/test --image-tag=$(VERSION) --image-type=inkless

