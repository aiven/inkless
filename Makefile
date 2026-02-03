.PHONY: all
all: clean fmt test pitest build_release

VERSION := 4.1.0-inkless-SNAPSHOT

# Docker image settings (aligned with .github/workflows/inkless-release.yml)
REGISTRY := ghcr.io
IMAGE_NAME := $(REGISTRY)/aiven/inkless
DOCKER := docker

.PHONY: build
build:
	./gradlew :core:build :storage:inkless:build :metadata:build -x test -x generateJooqClasses

core/build/distributions/kafka_2.13-$(VERSION).tgz:
	echo "Building Kafka distribution with version $(VERSION)"
	./gradlew releaseTarGz

.PHONY: build_release
build_release: core/build/distributions/kafka_2.13-$(VERSION).tgz

# Get the actual version from the built tarball (handles SNAPSHOT versions)
# Exclude -site-docs.tgz; uses sort -V for deterministic selection when multiple tarballs exist
DIST_VERSION = $(shell find core/build/distributions -name "kafka_2.13-*.tgz" ! -name "*-site-docs.tgz" 2>/dev/null | sort -V | tail -1 | xargs basename 2>/dev/null | sed 's/kafka_2.13-\(.*\)\.tgz/\1/')

# Prepare docker build context (aligned with workflow)
# Only copy the main distribution tarball, exclude site-docs
.PHONY: docker_build_prep
docker_build_prep: build_release
	mkdir -p docker/resources/distributions
	find core/build/distributions -name "kafka_2.13-*.tgz" ! -name "*-site-docs.tgz" -exec cp {} docker/resources/distributions/ \;

.PHONY: docker_clean
docker_clean:
	rm -rf docker/resources/distributions

# Build docker image (used by both local development and CI)
#
# Optional parameters (CI can override these):
#   PLATFORM    - Target platform (e.g., linux/amd64). Default: native
#   DOCKER_TAGS - Space-separated image tags. Default: $(IMAGE_NAME):local $(IMAGE_NAME):$(DIST_VERSION)
#   PUSH        - Set to "true" to push to registry. Default: false (loads locally)
#   BUILD_DATE  - Build timestamp. Default: current time
#   CACHE_FROM  - Buildx cache source (e.g., type=gha,scope=mykey)
#   CACHE_TO    - Buildx cache destination
#
# Local usage:
#   make docker_build
#
# CI usage:
#   make docker_build PLATFORM=linux/amd64 DOCKER_TAGS="ghcr.io/aiven/inkless:4.1.0-0.33-amd64" PUSH=true
#
.PHONY: docker_build
docker_build: docker_build_prep
	@if [ -z "$(DIST_VERSION)" ]; then \
		echo "Error: No distribution tarball found. Run 'make build_release' first."; \
		exit 1; \
	fi
	@echo "Building Docker image with version $(DIST_VERSION)"
	$(DOCKER) buildx build \
		$(if $(PLATFORM),--platform '$(PLATFORM)') \
		--build-arg kafka_version=$(DIST_VERSION) \
		--build-arg build_date=$(or $(BUILD_DATE),$$(date -u +%Y-%m-%dT%H:%M:%SZ)) \
		$(if $(DOCKER_TAGS),$(foreach tag,$(DOCKER_TAGS),-t '$(tag)'),-t '$(IMAGE_NAME):local' -t '$(IMAGE_NAME):$(DIST_VERSION)') \
		$(if $(CACHE_FROM),--cache-from '$(CACHE_FROM)') \
		$(if $(CACHE_TO),--cache-to '$(CACHE_TO)') \
		$(if $(filter true,$(PUSH)),--push,--load) \
		-f docker/inkless/Dockerfile \
		docker

# Build docker image without cache (useful when build fails due to stale cache)
.PHONY: docker_build_nocache
docker_build_nocache: docker_build_prep
	@if [ -z "$(DIST_VERSION)" ]; then \
		echo "Error: No distribution tarball found. Run 'make build_release' first."; \
		exit 1; \
	fi
	@echo "Building Docker image with version $(DIST_VERSION) (no cache)"
	$(DOCKER) buildx build \
		--no-cache \
		--build-arg kafka_version=$(DIST_VERSION) \
		--build-arg build_date=$$(date -u +%Y-%m-%dT%H:%M:%SZ) \
		-t $(IMAGE_NAME):local \
		-t $(IMAGE_NAME):$(DIST_VERSION) \
		--load \
		-f docker/inkless/Dockerfile \
		docker

# Build and push multi-arch docker image (requires buildx with multi-arch support)
# Note: Multi-arch images cannot be loaded locally, they must be pushed to a registry
.PHONY: docker_build_multiarch
docker_build_multiarch: docker_login docker_build_prep
	@if [ -z "$(DIST_VERSION)" ]; then \
		echo "Error: No distribution tarball found. Run 'make build_release' first."; \
		exit 1; \
	fi
	@echo "Building and pushing multi-arch Docker image with version $(DIST_VERSION)"
	$(DOCKER) buildx build \
		--platform linux/amd64,linux/arm64 \
		--build-arg kafka_version=$(DIST_VERSION) \
		--build-arg build_date=$$(date -u +%Y-%m-%dT%H:%M:%SZ) \
		-t $(IMAGE_NAME):$(DIST_VERSION) \
		--push \
		-f docker/inkless/Dockerfile \
		docker

# Login to GitHub Container Registry
# Requires GITHUB_TOKEN environment variable or gh cli authentication
.PHONY: docker_login
docker_login:
	@if [ -n "$$GITHUB_TOKEN" ]; then \
		if [ -z "$(GITHUB_USER)" ]; then \
			echo "Error: GITHUB_TOKEN is set but GITHUB_USER is not. Set both or use gh cli."; \
			exit 1; \
		fi; \
		echo "$$GITHUB_TOKEN" | $(DOCKER) login $(REGISTRY) -u $(GITHUB_USER) --password-stdin; \
	elif command -v gh >/dev/null 2>&1; then \
		gh auth token | $(DOCKER) login $(REGISTRY) -u $$(gh api user -q .login) --password-stdin; \
	else \
		echo "Error: Set GITHUB_TOKEN and GITHUB_USER, or install gh cli"; \
		exit 1; \
	fi

# Push docker image to GHCR (aligned with workflow)
.PHONY: docker_push
docker_push: docker_login docker_build
	@echo "Pushing $(IMAGE_NAME):$(DIST_VERSION)"
	$(DOCKER) push $(IMAGE_NAME):$(DIST_VERSION)

# Alias for docker_build_multiarch (kept for backward compatibility)
.PHONY: docker_push_multiarch
docker_push_multiarch: docker_build_multiarch

.PHONY: docs
docs:
	./gradlew genInklessConfigDoc genInklessTopicConfigDoc genInklessMetricsDoc

.PHONY: fmt
fmt:
	./gradlew \
	  :core:checkstyleMain :core:checkstyleTest :metadata:checkstyleMain :metadata:checkstyleTest :storage:checkstyleMain :storage:checkstyleTest \
	  :core:spotlessJavaApply :metadata:spotlessJavaApply :storage:spotlessJavaApply :storage:inkless:spotlessJavaApply

.PHONY: test
test:
	./gradlew :storage:inkless:test :storage:inkless:integrationTest -x generateJooqClasses
	./gradlew :metadata:test --tests "org.apache.kafka.controller.*"
	./gradlew :core:test --tests "*Inkless*"

.PHONY: pitest
pitest:
	./gradlew :storage:inkless:pitest -x generateJooqClasses

.PHONY: integration_test
integration_test_core:
	./gradlew :core:test --tests "kafka.api.*" --max-workers 1

.PHONY: clean
clean:
	./gradlew clean

.PHONY: jooq
jooq:
	./gradlew :storage:inkless:generateJooqClasses

DEMO := s3-local
.PHONY: demo
demo:
	$(MAKE) -C docker/examples/docker-compose-files/inkless $(DEMO)

core/build/distributions/kafka_2.13-$(VERSION): core/build/distributions/kafka_2.13-$(VERSION).tgz
	tar -xf $< -C core/build/distributions
	touch $@  # prevent rebuilds

# Local development
CLUSTER_ID := ervoWKqFT-qvyKLkTo494w

.PHONY: kafka_storage_format
kafka_storage_format:
	./bin/kafka-storage.sh format -c config/inkless/single-broker-0.properties -t $(CLUSTER_ID)

.PHONY: local_pg
local_pg:
	cd docker/examples/docker-compose-files/inkless && \
		$(DOCKER) compose up -d postgres


.PHONY: local_minio
local_minio:
	cd docker/examples/docker-compose-files/inkless && \
		$(DOCKER) compose -f docker-compose.yml -f docker-compose.s3-local.yml up -d create_bucket

.PHONY: local_gcs
local_gcs:
	cd docker/examples/docker-compose-files/inkless && \
		$(DOCKER) compose -f docker-compose.yml -f docker-compose.gcs-local.yml up -d create_bucket

.PHONY: local_azure
local_azure:
	cd docker/examples/docker-compose-files/inkless && \
		$(DOCKER) compose -f docker-compose.yml -f docker-compose.azure-local.yml up -d create_bucket

.PHONY: cleanup
cleanup:
	cd docker/examples/docker-compose-files/inkless && \
		$(DOCKER) compose down --remove-orphans
	rm -rf ./_data

# make create_topic ARGS="topic"
.PHONY: create_topic
create_topic: core/build/distributions/kafka_2.13-$(VERSION)
	$</bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --config diskless.enable=true --topic $(ARGS)


.PHONY: dump_postgres_schema
dump_postgres_schema:
	$(DOCKER) compose -f dump-schema-compose.yml up
	$(DOCKER) compose -f dump-schema-compose.yml down --remove-orphans
	@echo "Dumped: ./storage/inkless/build/postgres_schema.sql"

.PHONY: antithesis_test_generator
antithesis_test_generator:
	docker build antithesis/test_generator/ -t inkless-antithesis-testgen:latest

.PHONY: antithesis_generate_tests
antithesis_generate_tests: antithesis_test_generator
	$(DOCKER) run --rm -ti -u "$(shell id -u):$(shell id -g)" -v "$(PWD):/workdir" \
		inkless-antithesis-testgen:latest \
		/workdir/antithesis/test_generator/docker_script.sh

.PHONY: antithesis_build_for_tests
antithesis_build_for_tests:
	./gradlew systemTestLibs

.PHONY: antithesis_base_ducker_image
antithesis_base_ducker_image:
	tests/docker/ducker-ak up
	tests/docker/ducker-ak down

.PHONY: antithesis_docker_images
antithesis_docker_images:
	$(DOCKER) build . -f antithesis/Dockerfile-driver \
		-t us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/inkless-systest-driver:latest
	$(DOCKER) build . -f antithesis/Dockerfile-vm \
		-t us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/inkless-systest-vm:latest

.PHONY: antithesis_push_docker_images
antithesis_push_docker_images:
	$(DOCKER) push us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/inkless-systest-driver:latest
	$(DOCKER) push us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/inkless-systest-vm:latest

CONFIG_IMAGE := 'us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/inkless-systest-config:latest'
.PHONY: antithesis_build_and_push_config_docker_image
antithesis_build_and_push_config_docker_image:
	$(DOCKER) build . -f antithesis/Dockerfile-config -t $(CONFIG_IMAGE)
	$(DOCKER) push $(CONFIG_IMAGE)
