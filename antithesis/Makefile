VERSION := 4.1.0-inkless-SNAPSHOT

.PHONY: build_inkless_docker
build_inkless_docker:
	rm ../core/build/distributions/kafka_2.13-$(VERSION).tgz
	$(MAKE) -C ../ docker_build
	docker build  --build-arg VERSION=$(VERSION) -f Dockerfile-inkless . \
		-t us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/kafka:$(VERSION)

.PHONY: push_inkless_docker
push_inkless_docker:
	docker push us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/kafka:$(VERSION)

.PHONY: build_workload_docker
build_workload_docker:
	docker build workload -t us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/inkless-workload

.PHONY: build_workload_java_docker
build_workload_java_docker:
	docker build --build-arg VERSION=$(VERSION) workload_java \
		-t us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/inkless-workload-java

.PHONY: push_workload_docker
push_workload_docker:
	docker push us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/inkless-workload

.PHONY: push_workload_java_docker
push_workload_java_docker:
	docker push us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/inkless-workload-java

CONFIG_IMAGE := 'us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/$(SCENARIO)'
.PHONY: build_and_push_config_docker_image
build_and_push_config_docker_image:
	@if [ -z "$(SCENARIO)" ]; then \
		echo "ERROR: SCENARIO must be defined. Use 'make build_and_push_config_docker_image SCENARIO=single-broker-in-memory'"; \
		exit 1; \
	fi
	docker build -f scenarios/Dockerfile scenarios/$(SCENARIO) -t $(CONFIG_IMAGE)
	docker push $(CONFIG_IMAGE)

CONFIG_IMAGE_JAVA := 'us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/java-scenario'
.PHONY: build_and_push_java_config_docker_image
build_and_push_java_config_docker_image:
	docker build -f scenario_java/Dockerfile scenario_java -t $(CONFIG_IMAGE_JAVA)
	docker push $(CONFIG_IMAGE_JAVA)
