.PHONY: inkless_docker
inkless_docker:
	rm ../core/build/distributions/kafka_2.13-4.1.0-inkless-SNAPSHOT.tgz
	$(MAKE) -C ../ docker_build

.PHONY: workload_docker
workload_docker:
	docker build workload -t antithesis-kafka-workload:latest
