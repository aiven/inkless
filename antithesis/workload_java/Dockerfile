ARG VERSION="set_it_outside"

FROM us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/kafka:${VERSION}

ADD producer_consumer.yaml /home/appuser/
ADD log4j2.yaml /home/appuser/

ENV KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=file:/home/appuser/log4j2.yaml"
CMD ["/opt/kafka/bin/kafka-run-class.sh", "io.aiven.inkless.ProducerConsumerTest", "/home/appuser/producer_consumer.yaml"]
