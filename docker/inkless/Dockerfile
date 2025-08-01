###############################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
###############################################################################

# Same as the jvm image, but using locally built kafka

FROM eclipse-temurin:21-jre-alpine AS build-jsa

USER root

ARG kafka_version

COPY ./resources/distributions/kafka_2.13-$kafka_version.tgz /kafka.tgz
COPY jsa_launch /etc/kafka/docker/jsa_launch

RUN set -eux ; \
    apk update ; \
    apk upgrade ; \
    apk add --no-cache wget gcompat gpg gpg-agent procps bash; \
    mkdir opt/kafka; \
    tar xfz kafka.tgz -C /opt/kafka --strip-components 1;

# Generate jsa files using dynamic CDS for kafka server start command and kafka storage format command
RUN /etc/kafka/docker/jsa_launch


FROM eclipse-temurin:21-jre-alpine

# exposed ports
EXPOSE 9092

USER root

ARG build_date

ARG kafka_version

COPY ./resources/distributions/kafka_2.13-$kafka_version.tgz /kafka.tgz

LABEL org.label-schema.name="kafka" \
      org.label-schema.description="Apache Kafka" \
      org.label-schema.build-date="${build_date}" \
      org.label-schema.vcs-url="https://github.com/apache/kafka" \
      maintainer="Apache Kafka"

RUN set -eux ; \
    apk update ; \
    apk upgrade ; \
    apk add --no-cache wget gcompat gpg gpg-agent procps bash; \
    mkdir opt/kafka; \
    tar xfz kafka.tgz -C /opt/kafka --strip-components 1; \
    mkdir -p /var/lib/kafka/data /etc/kafka/secrets; \
    mkdir -p /etc/kafka/docker /usr/logs /mnt/shared/config; \
    adduser -h /home/appuser -D --shell /bin/bash appuser; \
    chown appuser:appuser -R /usr/logs /opt/kafka /mnt/shared/config; \
    chown appuser:root -R /var/lib/kafka /etc/kafka/secrets /etc/kafka; \
    chmod -R ug+w /etc/kafka /var/lib/kafka /etc/kafka/secrets; \
    cp /opt/kafka/config/log4j2.yaml /etc/kafka/docker/log4j2.yaml; \
    cp /opt/kafka/config/tools-log4j2.yaml /etc/kafka/docker/tools-log4j2.yaml; \
    apk del wget gpg gpg-agent; \
    apk cache clean;

COPY --from=build-jsa kafka.jsa /opt/kafka/kafka.jsa
COPY --from=build-jsa storage.jsa /opt/kafka/storage.jsa
COPY --chown=appuser:appuser resources/common-scripts /etc/kafka/docker
COPY --chown=appuser:appuser launch /etc/kafka/docker/launch

USER appuser

VOLUME ["/etc/kafka/secrets", "/var/lib/kafka/data", "/mnt/shared/config"]

CMD ["/etc/kafka/docker/run"]
