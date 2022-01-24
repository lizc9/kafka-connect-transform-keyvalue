ARG BASE_IMAGE=confluentinc/cp-kafka-connect-base:7.0.1

FROM gradle:7.3-jdk11 as builder

COPY ./*.gradle /code/
COPY src/main/java /code/src/main/java
WORKDIR /code
RUN gradle jar --no-watch-fs

FROM ${BASE_IMAGE}

COPY --from=builder /code/build/libs/kafka-connect-transform-keyvalue*.jar /usr/share/"${COMPONENT}"/plugins/
COPY ./src/main/docker/launch /etc/confluent/docker/launch
