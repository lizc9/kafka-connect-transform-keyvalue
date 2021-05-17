ARG BASE_IMAGE=confluentinc/cp-kafka-connect-base:6.1.1-2-ubi8

FROM gradle:7.0-jdk8 as builder

COPY ./*.gradle /code/
COPY src/main/java /code/src/main/java
WORKDIR /code
RUN gradle jar

FROM ${BASE_IMAGE}

COPY --from=builder /code/build/libs/kafka-connect-transform*.jar /usr/share/"${COMPONENT}"/plugins/
COPY ./src/main/docker/launch /etc/confluent/docker/launch