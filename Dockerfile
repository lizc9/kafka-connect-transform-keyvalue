ARG BASE_IMAGE=confluentinc/cp-kafka-connect-base:5.5.1-1-ubi8

FROM gradle:6.4.1-jdk8 as builder

COPY ./*.gradle /code/
COPY src/main/java /code/src/main/java
WORKDIR /code
RUN gradle jar

FROM ${BASE_IMAGE}

COPY --from=builder /code/build/libs/kafka-connect-transform-keyvalue*.jar /usr/share/"${COMPONENT}"/plugins/
