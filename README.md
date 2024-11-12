# Introduction

Transforms can be used to change the data before it is written by a Kafka Connector.

## Build
要求 Java 11
```bash
./gradlew shadowJar
```
Jar会在 `build/libs` 目录下 `kafka-connect-transform-keyvalue.jar`

然后将 Jar 拷贝到 CONNECT_PLUGIN_PATH 目录下。
对于 `debezium`的官方镜像是： `/kafka/plugin`，Confluent的镜像则是在: `/usr/share/confluent-hub-components/`

DockerFile:

```bash
# 使用 Confluent 的 Kafka Connect 镜像作为基础镜像
FROM confluentinc/cp-kafka-connect:7.7.1

# 安装 Kafka Connect S3 连接器
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.17

COPY ./kafka-connect-transform-keyvalue.jar /usr/share/confluent-hub-components/


# # 暴露端口
# EXPOSE 8083

# # 启动命令
# CMD ["connect-distributed", "/etc/kafka/connect-distributed.properties"]
```

```bash

## Usage

Each transformation can be configured with a properties file, in case a standalone connector is used or with a JSON request if a distributed connector is used. The property names and values are identical for both cases.

A standalone properties file would look like this:
```properties
name=Connector1
connector.class=org.apache.kafka.some.Connector
transforms=MyTransformName
transforms.MyTransformName.type=org.radarbase.kafka.connect.transforms.MyTransformType
```

whereas in the distributed case the JSON file looks as follows:

```json
{
  "name": "Connector1",
  "connector.class": "org.apache.kafka.some.Connector",
  "transforms": "MyTransformName",
  "transforms.MyTransformName.type": "org.radarbase.kafka.connect.transforms.MyTransformType"
}
```

That JSON can be used to start a connector:

```bash
curl -s -X POST -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors
```

Change `http://localhost:8083/` the the endpoint of one of your Kafka Connect worker(s).

The JSON can also be used to update an existing connector:

```bash
curl -s -X POST -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors/Connector1/config
```

### Docker build

This repository creates two docker builds, a base connector image [radarbase/kafka-connect-transform-keyvalue](https://hub.docker.com/r/radarbase/kafka-connect-transform-keyvalue) and an extension of that image that is bundled with the Confluent S3 connector [radarbase/kafka-connect-transform-s3](https://hub.docker.com/r/radarbase/kafka-connect-transform-s3).

## Transformations

### MergeKey

The MergeKey transformation copies all fields from the record key into the record value, and adds a timestamp. If this causes duplicate fields names, the value will be picked, in order of preference, from value, key and finally timestamp.

Example configuration
```properties
transforms=mergeKey
transforms.mergeKey.type=org.radarbase.kafka.connect.transforms.MergeKey
```


### CombineKeyValue

The CombineKeyValue transformation creates a new value with a key field and value field, which will contain the original record key and record value.

Example configuration
```properties
transforms=combineKeyValue
transforms.combineKeyValue.type=org.radarbase.kafka.connect.transforms.CombineKeyValue
```


### TimestampConverter

The TimestampConverter transformation converts milliseconds value fields and floating point seconds value fields to logical timestamp fields. The fields that should be converted can be configured with the `fields` configuration.

Example configuration
```properties
transforms=convertTimestamp
transforms.convertTimestamp.type=org.radarbase.kafka.connect.transforms.TimestampConverter
transforms.convertTimestamp.fields=time,timeReceived,timeCompleted,timestamp
```

从 kafka 读取 Debezium CDC数据并且将数据保存为 Parquet 格式.
```json
{
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "s3.region": "oss-cn-wulanchabu",
    "topics.dir": "kafka-connect-oss",
    "flush.size": "2",
    "tasks.max": "1",
    "timezone": "Asia/Shanghai",
    "locale": "zh_CN",
    "s3.path.style.access.enabled": "false",
    "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
    "aws.access.key.id": "xxx",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "s3.bucket.name": "your-bucket",
    "store.kafka.keys": "false",
    "partition.duration.ms": "1000",
    "schema.compatibility": "NONE",
    "topics": "clip-test",
    "store.url": "https://oss-cn-shanghai.aliyuncs.com",
    "aws.secret.access.key": "xxxx",
    "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
    "name": "shanghai-oss-sink",
    "s3.http.send.expect.continue": "false",
    "keys.format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
    "storage.class": "io.confluent.connect.s3.storage.S3Storage",
    "path.format": "YYYY-MM-dd/HH",
    "rotate.schedule.interval.ms": "10000",
    "transforms": "DebeziumRecord",
    "transforms.DebeziumRecord.type": "org.radarbase.kafka.connect.transforms.DebeziumRecord"
}
```