package org.radarbase.kafka.connect.transforms;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.bson.json.JsonReader;
import org.radarbase.kafka.connect.utils.BsonDeserialization;
import org.radarbase.kafka.connect.utils.JsonUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class DebeziumRecord<R extends ConnectRecord<R>> implements Transformation<R> {
  private static final String PURPOSE = "copying fields from key/offset to value";
  private static final ConfigDef CONFIG_DEF = new ConfigDef();
  private static final String VALUE_SCHEMA_NAME = "org.radarbase.kafka.connect.transforms.DebeziumRecordValue";
  private static final String KEY_SCHEMA_NAME = "org.radarbase.kafka.connect.transforms.DebeziumRecordKey";
  private static final String FIELD_SCHEMA = "schema";
  private static final String FIELD_PAYLOAD = "payload";
  private static final String FIELD_BEFORE = "before";
  private static final String FIELD_AFTER = "after";
  private static final String FIELD_OP = "op";
  private static final String FIELD_SOURCE = "source";
  private static final String FIELD_UPDATE_DESCRIPTION = "update_description";
  private static final String FIELD_KAFKA_KEY = "kafka_key";
  private static final String FIELD_KAFKA_TIMESTAMP = "kafka_timestamp";
  private static final String FIELD_KAFKA_OFFSET = "kafka_offset";
  private static final String FIELD_KAFKA_PARTITION = "kafka_partition";

  private static final Schema VALUE_SCHEMA = SchemaBuilder.struct().name(VALUE_SCHEMA_NAME)
          .field(FIELD_BEFORE, Schema.OPTIONAL_STRING_SCHEMA)
          .field(FIELD_AFTER, Schema.OPTIONAL_STRING_SCHEMA)
          .field(FIELD_OP, Schema.OPTIONAL_STRING_SCHEMA)
          .field(FIELD_SOURCE, Schema.OPTIONAL_STRING_SCHEMA)
          .field(FIELD_UPDATE_DESCRIPTION, Schema.OPTIONAL_STRING_SCHEMA)
          .field(FIELD_KAFKA_KEY, Schema.STRING_SCHEMA)
          .field(FIELD_KAFKA_TIMESTAMP, Schema.INT64_SCHEMA)
          .field(FIELD_KAFKA_OFFSET, Schema.INT64_SCHEMA)
          .field(FIELD_KAFKA_PARTITION, Schema.INT32_SCHEMA)
          .build();
  private BsonDeserialization bsonDer;

  @Override
  public R apply(R r) {
    Map<String, Object> key;
    Map<String, Object> value;
    if (r.valueSchema() == null) {
      value = requireMap(r.value(), PURPOSE);
      key = requireMap(r.key(), PURPOSE);
    } else {
      value = structToMap(requireStruct(r.value(), PURPOSE));
      key = structToMap(requireStruct(r.key(), PURPOSE));
    }

    // 统一处理
    key = transformKey(key);
    value = transformValue(value);
    Struct valueStruct = new Struct(VALUE_SCHEMA);
    valueStruct.put(FIELD_BEFORE, getJsonString(value, FIELD_BEFORE));
    valueStruct.put(FIELD_AFTER, getJsonString(value, FIELD_AFTER));
    valueStruct.put(FIELD_OP, value.get(FIELD_OP));
    valueStruct.put(FIELD_SOURCE, getJsonString(value, FIELD_SOURCE));
    // 主要是用户 mongo 判断哪些字段被修改了
    valueStruct.put(FIELD_UPDATE_DESCRIPTION, getJsonString(value, "updateDescription"));
    valueStruct.put(FIELD_KAFKA_KEY, transformJsonString(key));
    valueStruct.put(FIELD_KAFKA_TIMESTAMP, r.timestamp());
    valueStruct.put(FIELD_KAFKA_OFFSET, getOffset(r));
    valueStruct.put(FIELD_KAFKA_PARTITION, r.kafkaPartition());

    return r.newRecord(r.topic(), r.kafkaPartition(), null, null, VALUE_SCHEMA, valueStruct, r.timestamp());

  }

  private Map<String, Object> transformKey(Map<String, Object> key) {
    if (key.containsKey(FIELD_SCHEMA)) {
      key = requireMap(key.get(FIELD_PAYLOAD), PURPOSE);
    }

    for (Map.Entry<String, Object> entry : key.entrySet()) {
      Object value = entry.getValue();
      if (value instanceof String) {
        key.put(entry.getKey(), transformObjectId((String) value));
      }
    }

    return key;
  }

  private String transformObjectId(String value) {
    if (value.startsWith("{") && value.endsWith("}")) {
      return new JsonReader(value).readObjectId().toHexString();
    }
    return value;
  }

  private Map<String, Object> transformValue(Map<String, Object> value) {
    if (value == null) {
      return Collections.emptyMap();
    }
    if (value.containsKey(FIELD_SCHEMA)) {
      value = requireMap(value.get(FIELD_PAYLOAD), PURPOSE);
    }
    return value;
  }

  private String getJsonString(Map<String, Object> value, String field) {
    return transformJsonString(value.get(field));
  }

  private String transformJsonString(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof String) {
      return bsonDer.convertBsonToJson((String) value);
    } else {
      try {
        return JsonUtils.getJsonMapper().writeValueAsString(value);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private Map<String, Object> structToMap(Struct value) {
    if (value == null) {
      return null;
    }
    HashMap<String, Object> result = new HashMap<>();

    List<Field> fields = value.schema().fields();
    for (Field field : fields) {
      Object o = value.get(field);
      if (o instanceof Struct) {
        result.put(field.name(), structToMap((Struct) o));
      } else {
        result.put(field.name(), o);
      }
    }

    return result;
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> map) {
    bsonDer = new BsonDeserialization();
  }

  private Long getOffset(ConnectRecord<?> record) {
    if (record instanceof SourceRecord) {
      return null;
    } else if (record instanceof SinkRecord) {
      SinkRecord sinkRecord = (SinkRecord) record;
      return sinkRecord.originalKafkaOffset();
    } else {
      throw new ConfigException("Unsupported record type: " + record.getClass().getName());
    }
  }
}
