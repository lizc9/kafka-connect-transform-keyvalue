/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.radarbase.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.transforms.Transformation;
import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.*;

/**
 * This transforms records by copying the record key into the record value.
 */
public class CombineKeyValue<R extends ConnectRecord<R>> implements Transformation<R> {
  private static final String PURPOSE = "copying fields from key to value";
  private static final String KEYVALUE_SCHEMA_NAME = "org.radarbase.kafka.connect.transforms.CombineKeyValue";
  private static final ConfigDef CONFIG_DEF = new ConfigDef();

  @Override
  public R apply(R record) {
    if (record.valueSchema() == null) {
      return applySchemaless(record);
    } else {
      return applyWithSchema(record);
    }
  }

  private R applyWithSchema(R r) {
    Schema schema = SchemaBuilder.struct().name(KEYVALUE_SCHEMA_NAME)
            .field("key", r.keySchema())
            .field("value", r.valueSchema())
            .build();

    final Struct recordKey = requireStruct(r.key(), PURPOSE);
    final Struct recordValue = requireStruct(r.value(), PURPOSE);

    Struct keyValueStruct = new Struct(schema);
    keyValueStruct.put("key", recordKey);
    keyValueStruct.put("value", recordValue);

    return r.newRecord(r.topic(), r.kafkaPartition(), null, null, schema, keyValueStruct, r.timestamp());
  }

  private R applySchemaless(R r) {
    Map<String, Object> key = requireMap(r.key(), PURPOSE);
    Map<String, Object> value = requireMap(r.value(), PURPOSE);
    Map<String, Object> keyValueMap = new HashMap<>();
    keyValueMap.put("key", key);
    keyValueMap.put("value", value);
    return r.newRecord(r.topic(), r.kafkaPartition(), null, null, null, keyValueMap, r.timestamp());
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
    // no close
  }

  @Override
  public void configure(Map<String, ?> map) {
    // no config
  }
}
