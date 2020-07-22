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
 *
 *
 * This transforms records by copying the record key into the record value.
 */
public class MergeKey<R extends ConnectRecord<R>> implements Transformation<R> {
  private static final String PURPOSE = "copying fields from key to value and converting timestamps";
  private static final String TIMESTAMP_FIELD = "timestamp";
  private static final String KEYVALUE_SCHEMA_NAME = "org.radarbase.kafka.connect.transforms.MergeKey";
  private static final ConfigDef CONFIG_DEF = new ConfigDef();

  @Override
  public R apply(R r) {
    if (r.valueSchema() == null) {
      return applySchemaless(r);
    } else {
      return applyWithSchema(r);
    }
  }

  private R applyWithSchema(R r) {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(KEYVALUE_SCHEMA_NAME);
    schemaBuilder.field(TIMESTAMP_FIELD, Schema.INT64_SCHEMA);
    for (Field field: r.keySchema().fields()) {
      schemaBuilder.field(field.name(), field.schema());
    }
    for (Field field: r.valueSchema().fields()) {
      schemaBuilder.field(field.name(), field.schema());
    }
    Schema schema = schemaBuilder.build();

    final Struct recordKey = requireStruct(r.key(), PURPOSE);
    Struct newValue = new Struct(schema);
    newValue.put(TIMESTAMP_FIELD, r.timestamp());
    for (Field field: r.keySchema().fields()) {
      newValue.put(field.name(), recordKey.get(field));
    }
    final Struct recordValue = requireStruct(r.value(), PURPOSE);
    for (Field field: r.valueSchema().fields()) {
      newValue.put(field.name(), recordValue.get(field));
    }

    return r.newRecord(r.topic(), r.kafkaPartition(), r.keySchema(), r.key(), schema, newValue, r.timestamp());
  }

  private R applySchemaless(R r) {
    Map<String, Object> newValue = new HashMap<>();
    newValue.put(TIMESTAMP_FIELD, r.timestamp());
    newValue.putAll(requireMap(r.key(), PURPOSE));
    newValue.putAll(requireMap(r.value(), PURPOSE));

    return r.newRecord(r.topic(), r.kafkaPartition(), r.keySchema(), r.key(), null, newValue, r.timestamp());
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
  }
}
