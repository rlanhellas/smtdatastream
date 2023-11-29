/*
 * Copyright © 2019 Christopher Matta (chris.matta@gmail.com)
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

package br.com.datastream.kafka.connect.smt;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class RemoveStringFieldName<R extends ConnectRecord<R>> implements Transformation<R> {

  private static final Logger log = LoggerFactory.getLogger(RemoveStringFieldName.class);

  public static final String OVERVIEW_DOC =
    "Remove string from json fields";


  private interface ConfigName {
    String STRING_TO_REMOVE = "string.to.remove";
  }

  public static final ConfigDef CONFIG_DEF = new ConfigDef().define(ConfigName.STRING_TO_REMOVE, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
          "String to be removed from field name");

  private static final String PURPOSE = "Remove string from json fields";

  private Cache<Schema, Schema> schemaUpdateCache;

  private String stringToRemove;

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    stringToRemove = config.getString(ConfigName.STRING_TO_REMOVE);

    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
  }


  @Override
  public R apply(R record) {
    if (operatingSchema(record) == null) {
      return applySchemaless(record);
    } else if (operatingSchema(record) == Schema.STRING_SCHEMA) {
      return applyWithStringSchema(record);
    } else {
      return applyWithSchema(record);
    }
  }

  private R applySchemaless(R record) {
    final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

    final Map<String, Object> updatedValue = new HashMap<>();
    for (Map.Entry<String, Object> entry : value.entrySet()) {
      updatedValue.put(entry.getKey().replace(stringToRemove,""),entry.getValue());
    }

    return newRecord(record, null, updatedValue);
  }

  private R applyWithSchema(R record) {
    final Struct value = requireStruct(operatingValue(record), PURPOSE);

    final Struct newValue = replaceFieldNames(value);

    return newRecord(record, newValue.schema(), newValue);
  }

  private R applyWithStringSchema(R record) {
    if (record != null && record.value() != null) {
      final String replacedValue = record.value().toString().replace(stringToRemove, "");
      SchemaAndValue newSchema = new SchemaAndValue(record.valueSchema(), replacedValue);
      return newRecord(record, newSchema.schema(), newSchema.value());
    }else{
      throw new DataException("record and record.value() should not be null");
    }

  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
    schemaUpdateCache = null;
  }

  private String getRandomUuid() {
    return UUID.randomUUID().toString();
  }

  private Schema makeUpdatedSchema(Struct originalStruct, Schema schema) {
    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

    for (Field field: schema.fields()) {
      if (field.schema().type() == Schema.Type.STRUCT){
        if (originalStruct.getStruct(field.name()) != null) {
          builder.field(field.name().replace(stringToRemove, ""), replaceFieldNames(originalStruct.getStruct(field.name()))
                  .schema());
        }
      }else{
        builder.field(field.name().replace(stringToRemove, ""), field.schema());
      }
    }

    return builder.build();
  }

  public Struct replaceFieldNames(Struct originalStruct) {
    Schema originalSchema = originalStruct.schema();
    Schema newSchema = makeUpdatedSchema(originalStruct, originalSchema);

    Struct newStruct = new Struct(newSchema);

    for (Field field : originalSchema.fields()) {
      String originalFieldName = field.name();
      String newFieldName = originalFieldName.replace(stringToRemove, "");

      if (field.schema().type() == Schema.Type.STRUCT) {
        // Handle nested struct
        Struct nestedStruct = originalStruct.getStruct(originalFieldName);
        if (nestedStruct != null) {
          Struct updatedNestedStruct = replaceFieldNames(nestedStruct);
          newStruct.put(newFieldName, updatedNestedStruct);
        }
      } else {
        // Non-struct field
        Object value = originalStruct.get(originalFieldName);
        newStruct.put(newFieldName, value);
      }
    }

    return newStruct;
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  public static class Key<R extends ConnectRecord<R>> extends RemoveStringFieldName<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.key();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
    }

  }

  public static class Value<R extends ConnectRecord<R>> extends RemoveStringFieldName<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }

  }
}


