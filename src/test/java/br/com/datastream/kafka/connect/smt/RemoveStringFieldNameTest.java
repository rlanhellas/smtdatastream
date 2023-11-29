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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class RemoveStringFieldNameTest {

  private RemoveStringFieldName<SourceRecord> xform = new RemoveStringFieldName.Value<>();

  @After
  public void tearDown() throws Exception {
    xform.close();
  }

  @Test(expected = DataException.class)
  public void topLevelStructRequired() {
    xform.apply(new SourceRecord(null, null, "", 0, Schema.INT32_SCHEMA, 42));
  }

  @Test
  public void removeStringFromFieldNameSchema() {
    Map<String,Object> props = new HashMap<>();
    props.put("string.to.remove", "/");
    xform.configure(props);


    Struct innerStructl2 = new Struct(SchemaBuilder.struct()
            .field("inner/Field1l2", Schema.STRING_SCHEMA)
            .build());

    Struct innerStruct = new Struct(SchemaBuilder.struct()
            .field("inner/Field1", Schema.STRING_SCHEMA)
            .field("//innerField2", Schema.INT32_SCHEMA)
            .field("innerField3", innerStructl2.schema())
            .build());

    Struct originalStruct = new Struct(SchemaBuilder.struct()
            .field("field1", Schema.STRING_SCHEMA)
            .field("field2", innerStruct.schema())
            .build());

    originalStruct.put("field1", "value1");
    originalStruct.put("field2", innerStruct
            .put("inner/Field1", "nestedValue1")
            .put("//innerField2", 42)
            .put("innerField3", innerStructl2.put("inner/Field1l2", "test"))
    );


    final SourceRecord record = new SourceRecord(null, null, "test", 0, originalStruct.schema(), originalStruct);
    final SourceRecord transformedRecord = xform.apply(record);

    assertEquals(transformedRecord.valueSchema().field("field2").schema().field("innerField1").index(), 0);
    assertEquals(transformedRecord.valueSchema().field("field2").schema().field("innerField2").index(), 1);
  }

  @Test
  public void schemalessRemoveBackslash() {
    Map<String,Object> props = new HashMap<>();
    props.put("string.to.remove", "/");
    xform.configure(props);

    final SourceRecord record = new SourceRecord(null, null, "test", 0,
      null, Collections.singletonMap("/magic", 42L));

    final SourceRecord transformedRecord = xform.apply(record);
    assertEquals(42L, ((Map) transformedRecord.value()).get("magic"));
  }

  @Test
  public void schemaStringRemoveBackslash() {
    Map<String,Object> props = new HashMap<>();
    props.put("string.to.remove", "/");
    xform.configure(props);

    final SourceRecord record = new SourceRecord(null, null, "test", 0,
            Schema.STRING_SCHEMA, "/magic");

    final SourceRecord transformedRecord = xform.apply(record);
    assertEquals("magic", transformedRecord.value());
  }
}