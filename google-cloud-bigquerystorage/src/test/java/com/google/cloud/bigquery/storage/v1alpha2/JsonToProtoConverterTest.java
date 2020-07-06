/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigquery.storage.v1alpha2;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.google.cloud.bigquery.storage.test.SchemaTest.*;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class JsonToProtoConverterTest {
  private static ImmutableMap<Table.TableFieldSchema.Type, Descriptor>
      BQTableTypeToProtoDescriptor =
          new ImmutableMap.Builder<Table.TableFieldSchema.Type, Descriptor>()
              .put(Table.TableFieldSchema.Type.BOOL, BoolType.getDescriptor())
              .put(Table.TableFieldSchema.Type.BYTES, BytesType.getDescriptor())
              .put(Table.TableFieldSchema.Type.DATE, Int64Type.getDescriptor())
              .put(Table.TableFieldSchema.Type.DATETIME, Int64Type.getDescriptor())
              .put(Table.TableFieldSchema.Type.DOUBLE, DoubleType.getDescriptor())
              .put(Table.TableFieldSchema.Type.GEOGRAPHY, BytesType.getDescriptor())
              .put(Table.TableFieldSchema.Type.INT64, Int64Type.getDescriptor())
              .put(Table.TableFieldSchema.Type.NUMERIC, BytesType.getDescriptor())
              .put(Table.TableFieldSchema.Type.STRING, StringType.getDescriptor())
              .put(Table.TableFieldSchema.Type.TIME, Int64Type.getDescriptor())
              .put(Table.TableFieldSchema.Type.TIMESTAMP, Int64Type.getDescriptor())
              .build();
  private static ImmutableMap<Table.TableFieldSchema.Type, String> BQTableTypeToDebugMessage =
      new ImmutableMap.Builder<Table.TableFieldSchema.Type, String>()
          .put(Table.TableFieldSchema.Type.BOOL, "boolean")
          .put(Table.TableFieldSchema.Type.BYTES, "string")
          .put(Table.TableFieldSchema.Type.DATE, "int64")
          .put(Table.TableFieldSchema.Type.DATETIME, "int64")
          .put(Table.TableFieldSchema.Type.DOUBLE, "double")
          .put(Table.TableFieldSchema.Type.GEOGRAPHY, "string")
          .put(Table.TableFieldSchema.Type.INT64, "int64")
          .put(Table.TableFieldSchema.Type.NUMERIC, "string")
          .put(Table.TableFieldSchema.Type.STRING, "string")
          .put(Table.TableFieldSchema.Type.TIME, "int64")
          .put(Table.TableFieldSchema.Type.TIMESTAMP, "int64")
          .build();

  private static JSONObject[] simpleJSONObjects = {
    new JSONObject().put("test_field_type", 9223372036854775807L),
    new JSONObject().put("test_field_type", 1.23),
    new JSONObject().put("test_field_type", true),
    new JSONObject().put("test_field_type", "test")
  };

  private static JSONObject[] simpleJSONArrays = {
    new JSONObject()
        .put(
            "test_field_type",
            new JSONArray("[9223372036854775807, 9223372036854775806, 9223372036854775805]")),
    new JSONObject().put("test_field_type", new JSONArray("[1.1, 2.2, 3.3]")),
    new JSONObject().put("test_field_type", new JSONArray("[true, false]")),
    new JSONObject().put("test_field_type", new JSONArray("[hello, test]"))
  };

  private static ImmutableMap<Table.TableFieldSchema.Type, Integer> TableFieldTypeToAccepted =
      new ImmutableMap.Builder<Table.TableFieldSchema.Type, Integer>()
          .put(Table.TableFieldSchema.Type.BOOL, 1)
          .put(Table.TableFieldSchema.Type.BYTES, 1)
          .put(Table.TableFieldSchema.Type.DATE, 1)
          .put(Table.TableFieldSchema.Type.DATETIME, 1)
          .put(Table.TableFieldSchema.Type.DOUBLE, 1)
          .put(Table.TableFieldSchema.Type.GEOGRAPHY, 1)
          .put(Table.TableFieldSchema.Type.INT64, 1)
          .put(Table.TableFieldSchema.Type.NUMERIC, 1)
          .put(Table.TableFieldSchema.Type.STRING, 1)
          .put(Table.TableFieldSchema.Type.TIME, 1)
          .put(Table.TableFieldSchema.Type.TIMESTAMP, 1)
          .build();

  private boolean isDescriptorEqual(Descriptor convertedProto, Descriptor originalProto) {
    for (FieldDescriptor convertedField : convertedProto.getFields()) {
      FieldDescriptor originalField = originalProto.findFieldByName(convertedField.getName());
      if (originalField == null) {
        return false;
      }
      FieldDescriptor.Type convertedType = convertedField.getType();
      FieldDescriptor.Type originalType = originalField.getType();
      if (convertedType != originalType) {
        return false;
      }
      if (convertedType == FieldDescriptor.Type.MESSAGE) {
        if (!isDescriptorEqual(convertedField.getMessageType(), originalField.getMessageType())) {
          return false;
        }
      }
    }
    return true;
  }

  private boolean isProtoJsonEqual(DynamicMessage proto, JSONObject json) {
    for (Map.Entry<FieldDescriptor, java.lang.Object> entry : proto.getAllFields().entrySet()) {
      FieldDescriptor key = entry.getKey();
      java.lang.Object value = entry.getValue();
      if (key.isRepeated()) {
        if (!isProtoArrayJsonArrayEqual(key, value, json)) {
          return false;
        }
      } else {
        if (!isProtoFieldJsonFieldEqual(key, value, json)) {
          return false;
        }
      }
    }
    return true;
  }

  private boolean isProtoFieldJsonFieldEqual(
      FieldDescriptor key, java.lang.Object value, JSONObject json) {
    String fieldName = key.getName();
    switch (key.getType()) {
      case BOOL:
        return (Boolean) value == json.getBoolean(fieldName);
      case BYTES:
        return Arrays.equals((byte[]) value, json.getString(fieldName).getBytes());
      case INT64:
        return (long) value == json.getLong(fieldName);
      case STRING:
        return ((String) value).equals(json.getString(fieldName));
      case DOUBLE:
        return (double) value == json.getDouble(fieldName);
      case MESSAGE:
        return isProtoJsonEqual((DynamicMessage) value, json.getJSONObject(fieldName));
    }
    return false;
  }

  private boolean isProtoArrayJsonArrayEqual(
      FieldDescriptor key, java.lang.Object value, JSONObject json) {
    String fieldName = key.getName();
    JSONArray jsonArray = json.getJSONArray(fieldName);
    switch (key.getType()) {
      case BOOL:
        List<Boolean> boolArr = (List<Boolean>) value;
        for (int i = 0; i < jsonArray.length(); i++) {
          if (!(boolArr.get(i) == jsonArray.getBoolean(i))) {
            return false;
          }
        }
        return true;
      case BYTES:
        List<byte[]> byteArr = (List<byte[]>) value;
        for (int i = 0; i < jsonArray.length(); i++) {
          if (!Arrays.equals(byteArr.get(i), jsonArray.getString(i).getBytes())) {
            return false;
          }
        }
        return true;
      case INT64:
        List<Long> longArr = (List<Long>) value;
        for (int i = 0; i < jsonArray.length(); i++) {
          if (!(longArr.get(i) == jsonArray.getLong(i))) {
            return false;
          }
        }
        return true;
      case STRING:
        List<String> stringArr = (List<String>) value;
        for (int i = 0; i < jsonArray.length(); i++) {
          if (!stringArr.get(i).equals(jsonArray.getString(i))) {
            return false;
          }
        }
        return true;
      case DOUBLE:
        List<Double> doubleArr = (List<Double>) value;
        for (int i = 0; i < jsonArray.length(); i++) {
          if (!(doubleArr.get(i) == jsonArray.getDouble(i))) {
            return false;
          }
        }
        return true;
      case MESSAGE:
        List<DynamicMessage> messageArr = (List<DynamicMessage>) value;
        for (int i = 0; i < jsonArray.length(); i++) {
          if (!isProtoJsonEqual(messageArr.get(i), jsonArray.getJSONObject(i))) {
            return false;
          }
        }
        return true;
    }
    return false;
  }

  @Test
  public void testBQTableSchemaToProtoDescriptorSimpleTypes() throws Exception {
    for (Map.Entry<Table.TableFieldSchema.Type, Descriptor> entry :
        BQTableTypeToProtoDescriptor.entrySet()) {
      Table.TableFieldSchema tableFieldSchema =
          Table.TableFieldSchema.newBuilder()
              .setType(entry.getKey())
              .setMode(Table.TableFieldSchema.Mode.NULLABLE)
              .setName("test_field_type")
              .build();
      Table.TableSchema tableSchema =
          Table.TableSchema.newBuilder().addFields(0, tableFieldSchema).build();
      Descriptor descriptor = JsonToProtoConverter.BQTableSchemaToProtoSchema(tableSchema);
      assertTrue(isDescriptorEqual(descriptor, entry.getValue()));
    }
  }

  @Test
  public void testBQTableSchemaToProtoDescriptorStruct() throws Exception {
    Table.TableFieldSchema StringType =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.STRING)
            .setMode(Table.TableFieldSchema.Mode.NULLABLE)
            .setName("test_field_type")
            .build();
    Table.TableFieldSchema tableFieldSchema =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.STRUCT)
            .setMode(Table.TableFieldSchema.Mode.NULLABLE)
            .setName("test_field_type")
            .addFields(0, StringType)
            .build();
    Table.TableSchema tableSchema =
        Table.TableSchema.newBuilder().addFields(0, tableFieldSchema).build();
    Descriptor descriptor = JsonToProtoConverter.BQTableSchemaToProtoSchema(tableSchema);
    assertTrue(isDescriptorEqual(descriptor, MessageType.getDescriptor()));
  }

  @Test
  public void testProtoSchemaToProtobufferSimpleTypes() throws Exception {
    for (Map.Entry<Table.TableFieldSchema.Type, Integer> entry :
        TableFieldTypeToAccepted.entrySet()) {
      Table.TableFieldSchema tableFieldSchema =
          Table.TableFieldSchema.newBuilder()
              .setType(entry.getKey())
              .setMode(Table.TableFieldSchema.Mode.NULLABLE)
              .setName("test_field_type")
              .build();
      Table.TableSchema tableSchema =
          Table.TableSchema.newBuilder().addFields(0, tableFieldSchema).build();
      int success = 0;
      for (JSONObject json : simpleJSONObjects) {
        try {
          Descriptor descriptor = JsonToProtoConverter.BQTableSchemaToProtoSchema(tableSchema);
          DynamicMessage protoMsg =
              JsonToProtoConverter.protoSchemaToProtoMessage(descriptor, json);
          success += 1;
        } catch (IllegalArgumentException e) {
          assertEquals(
              e.getMessage(),
              "JSONObject does not have a "
                  + BQTableTypeToDebugMessage.get(entry.getKey())
                  + " field at .test_field_type.");
        }
      }
      assertEquals((int) entry.getValue(), success);
    }
  }

  @Test
  public void testBQSchemaToProtobufferStructSimple() throws Exception {
    Table.TableFieldSchema StringType =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.STRING)
            .setMode(Table.TableFieldSchema.Mode.NULLABLE)
            .setName("test_field_type")
            .build();
    Table.TableFieldSchema tableFieldSchema =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.STRUCT)
            .setMode(Table.TableFieldSchema.Mode.NULLABLE)
            .setName("test_field_type")
            .addFields(0, StringType)
            .build();
    Table.TableSchema tableSchema =
        Table.TableSchema.newBuilder().addFields(0, tableFieldSchema).build();

    JSONObject stringType = new JSONObject();
    stringType.put("test_field_type", "test");
    JSONObject json = new JSONObject();
    json.put("test_field_type", stringType);

    Descriptor descriptor = JsonToProtoConverter.BQTableSchemaToProtoSchema(tableSchema);
    DynamicMessage protoMsg = JsonToProtoConverter.protoSchemaToProtoMessage(descriptor, json);
    assertTrue(isProtoJsonEqual(protoMsg, json));
  }

  @Test
  public void testBQSchemaToProtobufferStructSimpleFail() throws Exception {
    Table.TableFieldSchema StringType =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.STRING)
            .setMode(Table.TableFieldSchema.Mode.NULLABLE)
            .setName("test_field_type")
            .build();
    Table.TableFieldSchema tableFieldSchema =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.STRUCT)
            .setMode(Table.TableFieldSchema.Mode.NULLABLE)
            .setName("test_field_type")
            .addFields(0, StringType)
            .build();
    Table.TableSchema tableSchema =
        Table.TableSchema.newBuilder().addFields(0, tableFieldSchema).build();

    JSONObject stringType = new JSONObject();
    stringType.put("test_field_type", 1);
    JSONObject json = new JSONObject();
    json.put("test_field_type", stringType);
    try {
      Descriptor descriptor = JsonToProtoConverter.BQTableSchemaToProtoSchema(tableSchema);
      DynamicMessage protoMsg = JsonToProtoConverter.protoSchemaToProtoMessage(descriptor, json);
    } catch (IllegalArgumentException e) {
      assertEquals(
          e.getMessage(),
          "JSONObject does not have a string field at .test_field_type.test_field_type.");
    }
  }

  @Test
  public void testBQSchemaToProtobufferStructComplex() throws Exception {
    Table.TableFieldSchema bqBytes =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.BYTES)
            .setMode(Table.TableFieldSchema.Mode.NULLABLE)
            .setName("bytes")
            .build();
    Table.TableFieldSchema bqInt =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.INT64)
            .setMode(Table.TableFieldSchema.Mode.NULLABLE)
            .setName("int")
            .build();
    Table.TableFieldSchema record1 =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.STRUCT)
            .setMode(Table.TableFieldSchema.Mode.NULLABLE)
            .setName("record1")
            .addFields(0, bqInt)
            .build();
    Table.TableFieldSchema record =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.STRUCT)
            .setMode(Table.TableFieldSchema.Mode.NULLABLE)
            .setName("record")
            .addFields(0, bqInt)
            .addFields(1, bqBytes)
            .addFields(2, record1)
            .build();
    Table.TableFieldSchema bqDouble =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.DOUBLE)
            .setMode(Table.TableFieldSchema.Mode.NULLABLE)
            .setName("float")
            .build();
    Table.TableSchema tableSchema =
        Table.TableSchema.newBuilder().addFields(0, record).addFields(1, bqDouble).build();

    JSONObject jsonRecord1 = new JSONObject();
    jsonRecord1.put("int", 2048);
    JSONObject jsonRecord = new JSONObject();
    jsonRecord.put("int", 1024);
    jsonRecord.put("bytes", "testing");
    jsonRecord.put("record1", jsonRecord1);
    JSONObject json = new JSONObject();
    json.put("record", jsonRecord);
    json.put("float", 1.23);

    Descriptor descriptor = JsonToProtoConverter.BQTableSchemaToProtoSchema(tableSchema);
    DynamicMessage protoMsg = JsonToProtoConverter.protoSchemaToProtoMessage(descriptor, json);
    assertTrue(isProtoJsonEqual(protoMsg, json));
  }

  @Test
  public void testBQSchemaToProtobufferStructComplexFail() throws Exception {
    Table.TableFieldSchema bqInt =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.INT64)
            .setMode(Table.TableFieldSchema.Mode.NULLABLE)
            .setName("int")
            .build();
    Table.TableFieldSchema record =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.STRUCT)
            .setMode(Table.TableFieldSchema.Mode.NULLABLE)
            .setName("record")
            .addFields(0, bqInt)
            .build();
    Table.TableFieldSchema bqDouble =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.DOUBLE)
            .setMode(Table.TableFieldSchema.Mode.NULLABLE)
            .setName("float")
            .build();
    Table.TableSchema tableSchema =
        Table.TableSchema.newBuilder().addFields(0, record).addFields(1, bqDouble).build();

    JSONObject json = new JSONObject();
    json.put("record", 1.23);
    json.put("float", 1.23);
    try {
      Descriptor descriptor = JsonToProtoConverter.BQTableSchemaToProtoSchema(tableSchema);
      DynamicMessage protoMsg = JsonToProtoConverter.protoSchemaToProtoMessage(descriptor, json);
    } catch (IllegalArgumentException e) {
      assertEquals(e.getMessage(), "JSONObject does not have an object field at .record.");
    }
  }

  @Test
  public void testBQSchemaToProtobufferRepeatedSimple() throws Exception {
    for (Map.Entry<Table.TableFieldSchema.Type, Integer> entry :
        TableFieldTypeToAccepted.entrySet()) {
      Table.TableFieldSchema tableFieldSchema =
          Table.TableFieldSchema.newBuilder()
              .setType(entry.getKey())
              .setMode(Table.TableFieldSchema.Mode.REPEATED)
              .setName("test_field_type")
              .build();
      Table.TableSchema tableSchema =
          Table.TableSchema.newBuilder().addFields(0, tableFieldSchema).build();
      int success = 0;
      for (JSONObject json : simpleJSONArrays) {
        try {
          Descriptor descriptor = JsonToProtoConverter.BQTableSchemaToProtoSchema(tableSchema);
          DynamicMessage protoMsg =
              JsonToProtoConverter.protoSchemaToProtoMessage(descriptor, json);
          success += 1;
        } catch (IllegalArgumentException e) {
          assertEquals(
              e.getMessage(),
              "JSONObject does not have a "
                  + BQTableTypeToDebugMessage.get(entry.getKey())
                  + " field at .test_field_type[0].");
        }
      }
      assertEquals((int) entry.getValue(), success);
    }
  }

  @Test
  public void testBQSchemaToProtobufferRepeatedSimpleInt64() throws Exception {
    Table.TableFieldSchema bqInt =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.INT64)
            .setMode(Table.TableFieldSchema.Mode.REPEATED)
            .setName("int")
            .build();
    Table.TableFieldSchema bqLong =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.INT64)
            .setMode(Table.TableFieldSchema.Mode.REPEATED)
            .setName("long")
            .build();
    Table.TableSchema tableSchema =
        Table.TableSchema.newBuilder().addFields(0, bqInt).addFields(1, bqLong).build();
    JSONObject json = new JSONObject();
    int[] intArr = {1, 2, 3, 4, 5};
    long[] longArr = {1L, 2L, 3L, 4L, 5L};
    json.put("int", new JSONArray(intArr));
    json.put("long", new JSONArray(longArr));
    Descriptor descriptor = JsonToProtoConverter.BQTableSchemaToProtoSchema(tableSchema);
    DynamicMessage protoMsg = JsonToProtoConverter.protoSchemaToProtoMessage(descriptor, json);
    assertTrue(isProtoJsonEqual(protoMsg, json));
  }

  @Test
  public void testBQSchemaToProtobufferRepeatedSimpleDouble() throws Exception {
    Table.TableFieldSchema bqDouble =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.DOUBLE)
            .setMode(Table.TableFieldSchema.Mode.REPEATED)
            .setName("double")
            .build();
    Table.TableFieldSchema bqFloat =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.DOUBLE)
            .setMode(Table.TableFieldSchema.Mode.REPEATED)
            .setName("float")
            .build();
    Table.TableSchema tableSchema =
        Table.TableSchema.newBuilder().addFields(0, bqDouble).addFields(1, bqFloat).build();
    JSONObject json = new JSONObject();
    double[] doubleArr = {1.1, 2.2, 3.3, 4.4, 5.5};
    float[] floatArr = {1.1f, 2.2f, 3.3f, 4.4f, 5.5f};
    json.put("double", new JSONArray(doubleArr));
    json.put("float", new JSONArray(floatArr));
    Descriptor descriptor = JsonToProtoConverter.BQTableSchemaToProtoSchema(tableSchema);
    DynamicMessage protoMsg = JsonToProtoConverter.protoSchemaToProtoMessage(descriptor, json);
    assertTrue(isProtoJsonEqual(protoMsg, json));
  }

  @Test
  public void testBQSchemaToProtobufferRepeatedSimpleFail() throws Exception {
    Table.TableFieldSchema bqDouble =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.DOUBLE)
            .setMode(Table.TableFieldSchema.Mode.REPEATED)
            .setName("float")
            .build();
    Table.TableSchema tableSchema = Table.TableSchema.newBuilder().addFields(0, bqDouble).build();
    JSONObject json = new JSONObject();
    json.put("float", new JSONArray("[1.1, 2.2, 3.3, hello, 4.4]"));
    try {
      Descriptor descriptor = JsonToProtoConverter.BQTableSchemaToProtoSchema(tableSchema);
      DynamicMessage protoMsg = JsonToProtoConverter.protoSchemaToProtoMessage(descriptor, json);
    } catch (IllegalArgumentException e) {
      assertEquals(e.getMessage(), "JSONObject does not have a double field at .float[3].");
    }
  }

  @Test
  public void testBQSchemaToProtobufferRepeatedComplex() throws Exception {
    Table.TableFieldSchema bqString =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.STRING)
            .setMode(Table.TableFieldSchema.Mode.REPEATED)
            .setName("string")
            .build();
    Table.TableFieldSchema record =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.STRUCT)
            .setMode(Table.TableFieldSchema.Mode.NULLABLE)
            .setName("test_field_type")
            .addFields(0, bqString)
            .build();
    Table.TableFieldSchema bqInt =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.INT64)
            .setMode(Table.TableFieldSchema.Mode.REPEATED)
            .setName("int")
            .build();
    Table.TableFieldSchema bqDouble =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.DOUBLE)
            .setMode(Table.TableFieldSchema.Mode.REPEATED)
            .setName("float")
            .build();
    Table.TableSchema tableSchema =
        Table.TableSchema.newBuilder()
            .addFields(0, bqDouble)
            .addFields(1, bqInt)
            .addFields(2, record)
            .build();
    JSONObject json = new JSONObject();
    double[] doubleArr = {1.1, 2.2, 3.3, 4.4, 5.5};
    String[] stringArr = {"hello", "this", "is", "a", "test"};
    int[] intArr = {1, 2, 3, 4, 5};
    json.put("float", new JSONArray(doubleArr));
    json.put("int", new JSONArray(intArr));
    JSONObject jsonRecord = new JSONObject();
    jsonRecord.put("string", new JSONArray(stringArr));
    json.put("test_field_type", jsonRecord);
    Descriptor descriptor = JsonToProtoConverter.BQTableSchemaToProtoSchema(tableSchema);
    DynamicMessage protoMsg = JsonToProtoConverter.protoSchemaToProtoMessage(descriptor, json);
    assertTrue(isProtoJsonEqual(protoMsg, json));
  }

  @Test
  public void testBQSchemaToProtobufferRepeatedComplexFail() throws Exception {
    Table.TableFieldSchema bqInt =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.INT64)
            .setMode(Table.TableFieldSchema.Mode.REPEATED)
            .setName("int")
            .build();
    Table.TableFieldSchema bqDouble =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.DOUBLE)
            .setMode(Table.TableFieldSchema.Mode.REPEATED)
            .setName("float")
            .build();
    Table.TableSchema tableSchema =
        Table.TableSchema.newBuilder().addFields(0, bqDouble).addFields(1, bqInt).build();
    JSONObject json = new JSONObject();
    int[] intArr = {1, 2, 3, 4, 5};
    json.put("float", "1");
    json.put("int", new JSONArray(intArr));
    try {
      Descriptor descriptor = JsonToProtoConverter.BQTableSchemaToProtoSchema(tableSchema);
      DynamicMessage protoMsg = JsonToProtoConverter.protoSchemaToProtoMessage(descriptor, json);
    } catch (IllegalArgumentException e) {
      assertEquals(e.getMessage(), "JSONObject does not have an array field at .float.");
    }
  }

  @Test
  public void testBQSchemaToProtobufferAllowUnknownFields() throws Exception {
    Table.TableFieldSchema bqDouble =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.DOUBLE)
            .setMode(Table.TableFieldSchema.Mode.NULLABLE)
            .setName("float")
            .build();
    Table.TableSchema tableSchema = Table.TableSchema.newBuilder().addFields(0, bqDouble).build();
    JSONObject json = new JSONObject();
    json.put("float", 1.1);
    json.put("string", "hello");

    Descriptor descriptor = JsonToProtoConverter.BQTableSchemaToProtoSchema(tableSchema);
    DynamicMessage protoMsg =
        JsonToProtoConverter.protoSchemaToProtoMessage(descriptor, json, true);
    assertTrue(isProtoJsonEqual(protoMsg, json));
  }

  @Test
  public void testBQSchemaToProtobufferAllowUnknownFieldsFail() throws Exception {
    Table.TableFieldSchema bqDouble =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.DOUBLE)
            .setMode(Table.TableFieldSchema.Mode.NULLABLE)
            .setName("float")
            .build();
    Table.TableSchema tableSchema = Table.TableSchema.newBuilder().addFields(0, bqDouble).build();
    JSONObject json = new JSONObject();
    json.put("float", 1.1);
    json.put("string", "hello");
    try {
      Descriptor descriptor = JsonToProtoConverter.BQTableSchemaToProtoSchema(tableSchema);
      DynamicMessage protoMsg = JsonToProtoConverter.protoSchemaToProtoMessage(descriptor, json);
    } catch (IllegalArgumentException e) {
      assertEquals(
          e.getMessage(),
          "JSONObject has unknown fields. Set allowUnknownFields to True to ignore unknown fields.");
    }
  }

  @Test
  public void testBQSchemaToProtobufferRequiredFail() throws Exception {
    Table.TableFieldSchema bqDouble =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.DOUBLE)
            .setMode(Table.TableFieldSchema.Mode.REQUIRED)
            .setName("float")
            .build();
    Table.TableSchema tableSchema = Table.TableSchema.newBuilder().addFields(0, bqDouble).build();
    JSONObject json = new JSONObject();
    json.put("float1", 1.1);
    try {
      Descriptor descriptor = JsonToProtoConverter.BQTableSchemaToProtoSchema(tableSchema);
      DynamicMessage protoMsg = JsonToProtoConverter.protoSchemaToProtoMessage(descriptor, json);
    } catch (IllegalArgumentException e) {
      assertEquals(e.getMessage(), "JSONObject does not have the required field .float.");
    }
  }

  @Test
  public void testBQSchemaToProtobufferTopLevelMatchFail() throws Exception {
    Table.TableFieldSchema bqDouble =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.DOUBLE)
            .setMode(Table.TableFieldSchema.Mode.NULLABLE)
            .setName("float")
            .build();
    Table.TableSchema tableSchema = Table.TableSchema.newBuilder().addFields(0, bqDouble).build();
    JSONObject json = new JSONObject();
    json.put("float1", 1.1);
    try {
      Descriptor descriptor = JsonToProtoConverter.BQTableSchemaToProtoSchema(tableSchema);
      DynamicMessage protoMsg = JsonToProtoConverter.protoSchemaToProtoMessage(descriptor, json);
    } catch (IllegalArgumentException e) {
      assertEquals(
          e.getMessage(),
          "There are no matching fields found for the JSONObject and the BigQuery table.");
    }
  }

  @Test
  public void testBQSchemaToProtobufferOptional() throws Exception {
    Table.TableFieldSchema StringType =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.STRING)
            .setMode(Table.TableFieldSchema.Mode.NULLABLE)
            .setName("test_field_type")
            .build();
    Table.TableFieldSchema tableFieldSchema =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.STRUCT)
            .setMode(Table.TableFieldSchema.Mode.NULLABLE)
            .setName("test_field_type")
            .addFields(0, StringType)
            .build();
    Table.TableFieldSchema actualStringType =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.STRING)
            .setMode(Table.TableFieldSchema.Mode.NULLABLE)
            .setName("test_field_type1")
            .build();
    Table.TableSchema tableSchema =
        Table.TableSchema.newBuilder()
            .addFields(0, tableFieldSchema)
            .addFields(1, actualStringType)
            .build();

    JSONObject stringType = new JSONObject();
    stringType.put("test_field_type1", 1);
    JSONObject json = new JSONObject();
    json.put("test_field_type", stringType);
    json.put("test_field_type2", "hello");

    Descriptor descriptor = JsonToProtoConverter.BQTableSchemaToProtoSchema(tableSchema);
    DynamicMessage protoMsg = JsonToProtoConverter.protoSchemaToProtoMessage(descriptor, json);
    assertTrue(isProtoJsonEqual(protoMsg, json));
  }

  @Test
  public void testBQSchemaToProtobufferNullJson() throws Exception {
    Table.TableFieldSchema bqDouble =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.DOUBLE)
            .setMode(Table.TableFieldSchema.Mode.NULLABLE)
            .setName("float")
            .build();
    Table.TableSchema tableSchema = Table.TableSchema.newBuilder().addFields(0, bqDouble).build();
    JSONObject json = new JSONObject();
    json.put("float", JSONObject.NULL);
    try {
      Descriptor descriptor = JsonToProtoConverter.BQTableSchemaToProtoSchema(tableSchema);
      DynamicMessage protoMsg = JsonToProtoConverter.protoSchemaToProtoMessage(descriptor, json);
    } catch (IllegalArgumentException e) {
      assertEquals(e.getMessage(), "JSONObject does not have a double field at .float.");
    }
  }
}
