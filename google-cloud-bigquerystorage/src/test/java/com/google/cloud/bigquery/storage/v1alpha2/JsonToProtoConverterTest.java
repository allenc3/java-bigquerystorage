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
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.util.Arrays;
import java.util.List;

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

  private static JSONObject[] simpleJSONObjects = {
    new JSONObject().put("test_field_type", 21474836470L),
    new JSONObject().put("test_field_type", 1.23),
    new JSONObject().put("test_field_type", true),
    new JSONObject().put("test_field_type", "test")
  };

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
  public void testBQTableSchemaToProtoDescriptorComplex() throws Exception {
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
  public void testBQSchemaToProtobufferBoolean() throws Exception {
    Table.TableFieldSchema tableFieldSchema =
        Table.TableFieldSchema.newBuilder()
            .setType(Table.TableFieldSchema.Type.BOOL)
            .setMode(Table.TableFieldSchema.Mode.NULLABLE)
            .setName("test_field_type")
            .build();
    Table.TableSchema tableSchema =
        Table.TableSchema.newBuilder().addFields(0, tableFieldSchema).build();
    int success = 0;
    for (JSONObject json : simpleJSONObjects) {
      try {
        Descriptor descriptor = JsonToProtoConverter.BQTableSchemaToProtoSchema(tableSchema);
        DynamicMessage protoMsg = JsonToProtoConverter.protoSchemaToProtoMessage(descriptor, json);
        success += 1;
      } catch (IllegalArgumentException e) {
        assertEquals(
            e.getMessage(), "JSONObject does not have the boolean field .test_field_type.");
      }
    }
  }
}
