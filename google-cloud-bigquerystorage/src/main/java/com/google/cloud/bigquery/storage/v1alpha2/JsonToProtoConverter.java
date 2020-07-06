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

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * A class that checks the schema compatibility between user schema in proto descriptor and Bigquery
 * table schema. If this check is passed, then user can write to BigQuery table using the user
 * schema, otherwise the write will fail.
 *
 * <p>The implementation as of now is not complete, which measn, if this check passed, there is
 * still a possbility of writing will fail.
 */
public class JsonToProtoConverter {
  private static ImmutableMap<Table.TableFieldSchema.Mode, FieldDescriptorProto.Label>
      BQTableSchemaModeMap =
          ImmutableMap.of(
              Table.TableFieldSchema.Mode.NULLABLE, FieldDescriptorProto.Label.LABEL_OPTIONAL,
              Table.TableFieldSchema.Mode.REPEATED, FieldDescriptorProto.Label.LABEL_REPEATED,
              Table.TableFieldSchema.Mode.REQUIRED, FieldDescriptorProto.Label.LABEL_REQUIRED);

  private static ImmutableMap<Table.TableFieldSchema.Type, FieldDescriptorProto.Type>
      BQTableSchemaTypeMap =
          new ImmutableMap.Builder<Table.TableFieldSchema.Type, FieldDescriptorProto.Type>()
              .put(Table.TableFieldSchema.Type.BOOL, FieldDescriptorProto.Type.TYPE_BOOL)
              .put(Table.TableFieldSchema.Type.BYTES, FieldDescriptorProto.Type.TYPE_BYTES)
              .put(Table.TableFieldSchema.Type.DATE, FieldDescriptorProto.Type.TYPE_INT64)
              .put(Table.TableFieldSchema.Type.DATETIME, FieldDescriptorProto.Type.TYPE_INT64)
              .put(Table.TableFieldSchema.Type.DOUBLE, FieldDescriptorProto.Type.TYPE_DOUBLE)
              .put(Table.TableFieldSchema.Type.GEOGRAPHY, FieldDescriptorProto.Type.TYPE_BYTES)
              .put(Table.TableFieldSchema.Type.INT64, FieldDescriptorProto.Type.TYPE_INT64)
              .put(Table.TableFieldSchema.Type.NUMERIC, FieldDescriptorProto.Type.TYPE_BYTES)
              .put(Table.TableFieldSchema.Type.STRING, FieldDescriptorProto.Type.TYPE_STRING)
              .put(Table.TableFieldSchema.Type.STRUCT, FieldDescriptorProto.Type.TYPE_MESSAGE)
              .put(Table.TableFieldSchema.Type.TIME, FieldDescriptorProto.Type.TYPE_INT64)
              .put(Table.TableFieldSchema.Type.TIMESTAMP, FieldDescriptorProto.Type.TYPE_INT64)
              .build();

  /**
   * Converts Table.TableSchema to a Descriptors.Descriptor object.
   *
   * @param BQTableSchema
   * @throws Descriptors.DescriptorValidationException
   */
  public static Descriptor BQTableSchemaToProtoSchema(Table.TableSchema BQTableSchema)
      throws Descriptors.DescriptorValidationException {
    Descriptor descriptor = BQTableSchemaToProtoSchemaImpl(BQTableSchema, "root");
    return descriptor;
  }

  /**
   * Implementation that converts a Table.TableSchema to a Descriptors.Descriptor object.
   *
   * @param BQTableSchema
   * @param scope Keeps track of current scope to prevent repeated naming while constructing
   *     descriptor.
   * @throws Descriptors.DescriptorValidationException
   */
  private static Descriptor BQTableSchemaToProtoSchemaImpl(
      Table.TableSchema BQTableSchema, String scope)
      throws Descriptors.DescriptorValidationException {
    List<FileDescriptor> dependenciesList = new ArrayList<FileDescriptor>();
    List<FieldDescriptorProto> fields = new ArrayList<FieldDescriptorProto>();
    int index = 1;
    for (Table.TableFieldSchema BQTableField : BQTableSchema.getFieldsList()) {
      if (BQTableField.getType() == Table.TableFieldSchema.Type.STRUCT) {
        String currentScope = scope + BQTableField.getName();
        dependenciesList.add(
            BQTableSchemaToProtoSchemaImpl(
                    Table.TableSchema.newBuilder()
                        .addAllFields(BQTableField.getFieldsList())
                        .build(),
                    currentScope)
                .getFile());
        fields.add(BQStructToProtoMessage(BQTableField, index++, currentScope));
      } else {
        fields.add(BQTableFieldToProtoField(BQTableField, index++));
      }
    }
    FileDescriptor[] dependenciesArray = new FileDescriptor[dependenciesList.size()];
    dependenciesArray = dependenciesList.toArray(dependenciesArray);
    DescriptorProto descriptorProto =
        DescriptorProto.newBuilder().setName(scope).addAllField(fields).build();
    FileDescriptorProto fileDescriptorProto =
        FileDescriptorProto.newBuilder().addMessageType(descriptorProto).build();
    FileDescriptor fileDescriptor =
        FileDescriptor.buildFrom(fileDescriptorProto, dependenciesArray);
    Descriptor descriptor = fileDescriptor.findMessageTypeByName(scope);
    return descriptor;
  }

  /**
   * Constructs a FieldDescriptorProto for non-struct BQ fields.
   *
   * @param BQTableField BQ Field used to construct a FieldDescriptorProto
   * @param index Index for protobuf fields.
   */
  private static FieldDescriptorProto BQTableFieldToProtoField(
      Table.TableFieldSchema BQTableField, int index) {
    String fieldName = BQTableField.getName();
    Table.TableFieldSchema.Mode mode = BQTableField.getMode();
    return FieldDescriptorProto.newBuilder()
        .setName(fieldName)
        .setType((FieldDescriptorProto.Type) BQTableSchemaTypeMap.get(BQTableField.getType()))
        .setLabel((FieldDescriptorProto.Label) BQTableSchemaModeMap.get(mode))
        .setNumber(index)
        .build();
  }

  /**
   * Constructs a FieldDescriptorProto for a Struct type BQ field.
   *
   * @param BQTableField BQ Field used to construct a FieldDescriptorProto
   * @param index Index for protobuf fields.
   * @param scope Need scope to prevent naming issues
   */
  private static FieldDescriptorProto BQStructToProtoMessage(
      Table.TableFieldSchema BQTableField, int index, String scope) {
    String fieldName = BQTableField.getName();
    Table.TableFieldSchema.Mode mode = BQTableField.getMode();
    return FieldDescriptorProto.newBuilder()
        .setName(fieldName)
        .setTypeName(scope)
        .setLabel((FieldDescriptorProto.Label) BQTableSchemaModeMap.get(mode))
        .setNumber(index)
        .build();
  }

  /**
   * Creates a protobuf message using the protobuf descriptor and json data and follows
   * SchemaCompatibility rules.
   *
   * @param protoSchema
   * @param json
   * @throws IllegalArgumentException when JSON data is not compatible with proto descriptor.
   */
  public static DynamicMessage protoSchemaToProtoMessage(
      Descriptors.Descriptor protoSchema, JSONObject json) throws IllegalArgumentException {
    return protoSchemaToProtoMessage(protoSchema, json, false);
  }

  /**
   * Creates a protobuf message using the protobuf descriptor and json data and follows
   * SchemaCompatibility rules.
   *
   * @param protoSchema
   * @param json
   * @param allowUnknownFields Ignores unknown JSON fields.
   * @throws IllegalArgumentException when JSON data is not compatible with proto descriptor.
   */
  public static DynamicMessage protoSchemaToProtoMessage(
      Descriptors.Descriptor protoSchema, JSONObject json, boolean allowUnknownFields)
      throws IllegalArgumentException {
    return protoSchemaToProtoMessageImpl(protoSchema, json, "", true, allowUnknownFields);
  }

  /**
   * Implementation to create protobuf message using the descriptor and json data.
   *
   * @param protoSchema
   * @param json
   * @param topLevel If root level has no matching fields, throws exception.
   * @param allowUnknownFields Ignores unknown JSON fields.
   * @throws IllegalArgumentException when JSON data is not compatible with proto descriptor.
   */
  private static DynamicMessage protoSchemaToProtoMessageImpl(
      Descriptors.Descriptor protoSchema,
      JSONObject json,
      String jsonScope,
      boolean topLevel,
      boolean allowUnknownFields)
      throws IllegalArgumentException {
    DynamicMessage.Builder protoMsg = DynamicMessage.newBuilder(protoSchema);
    int matchedFields = 0;
    List<Descriptors.FieldDescriptor> protoFields = protoSchema.getFields();

    if (JSONObject.getNames(json).length > protoFields.size()) {
      if (!allowUnknownFields) {
        throw new IllegalArgumentException(
            "JSONObject has unknown fields. Set allowUnknownFields to True to ignore unknown fields.");
      }
    }

    for (Descriptors.FieldDescriptor field : protoFields) {
      String fieldName = field.getName();
      String currentScope = jsonScope + "." + fieldName;

      if (!json.has(fieldName)) {
        if (field.isRequired()) {
          throw new IllegalArgumentException(
              "JSONObject does not have the required field " + currentScope + ".");
        } else {
          continue;
        }
      }
      matchedFields++;
      if (!field.isRepeated()) {
        fillField(protoMsg, field, json, currentScope, allowUnknownFields);
      } else {
        fillRepeatedField(protoMsg, field, json, currentScope, allowUnknownFields);
      }
    }
    if (matchedFields == 0 && topLevel) {
      throw new IllegalArgumentException(
          "There are no matching fields found for the JSONObject and the BigQuery table.");
    }
    return protoMsg.build();
  }

  /**
   * Fills a non-repetaed protoField with the json data.
   *
   * @param protoMsg
   * @param field
   * @param json If root level has no matching fields, throws exception.
   * @param currentScope Debugging purposes
   * @param allowUnknownFields Ignores unknown JSON fields.
   * @throws IllegalArgumentException when JSON data is not compatible with proto descriptor.
   */
  private static void fillField(
      DynamicMessage.Builder protoMsg,
      Descriptors.FieldDescriptor field,
      JSONObject json,
      String currentScope,
      boolean allowUnknownFields)
      throws IllegalArgumentException {

    String fieldName = field.getName();
    switch (field.getType()) {
      case BOOL:
        try {
          protoMsg.setField(field, new Boolean(json.getBoolean(fieldName)));
        } catch (JSONException e) {
          throw new IllegalArgumentException(
              "JSONObject does not have a boolean field at " + currentScope + ".");
        }
        break;
      case BYTES:
        try {
          protoMsg.setField(field, json.getString(fieldName).getBytes());
        } catch (JSONException e) {
          throw new IllegalArgumentException(
              "JSONObject does not have a string field at " + currentScope + ".");
        }
        break;
      case INT64:
        try {
          java.lang.Object val = json.get(fieldName);
          if (val instanceof Integer) {
            protoMsg.setField(field, new Long((Integer) val));
          } else if (val instanceof Long) {
            protoMsg.setField(field, new Long((Long) val));
          } else {
            throw new IllegalArgumentException(
                "JSONObject does not have a int64 field at " + currentScope + ".");
          }
        } catch (JSONException e) {
          throw new IllegalArgumentException(
              "JSONObject does not have a int64 field at " + currentScope + ".");
        }
        break;
      case STRING:
        try {
          protoMsg.setField(field, json.getString(fieldName));
        } catch (JSONException e) {
          throw new IllegalArgumentException(
              "JSONObject does not have a string field at " + currentScope + ".");
        }
        break;
      case DOUBLE:
        try {
          java.lang.Object val = json.get(fieldName);
          if (val instanceof Double) {
            protoMsg.setField(field, new Double((double) val));
          } else if (val instanceof Float) {
            protoMsg.setField(field, new Double((float) val));
          } else {
            throw new IllegalArgumentException(
                "JSONObject does not have a double field at " + currentScope + ".");
          }
        } catch (JSONException e) {
          throw new IllegalArgumentException(
              "JSONObject does not have a double field at " + currentScope + ".");
        }
        break;
      case MESSAGE:
        Message.Builder message = protoMsg.newBuilderForField(field);
        try {
          protoMsg.setField(
              field,
              protoSchemaToProtoMessageImpl(
                  field.getMessageType(),
                  json.getJSONObject(fieldName),
                  currentScope,
                  false,
                  allowUnknownFields));
        } catch (JSONException e) {
          throw new IllegalArgumentException(
              "JSONObject does not have an object field at " + currentScope + ".");
        }
        break;
    }
  }

  /**
   * Fills a repeated protoField with the json data.
   *
   * @param protoMsg
   * @param field
   * @param json If root level has no matching fields, throws exception.
   * @param currentScope Debugging purposes
   * @param allowUnknownFields Ignores unknown JSON fields.
   * @throws IllegalArgumentException when JSON data is not compatible with proto descriptor.
   */
  private static void fillRepeatedField(
      DynamicMessage.Builder protoMsg,
      Descriptors.FieldDescriptor field,
      JSONObject json,
      String currentScope,
      boolean allowUnknownFields)
      throws IllegalArgumentException {
    String fieldName = field.getName();
    JSONArray jsonArray;
    try {
      jsonArray = json.getJSONArray(fieldName);
    } catch (JSONException e) {
      throw new IllegalArgumentException(
          "JSONObject does not have an array field at " + currentScope + ".");
    }

    switch (field.getType()) {
      case BOOL:
        for (int i = 0; i < jsonArray.length(); i++) {
          try {
            protoMsg.addRepeatedField(field, new Boolean(jsonArray.getBoolean(i)));
          } catch (JSONException e) {
            throw new IllegalArgumentException(
                "JSONObject does not have a boolean field at "
                    + currentScope
                    + "["
                    + i
                    + "]"
                    + ".");
          }
        }
        break;
      case BYTES:
        for (int i = 0; i < jsonArray.length(); i++) {
          try {
            protoMsg.addRepeatedField(field, jsonArray.getString(i).getBytes());
          } catch (JSONException e) {
            throw new IllegalArgumentException(
                "JSONObject does not have a string field at " + currentScope + "[" + i + "]" + ".");
          }
        }
        break;
      case INT64:
        for (int i = 0; i < jsonArray.length(); i++) {
          try {
            java.lang.Object val = jsonArray.get(i);
            if (val instanceof Integer) {
              protoMsg.addRepeatedField(field, new Long((Integer) val));
            } else if (val instanceof Long) {
              protoMsg.addRepeatedField(field, new Long((Long) val));
            } else {
              throw new IllegalArgumentException(
                  "JSONObject does not have a int64 field at "
                      + currentScope
                      + "["
                      + i
                      + "]"
                      + ".");
            }
          } catch (JSONException e) {
            throw new IllegalArgumentException(
                "JSONObject does not have a int64 field at " + currentScope + "[" + i + "]" + ".");
          }
        }
        break;
      case STRING:
        for (int i = 0; i < jsonArray.length(); i++) {
          try {
            protoMsg.addRepeatedField(field, jsonArray.getString(i));
          } catch (JSONException e) {
            throw new IllegalArgumentException(
                "JSONObject does not have a string field at " + currentScope + "[" + i + "]" + ".");
          }
        }
        break;
      case DOUBLE:
        for (int i = 0; i < jsonArray.length(); i++) {
          try {
            java.lang.Object val = jsonArray.get(i);
            if (val instanceof Double) {
              protoMsg.addRepeatedField(field, new Double((double) val));
            } else if (val instanceof Float) {
              protoMsg.addRepeatedField(field, new Double((float) val));
            } else {
              throw new IllegalArgumentException(
                  "JSONObject does not have a double field at "
                      + currentScope
                      + "["
                      + i
                      + "]"
                      + ".");
            }
          } catch (JSONException e) {
            throw new IllegalArgumentException(
                "JSONObject does not have a double field at " + currentScope + "[" + i + "]" + ".");
          }
        }
        break;
      case MESSAGE:
        for (int i = 0; i < jsonArray.length(); i++) {
          try {
            Message.Builder message = protoMsg.newBuilderForField(field);
            protoMsg.addRepeatedField(
                field,
                protoSchemaToProtoMessageImpl(
                    field.getMessageType(),
                    jsonArray.getJSONObject(i),
                    currentScope,
                    false,
                    allowUnknownFields));
          } catch (JSONException e) {
            throw new IllegalArgumentException(
                "JSONObject does not have an object field at "
                    + currentScope
                    + "["
                    + i
                    + "]"
                    + ".");
          }
        }
        break;
    }
  }
}
