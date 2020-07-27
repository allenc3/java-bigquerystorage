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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.api.core.*;
import com.google.api.core.ApiFuture;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.testing.LocalChannelProvider;
import com.google.api.gax.grpc.testing.MockGrpcService;
import com.google.api.gax.grpc.testing.MockServiceHelper;
import com.google.cloud.bigquery.storage.test.JsonTest.ComplexRoot;
import com.google.cloud.bigquery.storage.test.Test.FooType;
import com.google.cloud.bigquery.storage.test.Test.UpdatedFooType;
import com.google.cloud.bigquery.storage.v1alpha2.Storage.AppendRowsResponse;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Timestamp;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import java.util.logging.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Instant;

@RunWith(JUnit4.class)
public class JsonStreamWriterTest {
  private static final Logger LOG = Logger.getLogger(JsonStreamWriterTest.class.getName());
  private static final String TEST_STREAM = "projects/p/datasets/d/tables/t/streams/s";
  private static final ExecutorProvider SINGLE_THREAD_EXECUTOR =
      InstantiatingExecutorProvider.newBuilder().setExecutorThreadCount(1).build();
  private static LocalChannelProvider channelProvider;
  private FakeScheduledExecutorService fakeExecutor;
  private FakeBigQueryWrite testBigQueryWrite;
  private static MockServiceHelper serviceHelper;

  private final Table.TableFieldSchema FOO =
      Table.TableFieldSchema.newBuilder()
          .setType(Table.TableFieldSchema.Type.STRING)
          .setMode(Table.TableFieldSchema.Mode.NULLABLE)
          .setName("foo")
          .build();
  private final Table.TableSchema TABLE_SCHEMA =
      Table.TableSchema.newBuilder().addFields(0, FOO).build();

  private final Table.TableFieldSchema BAR =
      Table.TableFieldSchema.newBuilder()
          .setType(Table.TableFieldSchema.Type.STRING)
          .setMode(Table.TableFieldSchema.Mode.NULLABLE)
          .setName("bar")
          .build();
  private final Table.TableSchema UPDATED_TABLE_SCHEMA =
      Table.TableSchema.newBuilder().addFields(0, FOO).addFields(1, BAR).build();

  private final Table.TableFieldSchema TEST_INT =
      Table.TableFieldSchema.newBuilder()
          .setType(Table.TableFieldSchema.Type.INT64)
          .setMode(Table.TableFieldSchema.Mode.NULLABLE)
          .setName("test_int")
          .build();
  private final Table.TableFieldSchema TEST_STRING =
      Table.TableFieldSchema.newBuilder()
          .setType(Table.TableFieldSchema.Type.STRING)
          .setMode(Table.TableFieldSchema.Mode.REPEATED)
          .setName("test_string")
          .build();
  private final Table.TableFieldSchema TEST_BYTES =
      Table.TableFieldSchema.newBuilder()
          .setType(Table.TableFieldSchema.Type.BYTES)
          .setMode(Table.TableFieldSchema.Mode.REQUIRED)
          .setName("test_bytes")
          .build();
  private final Table.TableFieldSchema TEST_BOOL =
      Table.TableFieldSchema.newBuilder()
          .setType(Table.TableFieldSchema.Type.BOOL)
          .setMode(Table.TableFieldSchema.Mode.NULLABLE)
          .setName("test_bool")
          .build();
  private final Table.TableFieldSchema TEST_DOUBLE =
      Table.TableFieldSchema.newBuilder()
          .setType(Table.TableFieldSchema.Type.DOUBLE)
          .setMode(Table.TableFieldSchema.Mode.REPEATED)
          .setName("test_double")
          .build();
  private final Table.TableFieldSchema TEST_DATE =
      Table.TableFieldSchema.newBuilder()
          .setType(Table.TableFieldSchema.Type.DATE)
          .setMode(Table.TableFieldSchema.Mode.REQUIRED)
          .setName("test_date")
          .build();
  private final Table.TableFieldSchema COMPLEXLVL2 =
      Table.TableFieldSchema.newBuilder()
          .setType(Table.TableFieldSchema.Type.STRUCT)
          .setMode(Table.TableFieldSchema.Mode.REQUIRED)
          .addFields(0, TEST_INT)
          .setName("complex_lvl2")
          .build();
  private final Table.TableFieldSchema COMPLEXLVL1 =
      Table.TableFieldSchema.newBuilder()
          .setType(Table.TableFieldSchema.Type.STRUCT)
          .setMode(Table.TableFieldSchema.Mode.REQUIRED)
          .addFields(0, TEST_INT)
          .addFields(1, COMPLEXLVL2)
          .setName("complex_lvl1")
          .build();
  private final Table.TableSchema COMPLEX_TABLE_SCHEMA =
      Table.TableSchema.newBuilder()
          .addFields(0, TEST_INT)
          .addFields(1, TEST_STRING)
          .addFields(2, TEST_BYTES)
          .addFields(3, TEST_BOOL)
          .addFields(4, TEST_DOUBLE)
          .addFields(5, TEST_DATE)
          .addFields(6, COMPLEXLVL1)
          .addFields(7, COMPLEXLVL2)
          .build();

  @Before
  public void setUp() throws Exception {
    testBigQueryWrite = new FakeBigQueryWrite();
    serviceHelper =
        new MockServiceHelper(
            UUID.randomUUID().toString(), Arrays.<MockGrpcService>asList(testBigQueryWrite));
    serviceHelper.start();
    channelProvider = serviceHelper.createChannelProvider();
    fakeExecutor = new FakeScheduledExecutorService();
    testBigQueryWrite.setExecutor(fakeExecutor);
    Instant time = Instant.now();
    Timestamp timestamp =
        Timestamp.newBuilder().setSeconds(time.getEpochSecond()).setNanos(time.getNano()).build();
    // Add enough GetWriteStream response.
    for (int i = 0; i < 4; i++) {
      testBigQueryWrite.addResponse(
          Stream.WriteStream.newBuilder().setName(TEST_STREAM).setCreateTime(timestamp).build());
    }
  }

  @After
  public void tearDown() throws Exception {
    LOG.info("tearDown called");
    serviceHelper.stop();
  }

  private JsonStreamWriter.Builder getTestJsonStreamWriterBuilder(
      String testStream, Table.TableSchema BQTableSchema) {
    return JsonStreamWriter.newBuilder(testStream, BQTableSchema)
        .setChannelProvider(channelProvider)
        .setExecutorProvider(SINGLE_THREAD_EXECUTOR)
        .setCredentialsProvider(NoCredentialsProvider.create());
  }

  private JsonStreamWriter.Builder getTestSchemaUpdateBuilder(
      String testStream,
      Table.TableSchema BQTableSchema,
      OnSchemaUpdateRunnable onSchemaUpdateRunnable) {
    return JsonStreamWriter.newBuilder(testStream, BQTableSchema)
        .setChannelProvider(channelProvider)
        .setExecutorProvider(SINGLE_THREAD_EXECUTOR)
        .setCredentialsProvider(NoCredentialsProvider.create())
        .setOnSchemaUpdateRunnable(onSchemaUpdateRunnable);
  }

  @Test
  public void testTwoParamNewBuilder() throws Exception {
    try {
      getTestJsonStreamWriterBuilder(null, TABLE_SCHEMA);
    } catch (NullPointerException e) {
      assertEquals(e.getMessage(), "StreamName is null.");
    }

    try {
      getTestJsonStreamWriterBuilder(TEST_STREAM, null);
    } catch (NullPointerException e) {
      assertEquals(e.getMessage(), "TableSchema is null.");
    }

    JsonStreamWriter writer = getTestJsonStreamWriterBuilder(TEST_STREAM, TABLE_SCHEMA).build();
    assertEquals(TABLE_SCHEMA, writer.getTableSchema());
    assertEquals(TEST_STREAM, writer.getStreamName());
  }

  @Test
  public void testSingleAppendSimpleJson() throws Exception {
    FooType expectedProto = FooType.newBuilder().setFoo("allen").build();
    JSONObject foo = new JSONObject();
    foo.put("foo", "allen");
    JSONArray jsonArr = new JSONArray();
    jsonArr.put(foo);

    JsonStreamWriter writer = getTestJsonStreamWriterBuilder(TEST_STREAM, TABLE_SCHEMA).build();
    testBigQueryWrite.addResponse(Storage.AppendRowsResponse.newBuilder().setOffset(0).build());

    ApiFuture<AppendRowsResponse> appendFuture =
        writer.append(jsonArr, -1, /* allowUnknownFields */ false);

    assertEquals(0L, appendFuture.get().getOffset());
    assertEquals(
        1,
        testBigQueryWrite
            .getAppendRequests()
            .get(0)
            .getProtoRows()
            .getRows()
            .getSerializedRowsCount());
    assertEquals(
        testBigQueryWrite.getAppendRequests().get(0).getProtoRows().getRows().getSerializedRows(0),
        expectedProto.toByteString());
    writer.close();
  }

  @Test
  public void testSingleAppendMultipleSimpleJson() throws Exception {
    FooType expectedProto = FooType.newBuilder().setFoo("allen").build();
    JSONObject foo = new JSONObject();
    foo.put("foo", "allen");
    JSONObject foo1 = new JSONObject();
    foo1.put("foo", "allen");
    JSONObject foo2 = new JSONObject();
    foo2.put("foo", "allen");
    JSONObject foo3 = new JSONObject();
    foo3.put("foo", "allen");
    JSONArray jsonArr = new JSONArray();
    jsonArr.put(foo);
    jsonArr.put(foo1);
    jsonArr.put(foo2);
    jsonArr.put(foo3);

    JsonStreamWriter writer = getTestJsonStreamWriterBuilder(TEST_STREAM, TABLE_SCHEMA).build();
    testBigQueryWrite.addResponse(Storage.AppendRowsResponse.newBuilder().setOffset(0).build());

    ApiFuture<AppendRowsResponse> appendFuture =
        writer.append(jsonArr, -1, /* allowUnknownFields */ false);

    assertEquals(0L, appendFuture.get().getOffset());
    assertEquals(
        4,
        testBigQueryWrite
            .getAppendRequests()
            .get(0)
            .getProtoRows()
            .getRows()
            .getSerializedRowsCount());
    for (int i = 0; i < 4; i++) {
      assertEquals(
          testBigQueryWrite
              .getAppendRequests()
              .get(0)
              .getProtoRows()
              .getRows()
              .getSerializedRows(i),
          expectedProto.toByteString());
    }
    writer.close();
  }

  @Test
  public void testMultipleAppendSimpleJson() throws Exception {
    FooType expectedProto = FooType.newBuilder().setFoo("allen").build();
    JSONObject foo = new JSONObject();
    foo.put("foo", "allen");
    JSONArray jsonArr = new JSONArray();
    jsonArr.put(foo);

    JsonStreamWriter writer = getTestJsonStreamWriterBuilder(TEST_STREAM, TABLE_SCHEMA).build();
    testBigQueryWrite.addResponse(Storage.AppendRowsResponse.newBuilder().setOffset(0).build());
    testBigQueryWrite.addResponse(Storage.AppendRowsResponse.newBuilder().setOffset(1).build());
    testBigQueryWrite.addResponse(Storage.AppendRowsResponse.newBuilder().setOffset(2).build());
    testBigQueryWrite.addResponse(Storage.AppendRowsResponse.newBuilder().setOffset(3).build());
    ApiFuture<AppendRowsResponse> appendFuture;
    for (int i = 0; i < 4; i++) {
      appendFuture = writer.append(jsonArr, -1, /* allowUnknownFields */ false);

      assertEquals((long) i, appendFuture.get().getOffset());
      assertEquals(
          1,
          testBigQueryWrite
              .getAppendRequests()
              .get(i)
              .getProtoRows()
              .getRows()
              .getSerializedRowsCount());
      assertEquals(
          testBigQueryWrite
              .getAppendRequests()
              .get(i)
              .getProtoRows()
              .getRows()
              .getSerializedRows(0),
          expectedProto.toByteString());
    }
    writer.close();
  }

  @Test
  public void testSingleAppendComplexJson() throws Exception {
    ComplexRoot expectedProto =
        ComplexRoot.newBuilder()
            .setTestInt(1)
            .addTestString("a")
            .addTestString("b")
            .addTestString("c")
            .setTestBytes(ByteString.copyFrom("hello".getBytes()))
            .setTestBool(true)
            .addTestDouble(1.1)
            .addTestDouble(2.2)
            .addTestDouble(3.3)
            .addTestDouble(4.4)
            .setTestDate(1)
            .setComplexLvl1(
                com.google.cloud.bigquery.storage.test.JsonTest.ComplexLvl1.newBuilder()
                    .setTestInt(2)
                    .setComplexLvl2(
                        com.google.cloud.bigquery.storage.test.JsonTest.ComplexLvl2.newBuilder()
                            .setTestInt(3)
                            .build())
                    .build())
            .setComplexLvl2(
                com.google.cloud.bigquery.storage.test.JsonTest.ComplexLvl2.newBuilder()
                    .setTestInt(3)
                    .build())
            .build();
    JSONObject complex_lvl2 = new JSONObject();
    complex_lvl2.put("test_int", 3);

    JSONObject complex_lvl1 = new JSONObject();
    complex_lvl1.put("test_int", 2);
    complex_lvl1.put("complex_lvl2", complex_lvl2);

    JSONObject json = new JSONObject();
    json.put("test_int", 1);
    json.put("test_string", new JSONArray(new String[] {"a", "b", "c"}));
    json.put("test_bytes", "hello");
    json.put("test_bool", true);
    json.put("test_DOUBLe", new JSONArray(new Double[] {1.1, 2.2, 3.3, 4.4}));
    json.put("test_date", 1);
    json.put("complex_lvl1", complex_lvl1);
    json.put("complex_lvl2", complex_lvl2);
    JSONArray jsonArr = new JSONArray();
    jsonArr.put(json);

    JsonStreamWriter writer =
        getTestJsonStreamWriterBuilder(TEST_STREAM, COMPLEX_TABLE_SCHEMA).build();
    testBigQueryWrite.addResponse(Storage.AppendRowsResponse.newBuilder().setOffset(0).build());
    ApiFuture<AppendRowsResponse> appendFuture =
        writer.append(jsonArr, -1, /* allowUnknownFields */ false);

    assertEquals(0L, appendFuture.get().getOffset());
    assertEquals(
        1,
        testBigQueryWrite
            .getAppendRequests()
            .get(0)
            .getProtoRows()
            .getRows()
            .getSerializedRowsCount());
    assertEquals(
        testBigQueryWrite.getAppendRequests().get(0).getProtoRows().getRows().getSerializedRows(0),
        expectedProto.toByteString());
    writer.close();
  }

  private final OnSchemaUpdateRunnable ON_SCHEMA_UPDATE_RUNNABLE =
      new OnSchemaUpdateRunnable() {
        public void run() {
          this.getStreamWriter().setUpdatedSchema(this.getUpdatedSchema());
          try {
            this.getJsonStreamWriter().setDescriptor(this.getUpdatedSchema());
          } catch (Descriptors.DescriptorValidationException e) {
            LOG.severe(
                "Schema update fail: updated schema could not be converted to a valid descriptor.");
            return;
          }

          try {
            this.getStreamWriter().refreshAppend();
          } catch (IOException | InterruptedException e) {
            LOG.severe(
                "Schema update error: Got exception while reestablishing connection for schema update.");
            return;
          }

          LOG.info("Successfully updated schema: " + this.getUpdatedSchema());
        }
      };

  @Test
  public void testAppendSchemaUpdate() throws Exception {
    JsonStreamWriter writer =
        getTestSchemaUpdateBuilder(TEST_STREAM, TABLE_SCHEMA, ON_SCHEMA_UPDATE_RUNNABLE).build();
    // Add fake resposne for FakeBigQueryWrite, first response has updated schema.
    testBigQueryWrite.addResponse(
        Storage.AppendRowsResponse.newBuilder()
            .setOffset(0)
            .setUpdatedSchema(UPDATED_TABLE_SCHEMA)
            .build());
    testBigQueryWrite.addResponse(Storage.AppendRowsResponse.newBuilder().setOffset(1).build());
    // First append
    JSONObject foo = new JSONObject();
    foo.put("foo", "allen");
    JSONArray jsonArr = new JSONArray();
    jsonArr.put(foo);

    ApiFuture<AppendRowsResponse> appendFuture1 =
        writer.append(jsonArr, -1, /* allowUnknownFields */ false);

    Table.TableSchema updatedSchema = appendFuture1.get().getUpdatedSchema();
    int millis = 0;
    while (millis < 1000) {
      if (updatedSchema.toString().equals(writer.getTableSchema().toString())) {
        break;
      }
      Thread.sleep(10);
      millis += 10;
    }
    LOG.info("Took " + millis + " millis to finish schema update.");
    assertEquals(0L, appendFuture1.get().getOffset());
    assertEquals(
        1,
        testBigQueryWrite
            .getAppendRequests()
            .get(0)
            .getProtoRows()
            .getRows()
            .getSerializedRowsCount());
    assertEquals(
        testBigQueryWrite.getAppendRequests().get(0).getProtoRows().getRows().getSerializedRows(0),
        FooType.newBuilder().setFoo("allen").build().toByteString());

    // Second append with updated schema.
    JSONObject updatedFoo = new JSONObject();
    updatedFoo.put("foo", "allen");
    updatedFoo.put("bar", "allen2");
    JSONArray updatedJsonArr = new JSONArray();
    updatedJsonArr.put(updatedFoo);

    ApiFuture<AppendRowsResponse> appendFuture2 =
        writer.append(updatedJsonArr, -1, /* allowUnknownFields */ false);

    assertEquals(1L, appendFuture2.get().getOffset());
    assertEquals(
        testBigQueryWrite.getAppendRequests().get(1).getProtoRows().getRows().getSerializedRows(0),
        UpdatedFooType.newBuilder().setFoo("allen").setBar("allen2").build().toByteString());
    assertEquals(
        1,
        testBigQueryWrite
            .getAppendRequests()
            .get(1)
            .getProtoRows()
            .getRows()
            .getSerializedRowsCount());
    // Check if writer schemas were added in for both connections.
    assertTrue(testBigQueryWrite.getAppendRequests().get(0).getProtoRows().hasWriterSchema());
    assertTrue(testBigQueryWrite.getAppendRequests().get(1).getProtoRows().hasWriterSchema());
    // .equals() method implemented in the Message interface to check if table schemas are the same.
    assertEquals(
        ProtoSchemaConverter.convert(
            BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(TABLE_SCHEMA)),
        testBigQueryWrite.getAppendRequests().get(0).getProtoRows().getWriterSchema());
    assertEquals(
        ProtoSchemaConverter.convert(
            BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
                UPDATED_TABLE_SCHEMA)),
        testBigQueryWrite.getAppendRequests().get(1).getProtoRows().getWriterSchema());
    writer.close();
  }

  @Test
  public void testAppendAlreadyExistsException() throws Exception {
    JsonStreamWriter writer =
        getTestSchemaUpdateBuilder(TEST_STREAM, TABLE_SCHEMA, ON_SCHEMA_UPDATE_RUNNABLE).build();
    testBigQueryWrite.addResponse(
        Storage.AppendRowsResponse.newBuilder()
            .setError(com.google.rpc.Status.newBuilder().setCode(6).build())
            .build());
    JSONObject foo = new JSONObject();
    foo.put("foo", "allen");
    JSONArray jsonArr = new JSONArray();
    jsonArr.put(foo);
    ApiFuture<AppendRowsResponse> appendFuture =
        writer.append(jsonArr, -1, /* allowUnknownFields */ false);
    try {
      appendFuture.get();
    } catch (Throwable t) {
      assertEquals(t.getCause().getMessage(), "ALREADY_EXISTS: ");
    }
    writer.close();
  }

  @Test
  public void testAppendOutOfRangeException() throws Exception {
    JsonStreamWriter writer =
        getTestSchemaUpdateBuilder(TEST_STREAM, TABLE_SCHEMA, ON_SCHEMA_UPDATE_RUNNABLE).build();
    testBigQueryWrite.addResponse(
        Storage.AppendRowsResponse.newBuilder()
            .setError(com.google.rpc.Status.newBuilder().setCode(11).build())
            .build());
    JSONObject foo = new JSONObject();
    foo.put("foo", "allen");
    JSONArray jsonArr = new JSONArray();
    jsonArr.put(foo);
    ApiFuture<AppendRowsResponse> appendFuture =
        writer.append(jsonArr, -1, /* allowUnknownFields */ false);
    try {
      appendFuture.get();
    } catch (Throwable t) {
      assertEquals(t.getCause().getMessage(), "OUT_OF_RANGE: ");
    }
    writer.close();
  }

  @Test
  public void testAppendOutOfRangeAndUpdateSchema() throws Exception {
    JsonStreamWriter writer =
        getTestSchemaUpdateBuilder(TEST_STREAM, TABLE_SCHEMA, ON_SCHEMA_UPDATE_RUNNABLE).build();
    testBigQueryWrite.addResponse(
        Storage.AppendRowsResponse.newBuilder()
            .setError(com.google.rpc.Status.newBuilder().setCode(11).build())
            .setUpdatedSchema(UPDATED_TABLE_SCHEMA)
            .build());
    testBigQueryWrite.addResponse(Storage.AppendRowsResponse.newBuilder().setOffset(0).build());

    JSONObject foo = new JSONObject();
    foo.put("foo", "allen");
    JSONArray jsonArr = new JSONArray();
    jsonArr.put(foo);
    ApiFuture<AppendRowsResponse> appendFuture =
        writer.append(jsonArr, -1, /* allowUnknownFields */ false);
    try {
      appendFuture.get();
    } catch (Throwable t) {
      assertEquals(t.getCause().getMessage(), "OUT_OF_RANGE: ");
      int millis = 0;
      while (millis < 1000) {
        if (writer.getTableSchema().equals(UPDATED_TABLE_SCHEMA)) {
          break;
        }
        Thread.sleep(10);
        millis += 10;
      }
      assertEquals(writer.getTableSchema(), UPDATED_TABLE_SCHEMA);
    }

    JSONObject updatedFoo = new JSONObject();
    updatedFoo.put("foo", "allen");
    updatedFoo.put("bar", "allen2");
    JSONArray updatedJsonArr = new JSONArray();
    updatedJsonArr.put(updatedFoo);

    ApiFuture<AppendRowsResponse> appendFuture2 =
        writer.append(updatedJsonArr, -1, /* allowUnknownFields */ false);

    assertEquals(0L, appendFuture2.get().getOffset());
    assertEquals(
        testBigQueryWrite.getAppendRequests().get(1).getProtoRows().getRows().getSerializedRows(0),
        UpdatedFooType.newBuilder().setFoo("allen").setBar("allen2").build().toByteString());
    assertEquals(
        1,
        testBigQueryWrite
            .getAppendRequests()
            .get(1)
            .getProtoRows()
            .getRows()
            .getSerializedRowsCount());
    // Check if writer schemas were added in for both connections.
    assertTrue(testBigQueryWrite.getAppendRequests().get(0).getProtoRows().hasWriterSchema());
    assertTrue(testBigQueryWrite.getAppendRequests().get(1).getProtoRows().hasWriterSchema());
    // .equals() method implemented in the Message interface to check if table schemas are the same.
    assertEquals(
        ProtoSchemaConverter.convert(
            BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(TABLE_SCHEMA)),
        testBigQueryWrite.getAppendRequests().get(0).getProtoRows().getWriterSchema());
    assertEquals(
        ProtoSchemaConverter.convert(
            BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
                UPDATED_TABLE_SCHEMA)),
        testBigQueryWrite.getAppendRequests().get(1).getProtoRows().getWriterSchema());

    writer.close();
  }
}
