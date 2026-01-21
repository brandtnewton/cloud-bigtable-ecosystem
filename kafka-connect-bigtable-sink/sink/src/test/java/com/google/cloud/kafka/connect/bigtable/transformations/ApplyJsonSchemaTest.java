package com.example.kafka.connect.smt;

import com.google.cloud.kafka.connect.bigtable.transformations.ApplyJsonSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class ApplyJsonSchemaTest {

  private ApplyJsonSchema.Value<SourceRecord> smt;

  // A complex schema with a nested struct and an array
  private static final String CONFIG_SCHEMA_JSON = "{" +
      "  \"type\": \"struct\"," +
      "  \"fields\": [" +
      "    { \"field\": \"id\", \"type\": \"int32\" }," +
      "    { \"field\": \"meta\", \"type\": \"struct\", \"fields\": [" +
      "        { \"field\": \"source\", \"type\": \"string\" }," +
      "        { \"field\": \"retry_count\", \"type\": \"int32\", \"optional\": true }" +
      "      ]" +
      "    }," +
      "    { \"field\": \"tags\", \"type\": \"array\", \"items\": { \"type\": \"string\" } }" +
      "  ]" +
      "}";

  @Before
  public void setUp() {
    smt = new ApplyJsonSchema.Value<>();
    Map<String, String> configs = new HashMap<>();
    configs.put(ApplyJsonSchema.SCHEMA_CONFIG, CONFIG_SCHEMA_JSON);
    smt.configure(configs);
  }

  @After
  public void tearDown() {
    smt.close();
  }

  @Test
  public void testApplyJsonSchemalessToStruct() {
    // 1. Construct the schemaless input (Nested Map)
    Map<String, Object> metaMap = new HashMap<>();
    metaMap.put("source", "api-v1");
    metaMap.put("retry_count", 5);

    Map<String, Object> rootMap = new HashMap<>();
    rootMap.put("id", 101);
    rootMap.put("meta", metaMap);
    rootMap.put("tags", Arrays.asList("production", "urgent"));

    // 2. Create the SourceRecord with NO schema
    SourceRecord record = new SourceRecord(
        null, null, "test-topic", 0,
        null, rootMap
    );

    // 3. Apply the SMT
    SourceRecord transformed = smt.apply(record);

    // 4. Assertions
    assertNotNull(transformed.valueSchema());
    assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type());
    assertTrue(transformed.value() instanceof Struct);

    Struct rootStruct = (Struct) transformed.value();
    assertEquals((Integer) 101, rootStruct.getInt32("id"));

    // Check Nested Struct
    Struct metaStruct = rootStruct.getStruct("meta");
    assertEquals("api-v1", metaStruct.getString("source"));
    assertEquals((Integer) 5, metaStruct.getInt32("retry_count"));

    // Check Array
    java.util.List<String> tags = rootStruct.getArray("tags");
    assertEquals(2, tags.size());
    assertEquals("production", tags.get(0));
  }

  @Test
  public void testOptionalFieldIsNull() {
    // 'retry_count' is optional in our JSON config. Let's omit it.
    Map<String, Object> metaMap = new HashMap<>();
    metaMap.put("source", "api-v2");
    // retry_count missing -> should be null

    Map<String, Object> rootMap = new HashMap<>();
    rootMap.put("id", 202);
    rootMap.put("meta", metaMap);
    rootMap.put("tags", Collections.emptyList());

    SourceRecord record = new SourceRecord(null, null, "topic", null, rootMap);
    SourceRecord transformed = smt.apply(record);

    Struct rootStruct = (Struct) transformed.value();
    Struct metaStruct = rootStruct.getStruct("meta");

    // Assert that the optional field is null
    assertNull(metaStruct.get("retry_count"));
  }

  @Test(expected = DataException.class)
  public void testMissingRequiredFieldThrowsException() {
    // 'source' inside 'meta' is REQUIRED. Let's omit it.
    Map<String, Object> metaMap = new HashMap<>();
    // source missing

    Map<String, Object> rootMap = new HashMap<>();
    rootMap.put("id", 303);
    rootMap.put("meta", metaMap);

    SourceRecord record = new SourceRecord(null, null, "topic", null, rootMap);

    // Should throw DataException because "source" is missing
    smt.apply(record);
  }

  @Test
  public void testTombstoneRecord() {
    // A tombstone record has a null value
    SourceRecord record = new SourceRecord(
        null, null, "test-topic", 0,
        null, null
    );

    SourceRecord transformed = smt.apply(record);

    assertNull(transformed.value());
    assertNull(transformed.valueSchema());
  }
}