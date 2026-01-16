package com.google.cloud.kafka.connect.bigtable.transformations;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.*;

import static org.junit.Assert.*;


@RunWith(JUnit4.class)
public class FlattenArrayElementTest {

  private FlattenArrayElement<SourceRecord> getTransformer() {
    FlattenArrayElement<SourceRecord> xform = new FlattenArrayElement<>();
    Map<String, String> props = new HashMap<>();
    props.put("array.fields", "products");
    props.put("element.fields", "element");
    xform.configure(props);
    return xform;
  }

  @Test
  public void testApply_Success() {
    FlattenArrayElement<SourceRecord> xform = getTransformer();
    // 1. Define the inner schema (the data we want to keep)
    Schema elementSchema = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .field("value", Schema.STRING_SCHEMA)
        .build();

    // 2. Define the wrapper schema (the "element" wrapper inside the list)
    Schema wrapperSchema = SchemaBuilder.struct()
        .field("element", elementSchema)
        .build();

    // 3. Define the full original schema
    Schema originalSchema = SchemaBuilder.struct()
        .field("orderId", Schema.STRING_SCHEMA)
        .field("products", SchemaBuilder.array(wrapperSchema).build())
        .build();

    Struct product1 = new Struct(wrapperSchema)
        .put("element",
            new Struct(elementSchema)
                .put("name", "goop")
                .put("value", "24")
        );

    Struct product2 = new Struct(wrapperSchema)
        .put("element",
            new Struct(elementSchema)
                .put("name", "toothpicks")
                .put("value", "246")
        );

    List<Struct> productsList = new ArrayList<>();
    productsList.add(product1);
    productsList.add(product2);

    Struct originalValue = new Struct(originalSchema)
        .put("orderId", "ORD-123")
        .put("products", productsList);

    // 5. Run the transformation
    SourceRecord record = new SourceRecord(null, null, "test-topic", 0,
        null, null, originalSchema, originalValue);

    SourceRecord transformedRecord = xform.apply(record);

    // 6. Assertions
    assertNotNull(transformedRecord);
    Struct resultValue = (Struct) transformedRecord.value();

    // Verify root level field still exists
    assertEquals("ORD-123", resultValue.get("orderId"));

    // Verify the array structure is flattened (unwrapped from "element")
    List<Struct> resultList = resultValue.getArray("products");
    assertEquals(2, resultList.size());

    Struct flattenedItem1 = resultList.get(0);
    assertEquals("goop", flattenedItem1.getString("name"));
    assertEquals("24", flattenedItem1.getString("value"));

    Struct flattenedItem2 = resultList.get(1);
    assertEquals("toothpicks", flattenedItem2.getString("name"));
    assertEquals("246", flattenedItem2.getString("value"));

    // Verify schema change: The array's value schema should now be the elementSchema
    assertEquals(Schema.Type.ARRAY, resultValue.schema().field("products").schema().type());
    assertEquals(elementSchema.fields().size(),
        resultValue.schema().field("products").schema().valueSchema().fields().size());
  }

  @Test
  public void testApply_ThrowsExceptionOnMissingField() {
    FlattenArrayElement<SourceRecord> xform = getTransformer();
    Schema simpleSchema = SchemaBuilder.struct().field("wrongField", Schema.STRING_SCHEMA).build();
    Struct simpleValue = new Struct(simpleSchema).put("wrongField", "data");

    SourceRecord record = new SourceRecord(null, null, "test-topic", 0,
        null, null, simpleSchema, simpleValue);

    assertThrows(IllegalArgumentException.class, () -> xform.apply(record));
  }
}
