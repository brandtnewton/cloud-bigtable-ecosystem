package com.google.cloud.kafka.connect.bigtable.util;

import com.google.cloud.kafka.connect.bigtable.mapping.LogicalTypeUtils;
import com.google.cloud.kafka.connect.bigtable.mapping.SchemaUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.util.Map;
import java.util.Optional;

public class SchemaParsingUtils {

  public static SchemaAndValue extractField(SchemaAndValue keySchemaAndValue, String[] fields) {
    return extractField(keySchemaAndValue, fields, 0);
  }

  /**
   * Extract possibly nested fields from the input value.
   *
   * @param keySchemaAndValue {@link org.apache.kafka.connect.sink.SinkRecord SinkRecord's} key or
   *                          some its child with corresponding {@link Schema}.
   * @param fields            Fields that need to be accessed before the target value is reached.
   * @param index             Index of the field that is being extracted.
   * @return Extracted nested field.
   */
  private static SchemaAndValue extractField(SchemaAndValue keySchemaAndValue, String[] fields, int index) {
    Object value = keySchemaAndValue.value();
    Optional<Schema> schema = Optional.ofNullable(keySchemaAndValue.schema());
    ensureKeyElementIsNotNull(value);
    LogicalTypeUtils.logIfLogicalTypeUnsupported(schema);
    if (index >= fields.length) {
      return keySchemaAndValue;
    }
    String field = fields[index];
    if (value instanceof Struct) {
      // Note that getWithoutDefault() throws if such a field does not exist.
      Object fieldValue = ((Struct) value).getWithoutDefault(field);
      Schema fieldSchema = SchemaUtils.maybeExtractFieldSchema(schema, field).orElse(null);
      return extractField(new SchemaAndValue(fieldSchema, fieldValue), fields, index + 1);
    } else if (value instanceof Map<?, ?>) {
      Object fieldValue = ((Map<?, ?>) value).get(field);
      Schema fieldSchema = SchemaUtils.maybeExtractFieldSchema(schema, field).orElse(null);
      return extractField(new SchemaAndValue(fieldSchema, fieldValue), fields, index + 1);
    } else {
      throw new DataException(
          "Unexpected class `"
              + value.getClass().getName()
              + "` doesn't support extracting field `"
              + field
              + "` using a dot.");
    }
  }

  public static void ensureKeyElementIsNotNull(Object value) {
    if (value == null) {
      // Matching Confluent's sink behavior.
      throw new DataException("Error with row key definition: row key fields cannot be null.");
    }
  }
}
