package com.google.cloud.kafka.connect.bigtable.transformations;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class FlattenArrayElement<R extends ConnectRecord<R>> implements Transformation<R> {

  public static final String ARRAY_FIELD_CONF = "array.fields";
  public static final String ELEMENT_WRAPPER_CONF = "element.fields";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(ARRAY_FIELD_CONF, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "Field containing the list")
      .define(ELEMENT_WRAPPER_CONF, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "The wrapper field inside the list items");

  private List<String> arrayFieldNames;
  private List<String> elementWrapperNames;

  @Override
  public void configure(Map<String, ?> configs) {
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
    this.arrayFieldNames = config.getList(ARRAY_FIELD_CONF);
    this.elementWrapperNames = config.getList(ELEMENT_WRAPPER_CONF);
    if (arrayFieldNames.size() != elementWrapperNames.size()) {
      throw new ConfigException("arrayFieldNames", this.arrayFieldNames, String.format("%s and %s must be the same length", ARRAY_FIELD_CONF, ELEMENT_WRAPPER_CONF));
    }
  }

  @Override
  public R apply(R record) {
    Schema schema = record.valueSchema();
    Object value = record.value();
    for (int i = 0; i < arrayFieldNames.size(); i++) {
      String arrayFieldName = arrayFieldNames.get(i);
      String elementWrapperFieldName = elementWrapperNames.get(i);

      schema = convertSchema(schema, arrayFieldName, elementWrapperFieldName);
      value = convertValue(schema, value, arrayFieldName, elementWrapperFieldName);
    }
    return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), schema, value, record.timestamp());
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
  }

  private Field getArrayElementField(Schema schema, String arrayFieldName, String elementWrapperName) {
    if (schema == null) {
      throw new IllegalArgumentException("No schema");
    }
    Field arrayField = schema.field(arrayFieldName);
    if (arrayField == null) {
      throw new IllegalArgumentException("Unknown field: " + arrayFieldName);
    }

    if (arrayField.schema().type() != Schema.Type.ARRAY) {
      throw new IllegalArgumentException("Array field: '" + arrayFieldName + "' is not an array");
    }

    Field innerElementField = arrayField.schema().valueSchema().field(elementWrapperName);
    if (innerElementField == null) {
      throw new IllegalArgumentException("Unknown array element field: " + elementWrapperName);
    }
    return innerElementField;
  }

  private Schema convertSchema(Schema schema, String arrayFieldName, String elementWrapperName) {
    // build a modified schema
    SchemaBuilder builder = SchemaBuilder.struct().name("FlattenedRecord");
    for (Field field : schema.fields()) {
      if (!Objects.equals(field.name(), arrayFieldName)) {
        builder.field(field.name(), field.schema());
      } else {
        Field innerElementField = getArrayElementField(schema, arrayFieldName, elementWrapperName);
        builder.field(arrayFieldName, SchemaBuilder.array(innerElementField.schema()));
      }
    }
    return builder.build();
  }

  private Struct convertValue(Schema schema, Object original, String arrayFieldName, String elementWrapperName) {
    if (original == null) {
      return null;
    }
    if (!(original instanceof Struct)) {
      throw new IllegalArgumentException("Message value is not a struct");
    }
    Struct originalStruct = (Struct) original;
    // copy other root fields over
    Struct resultValue = new Struct(schema);
    for (Field field : resultValue.schema().fields()) {
      if (!Objects.equals(field.name(), arrayFieldName)) {
        resultValue.put(field.name(), originalStruct.get(field));
      }
    }

    // add the array
    List<Struct> array = new ArrayList<>();
    for (Struct element : originalStruct.<Struct>getArray(arrayFieldName)) {
      // unpack the element wrapper
      Struct innerValue = element.getStruct(elementWrapperName);
      array.add(innerValue);
    }
    resultValue.put(arrayFieldName, array);
    return resultValue;
  }
}
