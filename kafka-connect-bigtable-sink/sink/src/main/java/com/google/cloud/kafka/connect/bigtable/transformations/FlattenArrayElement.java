package com.google.cloud.kafka.connect.bigtable.transformations;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class FlattenArrayElement<R extends ConnectRecord<R>> implements Transformation<R> {

  public static final String Array_FIELD_CONF = "array.field";
  public static final String ELEMENT_WRAPPER_CONF = "element.field";

  public static final ConfigDef CONFIG_DEF = new ConfigDef().define(Array_FIELD_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Field containing the list").define(ELEMENT_WRAPPER_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "The wrapper field inside the list items");

  private String arrayFieldName;
  private String elementWrapper;

  @Override
  public void configure(Map<String, ?> configs) {
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
    this.arrayFieldName = config.getString(Array_FIELD_CONF);
    this.elementWrapper = config.getString(ELEMENT_WRAPPER_CONF);
  }

  @Override
  public R apply(R record) {
    Field innerElementField = getArrayElementField(record);
    Schema resultSchema = convertSchema(record, innerElementField);
    Struct resultValue = copyOtherRootLevelFields(resultSchema, record.value());
    copyArrayField(record.value(), resultValue);
    return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), resultSchema, resultValue, record.timestamp());
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
  }

  private Field getArrayElementField(R record) {
    Field arrayField = record.valueSchema().field(arrayFieldName);
    if (arrayField == null) {
      throw new IllegalArgumentException("Unknown field: " + arrayFieldName);
    }

    if (arrayField.schema().type() != Schema.Type.ARRAY) {
      throw new IllegalArgumentException("Array field: '" + arrayFieldName + "' is not an array");
    }

    Field innerElementField = arrayField.schema().valueSchema().field(elementWrapper);
    if (innerElementField == null) {
      throw new IllegalArgumentException("Unknown array element field: " + elementWrapper);
    }
    return innerElementField;
  }

  private Schema convertSchema(R record, Field innerElementField) {
    // build a modified schema
    SchemaBuilder builder = SchemaBuilder.struct().name("FlattenedRecord");
    for (Field field : record.valueSchema().fields()) {
      if (!Objects.equals(field.name(), arrayFieldName)) {
        builder.field(field.name(), field.schema());
      } else {
        builder.field(arrayFieldName, SchemaBuilder.array(innerElementField.schema()));
      }
    }
    return builder.build();
  }

  private Struct copyOtherRootLevelFields(Schema schema, Object original) {
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
    return resultValue;
  }

  private void copyArrayField(Object original, Struct result) {
    if (original == null) {
      return;
    }
    if (!(original instanceof Struct)) {
      throw new IllegalArgumentException("Message value is not a struct");
    }
    List<Struct> array = new ArrayList<>();
    for (Struct element : ((Struct) original).<Struct>getArray(arrayFieldName)) {
      // unpack the element wrapper
      Struct innerValue = element.getStruct(this.elementWrapper);
      array.add(innerValue);
    }
    result.put(arrayFieldName, array);
  }
}
