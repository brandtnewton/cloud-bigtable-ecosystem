package com.google.cloud.kafka.connect.bigtable.mapping;

import com.google.cloud.kafka.connect.bigtable.config.KafkaMessageComponent;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;

public class RecordDataExtractor {
  private final KafkaMessageComponent messageComponent;

  public RecordDataExtractor(KafkaMessageComponent messageComponent) {
    this.messageComponent = messageComponent;
  }

  public SchemaAndValue getValue(SinkRecord record) {
    if (this.messageComponent == KafkaMessageComponent.VALUE) {
      return new SchemaAndValue(record.valueSchema(), record.value());
    }
    return new SchemaAndValue(record.keySchema(), record.key());
  }
}
