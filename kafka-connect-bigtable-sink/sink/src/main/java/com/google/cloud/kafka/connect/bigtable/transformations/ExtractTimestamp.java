/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.kafka.connect.bigtable.transformations;

import com.google.cloud.kafka.connect.bigtable.util.SchemaParsingUtils;
import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public abstract class ExtractTimestamp<R extends ConnectRecord<R>> implements Transformation<R> {

  public static final String TIMESTAMP_FIELD_NAME = "timestamp.field";
  public static final String TIMESTAMP_FORMAT_TYPE = "timestamp.format";

  public static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              TIMESTAMP_FIELD_NAME,
              ConfigDef.Type.STRING,
              ConfigDef.Importance.HIGH,
              "The name of the timestamp field.")
          .define(
              TIMESTAMP_FORMAT_TYPE,
              ConfigDef.Type.STRING,
              ConfigDef.Importance.HIGH,
              "TODO");

  private String[] fieldPath;
  private ExtractTimestampFormat elementWrapperFieldName;

  @Override
  public void configure(Map<String, ?> configs) {
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
    this.fieldPath = config.getString(TIMESTAMP_FIELD_NAME).split("\\.");
    this.elementWrapperFieldName = ExtractTimestampFormat.valueOf(config.getString(TIMESTAMP_FORMAT_TYPE));
  }

  @Override
  public R apply(R record) {
    SchemaAndValue timestampField = SchemaParsingUtils.extractField(getOperatingValue(record), fieldPath);
    long parsedTimestampMillis = ExtractTimestamp.parseTimestampToMillis(timestampField, elementWrapperFieldName);
    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        record.valueSchema(),
        record.value(),
        parsedTimestampMillis
    );
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
  }

  protected abstract SchemaAndValue getOperatingValue(R record);

  @VisibleForTesting
  static long parseTimestampToMillis(SchemaAndValue value, ExtractTimestampFormat timestampFormat) {
    if (value == null || value.value() == null) {
      throw new IllegalArgumentException("Cannot parse timestamp value of null");
    }

    Object rawValue = value.value();

    // Handle native Connect logical Timestamp/Date objects directly
    if (rawValue instanceof java.util.Date) {
      return ((java.util.Date) rawValue).getTime();
    }
    if (rawValue instanceof java.time.Instant) {
      return ((java.time.Instant) rawValue).toEpochMilli();
    }

    // Extract the epoch number safely (supports both Schema-based and Schemaless records)
    long epochValue;
    if (rawValue instanceof Number) {
      epochValue = ((Number) rawValue).longValue();
    } else if (rawValue instanceof String) {
      String strVal = ((String) rawValue).trim();
      try {
        epochValue = Long.parseLong(strVal);
      } catch (NumberFormatException e) {
        // Graceful fallback if the string is represented as a floating-point epoch (e.g., "1715698738.123")
        epochValue = (long) Double.parseDouble(strVal);
      }
    } else {
      throw new IllegalArgumentException(
          "Unsupported timestamp payload type: " + rawValue.getClass().getName());
    }

    // Resolve to target milliseconds resolution
    switch (timestampFormat) {
      case NANOS:
        return epochValue / 1_000_000L;
      case MICROS:
        return epochValue / 1000L;
      case MILLIS:
        return epochValue;
      case SECONDS:
        return epochValue * 1000L;
      default:
        throw new IllegalStateException("Unexpected timestamp format: " + timestampFormat);
    }
  }


  // Boilerplate for Key/Value distinct implementations
  public static class Key<R extends ConnectRecord<R>> extends ExtractTimestamp<R> {

    @Override
    protected SchemaAndValue getOperatingValue(R record) {
      return new SchemaAndValue(record.keySchema(), record.key());
    }
  }

  public static class Value<R extends ConnectRecord<R>> extends ExtractTimestamp<R> {
    @Override
    protected SchemaAndValue getOperatingValue(R record) {
      return new SchemaAndValue(record.valueSchema(), record.value());
    }
  }
}
