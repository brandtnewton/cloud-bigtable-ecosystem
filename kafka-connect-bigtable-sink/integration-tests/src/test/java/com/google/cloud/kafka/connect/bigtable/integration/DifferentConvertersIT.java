/*
 * Copyright 2025 Google LLC
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
package com.google.cloud.kafka.connect.bigtable.integration;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.json.JsonSchemaConverter;
import io.confluent.connect.protobuf.ProtobufConverter;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class DifferentConvertersIT extends BaseDataGeneratorIT {
  private Supplier<Converter> converterConstructor;
  private String converterClassName;
  private Map<String, String> converterBaseConfig;
  private boolean converterUsesSchemaRegistry;

  @Parameterized.Parameters
  public static Collection testCases() {
    return Arrays.asList(
        new Object[][] {
          {(Supplier<Converter>) AvroConverter::new, "io.confluent.connect.avro.AvroConverter", Map.of(), true},
          {(Supplier<Converter>) ProtobufConverter::new, "io.confluent.connect.protobuf.ProtobufConverter", Map.of(), true},
          {(Supplier<Converter>) JsonSchemaConverter::new, "io.confluent.connect.json.JsonSchemaConverter", Map.of(), true},
          {
            (Supplier<Converter>) JsonConverter::new,
            "org.apache.kafka.connect.json.JsonConverter",
            Map.of("schemas.enable", String.valueOf(false)),
            false
          },
          {
            (Supplier<Converter>) JsonConverter::new,
            "org.apache.kafka.connect.json.JsonConverter",
            Map.of("schemas.enable", String.valueOf(true)),
            false
          },
        });
  }

  public DifferentConvertersIT(
      Supplier<Converter> converterConstructor,
      String converterClassName,
      Map<String, String> converterBaseConfig,
      boolean converterUsesSchemaRegistry) {
    this.converterConstructor = converterConstructor;
    this.converterClassName = converterClassName;
    this.converterBaseConfig = converterBaseConfig;
    this.converterUsesSchemaRegistry = converterUsesSchemaRegistry;
  }

  @Test
  public void testConverter() throws InterruptedException, ExecutionException {
    Map<String, String> converterProps = new HashMap<>(converterBaseConfig);
    if (converterUsesSchemaRegistry) {
      converterProps.put(
          "schema.registry.url",
          schemaRegistry.schemaRegistryUrl());
    }
    Converter keyConverter = converterConstructor.get();
    keyConverter.configure(converterProps, true);
    Converter valueConverter = converterConstructor.get();
    valueConverter.configure(converterProps, false);

    Map<String, String> connectorProps = baseConnectorProps();
    for (Map.Entry<String, String> prop : converterProps.entrySet()) {
      connectorProps.put(
          "key.converter" + "." + prop.getKey(), prop.getValue());
      connectorProps.put(
          "value.converter" + "." + prop.getKey(), prop.getValue());
    }
    connectorProps.put(
        "key.converter", converterClassName);
    connectorProps.put(
        "value.converter", converterClassName);
    String topic = startSingleTopicConnector(connectorProps);
    createTablesAndColumnFamilies(Map.of(topic, valueFields(topic)));
    connect
        .assertions()
        .assertConnectorAndExactlyNumTasksAreRunning(topic, numTasks, "Connector start timeout");
    populateKafkaTopic(topic, numRecords, keyConverter, valueConverter);

    waitUntilBigtableContainsNumberOfRows(topic, numRecords);
  }
}
