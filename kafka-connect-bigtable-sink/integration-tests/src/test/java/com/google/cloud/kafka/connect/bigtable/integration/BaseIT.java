/*
 * Copyright 2024 Google LLC
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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public abstract class BaseIT {
  public static final String CREDENTIALS_PATH_ENV_VAR = "GOOGLE_APPLICATION_CREDENTIALS";
  public static final String GCP_PROJECT_ID = "PROJECT_ID";
  public static final String BIGTABLE_INSTANCE_ID = "INSTANCE_ID";
  public static final String CONNECTOR_CLASS_NAME =
      "com.google.cloud.kafka.connect.bigtable.BigtableSinkConnector";

  public int numTasks = 1;
  public int maxKafkaMessageSizeBytes = 300 * 1024 * 1024;

  public Map<String, String> baseConnectorProps() {
    Map<String, String> result = new HashMap<>();

    result.put("connector.class", CONNECTOR_CLASS_NAME);
    result.put("tasks.max", Integer.toString(numTasks));
    result.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
    result.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
    // Needed so that all messages we send to the input topics can be also sent to the DLQ by
    // DeadLetterQueueReporter.
    result.put("producer.max.request.size", String.valueOf(maxKafkaMessageSizeBytes));
    result.put("producer.buffer.memory", String.valueOf(maxKafkaMessageSizeBytes));

    if (System.getenv().containsKey("BIGTABLE_EMULATOR_HOST")) {
      result.put("gcp.bigtable.project.id", "EMULATOR");
      result.put("gcp.bigtable.instance.id", "EMULATOR");
    } else {
      result.put("gcp.bigtable.project.id", Objects.requireNonNull(System.getenv(GCP_PROJECT_ID)));
      result.put(
          "gcp.bigtable.instance.id", Objects.requireNonNull(System.getenv(BIGTABLE_INSTANCE_ID)));
      result.put(
          "gcp.bigtable.credentials.path",
          Objects.requireNonNull(System.getenv(CREDENTIALS_PATH_ENV_VAR)));
    }

    return result;
  }

  public BigtableDataClient getBigtableDataClient(Map<String, String> configProps) {
    return new BigtableSinkConfig(configProps).getBigtableDataClient();
  }

  public BigtableTableAdminClientInterface getBigtableAdminClient(Map<String, String> configProps) {
    return new BigtableSinkConfig(configProps).getBigtableAdminClient();
  }

  public String getTestClassId() {
    return TestId.getTestClassId(this.getClass());
  }

  public String getTestCaseId() {
    return TestId.getTestCaseId(this.getClass());
  }
}
