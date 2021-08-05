/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.statefun.flink.io.pulsar;

import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.common.json.Selectors;
import org.apache.flink.statefun.sdk.pulsar.egress.PulsarProducerSemantic;

final class PulsarEgressSpecJsonParser {

  private PulsarEgressSpecJsonParser() {}

  private static final JsonPointer PROPERTIES_POINTER =
      JsonPointer.compile("/egress/spec/properties");
  private static final JsonPointer ADDRESS_POINTER = JsonPointer.compile("/egress/spec/address");

  private static final JsonPointer DELIVERY_SEMANTICS_POINTER =
      JsonPointer.compile("/egress/spec/deliverySemantic");
  private static final JsonPointer DELIVERY_SEMANTICS_TYPE_POINTER =
      JsonPointer.compile("/egress/spec/deliverySemantic/type");

  private static final JsonPointer DELIVERY_EXACTLY_ONCE_DURATION_TXN_TIMEOUT_POINTER =
      JsonPointer.compile("/egress/spec/deliverySemantic/transactionTimeout");

  private static final JsonPointer SERVICE_URL = JsonPointer.compile("/egress/spec/service-url");
  private static final JsonPointer ADMIN_URL = JsonPointer.compile("/egress/spec/admin-url");

  static String pulsarServiceUrl(JsonNode json) {
    return Selectors.textAt(json, SERVICE_URL);
  }

  static String pulsarAdminUrl(JsonNode json) {
    return Selectors.textAt(json, ADMIN_URL);
  }

  static Properties kafkaClientProperties(JsonNode json) {
    Map<String, String> kvs = Selectors.propertiesAt(json, PROPERTIES_POINTER);
    Properties properties = new Properties();
    kvs.forEach(properties::setProperty);
    return properties;
  }

  static Optional<PulsarProducerSemantic> optionalDeliverySemantic(JsonNode json) {
    if (json.at(DELIVERY_SEMANTICS_POINTER).isMissingNode()) {
      return Optional.empty();
    }

    String deliverySemanticType =
        Selectors.textAt(json, DELIVERY_SEMANTICS_TYPE_POINTER).toLowerCase(Locale.ENGLISH);
    switch (deliverySemanticType) {
      case "at-least-once":
        return Optional.of(PulsarProducerSemantic.AT_LEAST_ONCE);
      case "exactly-once":
        return Optional.of(PulsarProducerSemantic.EXACTLY_ONCE);
      case "none":
        return Optional.of(PulsarProducerSemantic.NONE);
      default:
        throw new IllegalArgumentException(
            "Invalid delivery semantic type: "
                + deliverySemanticType
                + "; valid types are [at-least-once, exactly-once, none]");
    }
  }

  static Duration exactlyOnceDeliveryTxnTimeout(JsonNode json) {
    return Selectors.durationAt(json, DELIVERY_EXACTLY_ONCE_DURATION_TXN_TIMEOUT_POINTER);
  }
}
