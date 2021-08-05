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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.common.json.NamespaceNamePair;
import org.apache.flink.statefun.flink.common.json.Selectors;
import org.apache.flink.statefun.flink.io.generated.RoutingConfig;
import org.apache.flink.statefun.flink.io.generated.TargetFunctionType;
import org.apache.flink.statefun.sdk.pulsar.PulsarTopicPartition;
import org.apache.flink.statefun.sdk.pulsar.ingress.PulsarIngressStartupPosition;

final class PulsarIngressSpecJsonParser {

  private PulsarIngressSpecJsonParser() {}

  private static final JsonPointer TOPICS_POINTER = JsonPointer.compile("/ingress/spec/topics");
  private static final JsonPointer PROPERTIES_POINTER =
      JsonPointer.compile("/ingress/spec/properties");
  private static final JsonPointer SERVICE_URL = JsonPointer.compile("/ingress/spec/service-url");
  private static final JsonPointer ADMIN_URL = JsonPointer.compile("/ingress/spec/admin-url");
  private static final JsonPointer SUBSCRIPTION = JsonPointer.compile("/ingress/spec/subscription");

  private static final JsonPointer STARTUP_POS_POINTER =
      JsonPointer.compile("/ingress/spec/startupPosition");
  private static final JsonPointer STARTUP_POS_TYPE_POINTER =
      JsonPointer.compile("/ingress/spec/startupPosition/type");
  private static final JsonPointer STARTUP_SPECIFIC_OFFSETS_POINTER =
      JsonPointer.compile("/ingress/spec/startupPosition/offsets");

  private static final JsonPointer ROUTABLE_TOPIC_NAME_POINTER = JsonPointer.compile("/topic");
  private static final JsonPointer ROUTABLE_TOPIC_VALUE_TYPE_POINTER =
      JsonPointer.compile("/valueType");
  private static final JsonPointer ROUTABLE_TOPIC_TARGETS_POINTER = JsonPointer.compile("/targets");

  static Map<String, RoutingConfig> routableTopics(JsonNode json) {
    Map<String, RoutingConfig> routableTopics = new HashMap<>();
    for (JsonNode routableTopicNode : Selectors.listAt(json, TOPICS_POINTER)) {
      final String topic = Selectors.textAt(routableTopicNode, ROUTABLE_TOPIC_NAME_POINTER);
      final String typeUrl = Selectors.textAt(routableTopicNode, ROUTABLE_TOPIC_VALUE_TYPE_POINTER);
      final List<TargetFunctionType> targets = parseRoutableTargetFunctionTypes(routableTopicNode);

      routableTopics.put(
          topic,
          RoutingConfig.newBuilder()
              .setTypeUrl(typeUrl)
              .addAllTargetFunctionTypes(targets)
              .build());
    }
    return routableTopics;
  }

  static Properties pulsarClientProperties(JsonNode json) {
    Map<String, String> kvs = Selectors.propertiesAt(json, PROPERTIES_POINTER);
    Properties properties = new Properties();
    kvs.forEach(properties::setProperty);
    return properties;
  }

  static String pulsarServiceUrl(JsonNode json) {
    return Selectors.textAt(json, SERVICE_URL);
  }

  static String pulsarAdminUrl(JsonNode json) {
    return Selectors.textAt(json, ADMIN_URL);
  }

  static Optional<String> optionalSubscription(JsonNode json) {
    return Selectors.optionalTextAt(json, SUBSCRIPTION);
  }

  static Optional<PulsarIngressStartupPosition> optionalStartupPosition(JsonNode json) {
    if (json.at(STARTUP_POS_POINTER).isMissingNode()) {
      return Optional.empty();
    }

    String startupType =
        Selectors.textAt(json, STARTUP_POS_TYPE_POINTER).toLowerCase(Locale.ENGLISH);
    switch (startupType) {
      case "external-subscription":
        return Optional.of(PulsarIngressStartupPosition.fromExternalSubscription());
      case "earliest":
        return Optional.of(PulsarIngressStartupPosition.fromEarliest());
      case "latest":
        return Optional.of(PulsarIngressStartupPosition.fromLatest());
      case "specific-offsets":
        return Optional.of(
            PulsarIngressStartupPosition.fromSpecificOffsets(specificOffsetsStartupMap(json)));
      default:
        throw new IllegalArgumentException(
            "Invalid startup position type: "
                + startupType
                + "; valid values are [external-subscription, earliest, latest, specific-offsets]");
    }
  }

  private static Map<PulsarTopicPartition, Long> specificOffsetsStartupMap(JsonNode json) {
    Map<String, Long> kvs = Selectors.longPropertiesAt(json, STARTUP_SPECIFIC_OFFSETS_POINTER);
    Map<PulsarTopicPartition, Long> offsets = new HashMap<>(kvs.size());
    kvs.forEach(
        (partition, offset) ->
            offsets.put(PulsarTopicPartition.fromString(partition), validateOffsetLong(offset)));
    return offsets;
  }

  private static Long validateOffsetLong(Long offset) {
    if (offset < 0) {
      throw new IllegalArgumentException(
          "Invalid offset value: "
              + offset
              + "; must be a numeric integer with value between 0 and "
              + Long.MAX_VALUE);
    }

    return offset;
  }

  private static List<TargetFunctionType> parseRoutableTargetFunctionTypes(
      JsonNode routableTopicNode) {
    final List<TargetFunctionType> targets = new ArrayList<>();
    for (String namespaceAndName :
        Selectors.textListAt(routableTopicNode, ROUTABLE_TOPIC_TARGETS_POINTER)) {
      NamespaceNamePair namespaceNamePair = NamespaceNamePair.from(namespaceAndName);
      targets.add(
          TargetFunctionType.newBuilder()
              .setNamespace(namespaceNamePair.namespace())
              .setType(namespaceNamePair.name())
              .build());
    }
    return targets;
  }
}
