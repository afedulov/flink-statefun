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
package org.apache.flink.statefun.sdk.pulsar.ingress;

import java.util.Objects;
import java.util.Properties;
import org.apache.flink.statefun.sdk.IngressType;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.pulsar.PulsarConfig;
import org.apache.flink.statefun.sdk.pulsar.PulsarIOTypes;

public class PulsarIngressSpec<T> implements IngressSpec<T> {
  private final Properties properties;
  private final String adminUrl;
  private final String serviceUrl;
  private final PulsarIngressDeserializer<T> deserializer;
  private final PulsarIngressStartupPosition startupPosition;
  private final IngressIdentifier<T> ingressIdentifier;

  PulsarIngressSpec(
      IngressIdentifier<T> id,
      Properties properties,
      String serviceUrl,
      String adminUrl,
      PulsarIngressDeserializer<T> deserializer,
      PulsarIngressStartupPosition startupPosition) {
    requireTopicSetting(properties);
    this.properties = requireValidProperties(properties);
    this.serviceUrl = Objects.requireNonNull(serviceUrl);
    this.adminUrl = Objects.requireNonNull(adminUrl);
    this.startupPosition = requireValidStartupPosition(startupPosition, properties);

    this.deserializer = Objects.requireNonNull(deserializer);
    this.ingressIdentifier = Objects.requireNonNull(id);
  }

  @Override
  public IngressIdentifier<T> id() {
    return ingressIdentifier;
  }

  @Override
  public IngressType type() {
    return PulsarIOTypes.UNIVERSAL_INGRESS_TYPE;
  }

  public Properties properties() {
    return properties;
  }

  public String serviceUrl() {
    return serviceUrl;
  }

  public String adminUrl() {
    return adminUrl;
  }

  public PulsarIngressDeserializer<T> deserializer() {
    return deserializer;
  }

  public PulsarIngressStartupPosition startupPosition() {
    return startupPosition;
  }

  private static Properties requireValidProperties(Properties properties) {
    Objects.requireNonNull(properties);

    if (!properties.containsKey(PulsarConfig.SERVICE_URL)) {
      throw new IllegalArgumentException("Missing setting for Pulsar address.");
    }

    // TODO: Check which properties are needed for Pulsar?

    // TODO: we eventually want to make the ingress work out-of-the-box without the need to set the
    // consumer group id
    //    if (!properties.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
    //      throw new IllegalArgumentException("Missing setting for consumer group id.");
    //    }

    return properties;
  }

  private static void requireTopicSetting(Properties properties) {
    if (properties.get(PulsarConfig.TOPIC_SINGLE_OPTION_KEY) == null
        && properties.get(PulsarConfig.TOPIC_MULTI_OPTION_KEY) == null
        && properties.get(PulsarConfig.TOPIC_PATTERN_OPTION_KEY) == null) {
      throw new IllegalArgumentException("Must define at least one Pulsar topic to consume from.");
    }
  }

  private static PulsarIngressStartupPosition requireValidStartupPosition(
      PulsarIngressStartupPosition startupPosition, Properties properties) {
    if (startupPosition.isExternalSubscription()) {
      // TODO: Check which properties are needed for Pulsar?
      //        && !properties.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
      throw new IllegalStateException(
          "The ingress is configured to start from committed consumer group offsets in Kafka, but no consumer group id was set.\n"
              + "Please set the group id with the withConsumerGroupId(String) method.");
    }

    return startupPosition;
  }
}
