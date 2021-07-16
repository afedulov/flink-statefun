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

import static org.apache.flink.statefun.flink.io.pulsar.PulsarIngressSpecJsonParser.optionalAutoOffsetResetPosition;
import static org.apache.flink.statefun.flink.io.pulsar.PulsarIngressSpecJsonParser.optionalStartupPosition;
import static org.apache.flink.statefun.flink.io.pulsar.PulsarIngressSpecJsonParser.optionalSubscription;
import static org.apache.flink.statefun.flink.io.pulsar.PulsarIngressSpecJsonParser.pulsarAdminUrl;
import static org.apache.flink.statefun.flink.io.pulsar.PulsarIngressSpecJsonParser.pulsarClientProperties;
import static org.apache.flink.statefun.flink.io.pulsar.PulsarIngressSpecJsonParser.pulsarServiceUrl;
import static org.apache.flink.statefun.flink.io.pulsar.PulsarIngressSpecJsonParser.routableTopics;

import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.Map;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.io.generated.RoutingConfig;
import org.apache.flink.statefun.flink.io.spi.JsonIngressSpec;
import org.apache.flink.statefun.flink.io.spi.SourceProvider;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.pulsar.PulsarIngressBuilder;
import org.apache.flink.statefun.sdk.pulsar.PulsarIngressBuilderApiExtension;
import org.apache.flink.statefun.sdk.pulsar.PulsarIngressDeserializer;
import org.apache.flink.statefun.sdk.pulsar.PulsarIngressSpec;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

final class RoutableProtobufPulsarSourceProvider implements SourceProvider {

  private final PulsarSourceProvider delegateProvider = new PulsarSourceProvider();

  @Override
  public <T> SourceFunction<T> forSpec(IngressSpec<T> spec) {
    PulsarIngressSpec<T> pulsarIngressSpec = asPulsarIngressSpec(spec);
    return delegateProvider.forSpec(pulsarIngressSpec);
  }

  private static <T> PulsarIngressSpec<T> asPulsarIngressSpec(IngressSpec<T> spec) {
    if (!(spec instanceof JsonIngressSpec)) {
      throw new IllegalArgumentException("Wrong type " + spec.type());
    }
    JsonIngressSpec<T> casted = (JsonIngressSpec<T>) spec;

    IngressIdentifier<T> id = casted.id();
    Class<T> producedType = casted.id().producedType();
    if (!Message.class.isAssignableFrom(producedType)) {
      throw new IllegalArgumentException(
          "ProtocolBuffer based ingress is only able to produce types that derive from "
              + Message.class.getName()
              + " but "
              + producedType.getName()
              + " is provided.");
    }

    JsonNode json = casted.json();

    Map<String, RoutingConfig> routableTopics = routableTopics(json);

    PulsarIngressBuilder<T> pulsarIngressBuilder = PulsarIngressBuilder.forIdentifier(id);
    pulsarIngressBuilder
        .withServiceUrl(pulsarServiceUrl(json))
        .withAdminUrl(pulsarAdminUrl(json))
        .withProperties(pulsarClientProperties(json))
        .withTopics(new ArrayList<>(routableTopics.keySet()));

    optionalSubscription(json).ifPresent(pulsarIngressBuilder::withSubscription);
    optionalAutoOffsetResetPosition(json).ifPresent(pulsarIngressBuilder::withAutoResetPosition);
    optionalStartupPosition(json).ifPresent(pulsarIngressBuilder::withStartupPosition);

    PulsarIngressBuilderApiExtension.withDeserializer(
        pulsarIngressBuilder, deserializer(routableTopics));

    return pulsarIngressBuilder.build();
  }

  @SuppressWarnings("unchecked")
  private static <T> PulsarIngressDeserializer<T> deserializer(
      Map<String, RoutingConfig> routingConfig) {
    // this cast is safe since we've already checked that T is a Message
    return (PulsarIngressDeserializer<T>)
        new RoutableProtobufPulsarIngressDeserializer(routingConfig);
  }
}
