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

import static org.apache.flink.statefun.flink.io.pulsar.PulsarEgressSpecJsonParser.exactlyOnceDeliveryTxnTimeout;
import static org.apache.flink.statefun.flink.io.pulsar.PulsarEgressSpecJsonParser.kafkaClientProperties;
import static org.apache.flink.statefun.flink.io.pulsar.PulsarEgressSpecJsonParser.optionalDeliverySemantic;
import static org.apache.flink.statefun.flink.io.pulsar.PulsarEgressSpecJsonParser.pulsarAdminUrl;
import static org.apache.flink.statefun.flink.io.pulsar.PulsarEgressSpecJsonParser.pulsarServiceUrl;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.io.spi.JsonEgressSpec;
import org.apache.flink.statefun.flink.io.spi.SinkProvider;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.pulsar.egress.PulsarEgressBuilder;
import org.apache.flink.statefun.sdk.pulsar.egress.PulsarEgressSpec;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

final class GenericPulsarSinkProvider implements SinkProvider {

  private final PulsarSinkProvider delegateProvider = new PulsarSinkProvider();

  @Override
  public <T> SinkFunction<T> forSpec(EgressSpec<T> spec) {
    PulsarEgressSpec<T> kafkaEgressSpec = asPulsarEgressSpec(spec);
    return delegateProvider.forSpec(kafkaEgressSpec);
  }

  private static <T> PulsarEgressSpec<T> asPulsarEgressSpec(EgressSpec<T> spec) {
    if (!(spec instanceof JsonEgressSpec)) {
      throw new IllegalArgumentException("Wrong type " + spec.type());
    }
    JsonEgressSpec<T> casted = (JsonEgressSpec<T>) spec;

    EgressIdentifier<T> id = casted.id();
    validateConsumedType(id);

    JsonNode json = casted.json();

    PulsarEgressBuilder<T> pulsarEgressBuilder = PulsarEgressBuilder.forIdentifier(id);

    pulsarEgressBuilder
        .withServiceUrl(pulsarServiceUrl(json))
        .withAdminUrl(pulsarAdminUrl(json))
        .withProperties(kafkaClientProperties(json))
        .withSerializer(serializerClass());

    optionalDeliverySemantic(json)
        .ifPresent(
            semantic -> {
              switch (semantic) {
                case AT_LEAST_ONCE:
                  pulsarEgressBuilder.withAtLeastOnceProducerSemantics();
                  break;
                case EXACTLY_ONCE:
                  pulsarEgressBuilder.withExactlyOnceProducerSemantics(
                      exactlyOnceDeliveryTxnTimeout(json));
                  break;
                case NONE:
                  pulsarEgressBuilder.withNoProducerSemantics();
                  break;
                default:
                  throw new IllegalStateException("Unrecognized producer semantic: " + semantic);
              }
            });

    return pulsarEgressBuilder.build();
  }

  private static void validateConsumedType(EgressIdentifier<?> id) {
    Class<?> consumedType = id.consumedType();
    if (TypedValue.class != consumedType) {
      throw new IllegalArgumentException(
          "Generic Pulsar egress is only able to consume messages types of "
              + TypedValue.class.getName()
              + " but "
              + consumedType.getName()
              + " is provided.");
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> Class<T> serializerClass() {
    // this cast is safe, because we've already validated that the consumed type is Any.
    return (Class<T>) GenericPulsarEgressSerializer.class;
  }
}
