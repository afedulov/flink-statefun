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

import java.util.Optional;
import java.util.Properties;
import org.apache.flink.statefun.flink.io.common.ReflectionUtil;
import org.apache.flink.statefun.flink.io.spi.SinkProvider;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.pulsar.egress.PulsarEgressSerializer;
import org.apache.flink.statefun.sdk.pulsar.egress.PulsarEgressSpec;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSink;
import org.apache.flink.streaming.connectors.pulsar.internal.JsonSer;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarClientUtils;
import org.apache.flink.streaming.connectors.pulsar.table.PulsarSinkSemantic;
import org.apache.flink.streaming.util.serialization.PulsarSerializationSchema;
import org.apache.flink.streaming.util.serialization.PulsarSerializationSchemaWrapper;

public class PulsarSinkProvider implements SinkProvider {

  @Override
  public <T> SinkFunction<T> forSpec(EgressSpec<T> egressSpec) {
    PulsarEgressSpec<T> spec = asPulsarSpec(egressSpec);

    Properties properties = new Properties();
    properties.putAll(spec.properties());
    /*    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, spec.kafkaAddress());

    Semantic producerSemantic = semanticFromSpec(spec);

    if (producerSemantic == Semantic.EXACTLY_ONCE) {
      properties.setProperty(
          ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
          String.valueOf(spec.transactionTimeoutDuration().toMillis()));
    }*/

    PulsarSerializationSchema<byte[]> pulsarSerialization = new PulsarSerializationSchemaWrapper.Builder<>(
        JsonSer.of(Person.class))
    .usePojoMode(Person. class, RecordSchemaType.JSON)
    .setTopicExtractor(person -> null)
    .build();

    PulsarSerializationSchema<T> schema = serializationSchemaFromSpec(spec);

    return new FlinkPulsarSink<>(
        spec.adminUrl(),
        Optional.empty(),
        PulsarClientUtils.newClientConf(spec.serviceUrl(), properties),
        properties,
        schema,
        semanticFromSpec(spec));
  }

  private <T> PulsarSerializationSchema<T> serializationSchemaFromSpec(PulsarEgressSpec<T> spec) {
    PulsarEgressSerializer<T> serializer = ReflectionUtil.instantiate(spec.serializerClass());
    return new PulsarSerializationSchemaDelegate<>(serializer);
  }

  private static <T> PulsarSinkSemantic semanticFromSpec(PulsarEgressSpec<T> spec) {
    switch (spec.semantic()) {
      case EXACTLY_ONCE:
        return PulsarSinkSemantic.EXACTLY_ONCE;
      case AT_LEAST_ONCE:
        return PulsarSinkSemantic.AT_LEAST_ONCE;
      case NONE:
        return PulsarSinkSemantic.NONE;
      default:
        throw new IllegalArgumentException("Unknown producer semantic " + spec.semantic());
    }
  }

  private static <T> PulsarEgressSpec<T> asPulsarSpec(EgressSpec<T> spec) {
    if (spec instanceof PulsarEgressSpec) {
      return (PulsarEgressSpec<T>) spec;
    }
    if (spec == null) {
      throw new NullPointerException("Unable to translate a NULL spec");
    }
    throw new IllegalArgumentException(String.format("Wrong type %s", spec.type()));
  }
}
