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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.statefun.flink.io.spi.SourceProvider;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaTopicPartition;
import org.apache.flink.statefun.sdk.pulsar.ingress.PulsarIngressSpec;
import org.apache.flink.statefun.sdk.pulsar.ingress.PulsarIngressStartupPosition;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSource;
import org.apache.flink.streaming.util.serialization.PulsarDeserializationSchema;

public class PulsarSourceProvider implements SourceProvider {

  @Override
  public <T> SourceFunction<T> forSpec(IngressSpec<T> ingressSpec) {
    PulsarIngressSpec<T> spec = asPulsarSpec(ingressSpec);
    Properties propsTest = spec.properties();

    PulsarDeserializationSchema<T> schema = deserializationSchemaFromSpec(spec);

    FlinkPulsarSource<T> source =
        new FlinkPulsarSource<>(spec.serviceUrl(), spec.adminUrl(), schema, propsTest);

    configureStartupPosition(source, spec.startupPosition());
    return source;
  }

  private static <T> PulsarIngressSpec<T> asPulsarSpec(IngressSpec<T> ingressSpec) {
    if (ingressSpec instanceof PulsarIngressSpec) {
      return (PulsarIngressSpec<T>) ingressSpec;
    }
    if (ingressSpec == null) {
      throw new NullPointerException("Unable to translate a NULL spec");
    }
    throw new IllegalArgumentException(String.format("Wrong type %s", ingressSpec.type()));
  }

  private static <T> void configureStartupPosition(
      FlinkPulsarSource<T> source, PulsarIngressStartupPosition startupPosition) {

    // TODO: add missing
    if (startupPosition.isEarliest()) {
      source.setStartFromEarliest();
    } else if (startupPosition.isLatest()) {
      source.setStartFromLatest();
    }

    /*if (startupPosition.isGroupOffsets()) {
      consumer.setStartFromGroupOffsets();
    } else if (startupPosition.isEarliest()) {
      consumer.setStartFromEarliest();
    } else if (startupPosition.isLatest()) {
      consumer.setStartFromLatest();
    } else if (startupPosition.isSpecificOffsets()) {
      KafkaIngressStartupPosition.SpecificOffsetsPosition offsetsPosition =
          startupPosition.asSpecificOffsets();
      consumer.setStartFromSpecificOffsets(
          convertKafkaTopicPartitionMap(offsetsPosition.specificOffsets()));
    } else if (startupPosition.isDate()) {
      KafkaIngressStartupPosition.DatePosition datePosition = startupPosition.asDate();
      consumer.setStartFromTimestamp(datePosition.epochMilli());
    } else {
      throw new IllegalStateException("Safe guard; should not occur");
    }*/
  }

  private <T> PulsarDeserializationSchema<T> deserializationSchemaFromSpec(
      PulsarIngressSpec<T> spec) {
    return new PulsarDeserializationSchemaDelegate<>(spec.deserializer());
  }

  private static Map<
          org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition, Long>
      convertKafkaTopicPartitionMap(Map<KafkaTopicPartition, Long> offsets) {
    Map<org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition, Long> result =
        new HashMap<>(offsets.size());
    for (Map.Entry<KafkaTopicPartition, Long> offset : offsets.entrySet()) {
      result.put(convertKafkaTopicPartition(offset.getKey()), offset.getValue());
    }

    return result;
  }

  private static org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
      convertKafkaTopicPartition(KafkaTopicPartition partition) {
    return new org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition(
        partition.topic(), partition.partition());
  }
}
