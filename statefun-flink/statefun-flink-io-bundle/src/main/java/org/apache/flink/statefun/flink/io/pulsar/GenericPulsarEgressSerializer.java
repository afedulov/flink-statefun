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

import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.charset.StandardCharsets;
import org.apache.flink.statefun.flink.common.types.TypedValueUtil;
import org.apache.flink.statefun.sdk.egress.generated.KafkaProducerRecord;
import org.apache.flink.statefun.sdk.egress.generated.PulsarMessage;
import org.apache.flink.statefun.sdk.pulsar.egress.PulsarEgressSerializer;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.MessageImpl;

/**
 * A {@link PulsarEgressSerializer} used solely by sinks provided by the {@link
 * GenericPulsarSinkProvider}.
 *
 * <p>This serializer expects Protobuf messages of type {@link KafkaProducerRecord}, and simply
 * transforms those into Pulsar's {@link PulsarMessage}.
 */
public final class GenericPulsarEgressSerializer implements PulsarEgressSerializer<TypedValue> {

  private static final long serialVersionUID = 1L;

  @Override
  public void serialize(TypedValue message, TypedMessageBuilder messageBuilder) {
    PulsarMessage pulsarMessage = asPulsarMessage(message);

    messageBuilder.value(pulsarMessage);
  }

  @Override
  public Message<byte[]> serialize(TypedValue message) {
    PulsarMessage protobufMessage = asPulsarMessage(message);


    return toProducerRecord(protobufMessage);
  }

  private static PulsarMessage asPulsarMessage(TypedValue message) {
    if (!TypedValueUtil.isProtobufTypeOf(message, PulsarMessage.getDescriptor())) {
      throw new IllegalStateException(
          "The generic Pulsar egress expects only messages of type "
              + PulsarMessage.class.getName());
    }
    try {
      return PulsarMessage.parseFrom(message.getValue());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(
          "Unable to unpack message as a " + PulsarMessage.class.getName(), e);
    }
  }

  private static Message<byte[]> toProducerRecord(
      PulsarMessage protobufProducerRecord) {
    final String key = protobufProducerRecord.getKey();
    final String topic = protobufProducerRecord.getTopic();
    final byte[] valueBytes = protobufProducerRecord.getValueBytes().toByteArray();

    if (key == null || key.isEmpty()) {
      return new ProducerRecord<>(topic, valueBytes);

    } else {
      return new ProducerRecord<>(topic, key.getBytes(StandardCharsets.UTF_8), valueBytes);
    }
  }

}
