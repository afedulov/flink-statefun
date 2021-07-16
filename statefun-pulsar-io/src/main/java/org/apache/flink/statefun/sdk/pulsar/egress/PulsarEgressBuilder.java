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
package org.apache.flink.statefun.sdk.pulsar.egress;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.pulsar.PulsarProducerSemantic;

/**
 * A builder class for creating an {@link EgressSpec} that writes data out to a Pulsar cluster. By
 * default the egress will use {@link #withAtLeastOnceProducerSemantics()}.
 *
 * @param <OutT> The type written out to the cluster by the Egress.
 */
public final class PulsarEgressBuilder<OutT> {
  private final EgressIdentifier<OutT> id;
  private Class<? extends PulsarEgressSerializer<OutT>> serializer;
  private String pulsarAddress;
  private Properties properties = new Properties();
  private int pulsarProducerPoolSize = 5;
  private PulsarProducerSemantic semantic = PulsarProducerSemantic.AT_LEAST_ONCE;
  private Duration transactionTimeoutDuration = Duration.ZERO;

  private PulsarEgressBuilder(EgressIdentifier<OutT> id) {
    this.id = Objects.requireNonNull(id);
  }

  /**
   * @param egressIdentifier A unique egress identifier.
   * @param <OutT> The type the egress will output.
   * @return A {@link PulsarEgressBuilder}.
   */
  public static <OutT> PulsarEgressBuilder<OutT> forIdentifier(
      EgressIdentifier<OutT> egressIdentifier) {
    return new PulsarEgressBuilder<>(egressIdentifier);
  }

  /** @param pulsarAddress Comma separated addresses of the brokers. */
  public PulsarEgressBuilder<OutT> withPulsarAddress(String pulsarAddress) {
    this.pulsarAddress = Objects.requireNonNull(pulsarAddress);
    return this;
  }

  /** A configuration property for the PulsarProducer. */
  public PulsarEgressBuilder<OutT> withProperty(String key, String value) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(value);
    properties.setProperty(key, value);
    return this;
  }

  /** Configuration properties for the PulsarProducer. */
  public PulsarEgressBuilder<OutT> withProperties(Properties properties) {
    Objects.requireNonNull(properties);
    this.properties.putAll(properties);
    return this;
  }

  /**
   * @param serializer A serializer schema for turning user objects into a pulsar-consumable byte[]
   *     supporting key/value messages.
   */
  public PulsarEgressBuilder<OutT> withSerializer(
      Class<? extends PulsarEgressSerializer<OutT>> serializer) {
    this.serializer = Objects.requireNonNull(serializer);
    return this;
  }

  /** @param poolSize Overwrite default PulsarProducers pool size. The default is 5. */
  public PulsarEgressBuilder<OutT> withPulsarProducerPoolSize(int poolSize) {
    this.pulsarProducerPoolSize = poolSize;
    return this;
  }

  /**
   * PulsarProducerSemantic.EXACTLY_ONCE the egress will write all messages in a Pulsar transaction
   * that will be committed to Pulsar on a checkpoint.
   *
   * <p>With exactly-once producer semantics, users must also specify the transaction timeout. Note
   * that this value must not be larger than the {@code transaction.max.timeout.ms} value configured
   * on Pulsar brokers (by default, this is 15 minutes).
   *
   * @param transactionTimeoutDuration the transaction timeout.
   */
  public PulsarEgressBuilder<OutT> withExactlyOnceProducerSemantics(
      Duration transactionTimeoutDuration) {
    Objects.requireNonNull(
        transactionTimeoutDuration, "a transaction timeout duration must be provided.");
    if (transactionTimeoutDuration == Duration.ZERO) {
      throw new IllegalArgumentException(
          "Transaction timeout durations must be larger than 0 when using exactly-once producer semantics.");
    }

    this.semantic = PulsarProducerSemantic.EXACTLY_ONCE;
    this.transactionTimeoutDuration = transactionTimeoutDuration;
    return this;
  }

  /**
   * PulsarProducerSemantic.AT_LEAST_ONCE the egress will wait for all outstanding messages in the
   * Pulsar buffers to be acknowledged by the Pulsar producer on a checkpoint.
   */
  public PulsarEgressBuilder<OutT> withAtLeastOnceProducerSemantics() {
    this.semantic = PulsarProducerSemantic.AT_LEAST_ONCE;
    return this;
  }

  /**
   * PulsarProducerSemantic.NONE means that nothing will be guaranteed. Messages can be lost and/or
   * duplicated in case of failure.
   */
  public PulsarEgressBuilder<OutT> withNoProducerSemantics() {
    this.semantic = PulsarProducerSemantic.NONE;
    return this;
  }

  /** @return An {@link EgressSpec} that can be used in a {@code StatefulFunctionModule}. */
  public PulsarEgressSpec<OutT> build() {
    return new PulsarEgressSpec<>(
        id,
        serializer,
        pulsarAddress,
        properties,
        pulsarProducerPoolSize,
        semantic,
        transactionTimeoutDuration);
  }
}
