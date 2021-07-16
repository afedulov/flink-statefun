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
import org.apache.flink.statefun.sdk.EgressType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.pulsar.Constants;
import org.apache.flink.statefun.sdk.pulsar.PulsarProducerSemantic;

public final class PulsarEgressSpec<OutT> implements EgressSpec<OutT> {
  private final Class<? extends PulsarEgressSerializer<OutT>> serializer;
  private final String pulsarAddress;
  private final Properties properties;
  private final EgressIdentifier<OutT> id;
  private final int pulsarProducerPoolSize;
  private final PulsarProducerSemantic semantic;
  private final Duration transactionTimeoutDuration;

  PulsarEgressSpec(
      EgressIdentifier<OutT> id,
      Class<? extends PulsarEgressSerializer<OutT>> serializer,
      String pulsarAddress,
      Properties properties,
      int pulsarProducerPoolSize,
      PulsarProducerSemantic semantic,
      Duration transactionTimeoutDuration) {
    this.serializer = Objects.requireNonNull(serializer);
    this.pulsarAddress = Objects.requireNonNull(pulsarAddress);
    this.properties = Objects.requireNonNull(properties);
    this.id = Objects.requireNonNull(id);
    this.pulsarProducerPoolSize = pulsarProducerPoolSize;
    this.semantic = Objects.requireNonNull(semantic);
    this.transactionTimeoutDuration = Objects.requireNonNull(transactionTimeoutDuration);
  }

  @Override
  public EgressIdentifier<OutT> id() {
    return id;
  }

  @Override
  public EgressType type() {
    return Constants.PULSAR_EGRESS_TYPE;
  }

  public Class<? extends PulsarEgressSerializer<OutT>> serializerClass() {
    return serializer;
  }

  public String pulsarAddress() {
    return pulsarAddress;
  }

  public Properties properties() {
    return properties;
  }

  public int pulsarProducerPoolSize() {
    return pulsarProducerPoolSize;
  }

  public PulsarProducerSemantic semantic() {
    return semantic;
  }

  public Duration transactionTimeoutDuration() {
    return transactionTimeoutDuration;
  }
}
