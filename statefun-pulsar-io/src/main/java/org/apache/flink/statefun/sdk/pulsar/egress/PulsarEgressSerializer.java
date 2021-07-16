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

import java.io.Serializable;
import org.apache.pulsar.client.api.Message;

/**
 * A {@link PulsarEgressSerializer} defines how to serialize values of type {@code T} into {@link
 * Message ProducerRecords}.
 *
 * @param <OutT> the type of values being serialized
 */
public interface PulsarEgressSerializer<OutT> extends Serializable {

  /**
   * Serializes given element and returns it as a {@link Message}.
   *
   * @param t element to be serialized
   * @return Pulsar {@link Message}
   */
  Message<byte[]> serialize(OutT t);
}
