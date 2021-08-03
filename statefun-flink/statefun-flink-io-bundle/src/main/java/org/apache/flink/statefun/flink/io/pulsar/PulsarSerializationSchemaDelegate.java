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

import java.util.Objects;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.statefun.sdk.pulsar.egress.PulsarEgressSerializer;
import org.apache.flink.streaming.util.serialization.PulsarSerializationSchema;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;

final class PulsarSerializationSchemaDelegate<T> implements PulsarSerializationSchema<T> {

  private static final long serialVersionUID = 1L;

  private final PulsarEgressSerializer<T> serializer;

  PulsarSerializationSchemaDelegate(PulsarEgressSerializer<T> serializer) {
    this.serializer = Objects.requireNonNull(serializer);
  }

  @Override
  public void serialize(T t, TypedMessageBuilder<T> messageBuilder) {
    serializer.serialize(t);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Schema<T> getSchema() {
    return (Schema<T>) Schema.BYTES;
  }

  @Override
  public TypeInformation<T> getProducedType() {
    return null;
  }
}
