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
import org.apache.flink.statefun.flink.common.UnimplementedTypeInfo;
import org.apache.flink.statefun.sdk.pulsar.PulsarIngressDeserializer;
import org.apache.flink.streaming.util.serialization.PulsarDeserializationSchema;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;

final class PulsarDeserializationSchemaDelegate<T> implements PulsarDeserializationSchema<T> {

  private static final long serialVersionUID = 1;

  private final TypeInformation<T> producedTypeInfo = new UnimplementedTypeInfo<>();;
  private final PulsarIngressDeserializer<T> delegate;

  PulsarDeserializationSchemaDelegate(PulsarIngressDeserializer<T> delegate) {
    this.delegate = Objects.requireNonNull(delegate);
  }

  @Override
  public boolean isEndOfStream(T t) {
    return false;
  }

  @Override
  public T deserialize(Message<T> message) {
    return delegate.deserialize(message);
  }

  @Override
  public TypeInformation<T> getProducedType() {
    // this would never be actually used, it would be replaced during translation with the type
    // information
    // of IngressIdentifier's producedType.
    // see: Sources#setOutputType.
    // if this invariant would not hold in the future, this type information would produce a
    // serializer that fails immediately.
    return producedTypeInfo;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Schema<T> getSchema() {
    return (Schema<T>) Schema.BYTES;
  }
}
