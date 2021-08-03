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
package org.apache.flink.statefun.sdk.pulsar.ingress;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import org.apache.flink.statefun.sdk.annotations.ForRuntime;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.pulsar.OptionalConfig;
import org.apache.flink.statefun.sdk.pulsar.PulsarConfig;

/**
 * A builder for creating an {@link IngressSpec} for consuming data from Apache Pulsar.
 *
 * @param <T> The type consumed from Pulsar.
 */
public final class PulsarIngressBuilder<T> {

  private final IngressIdentifier<T> id;
  private final Properties properties = new Properties();

  private final OptionalConfig<String> serviceUrl =
      OptionalConfig.withoutDefault(PulsarConfig.SERVICE_URL);
  private final OptionalConfig<String> adminUrl =
      OptionalConfig.withoutDefault(PulsarConfig.ADMIN_URL);
  private final OptionalConfig<String> subscription =
      OptionalConfig.withoutDefault(PulsarConfig.SUBSCRIPTION_NAME);
  private final OptionalConfig<PulsarIngressStartupPosition> startupPosition =
      OptionalConfig.withDefault(PulsarIngressStartupPosition.fromLatest());

  // TODO: not directly available in FlinkPulsarSource. Check what happens.
  //  private final OptionalConfig<PulsarIngressAutoResetPosition> autoResetPosition =
  //      OptionalConfig.withDefault(PulsarIngressAutoResetPosition.LATEST);

  private OptionalConfig<PulsarIngressDeserializer<T>> deserializer =
      OptionalConfig.withoutDefault();

  private PulsarIngressBuilder(IngressIdentifier<T> id) {
    this.id = Objects.requireNonNull(id);
  }

  /**
   * @param id A unique ingress identifier.
   * @param <T> The type consumed from Pulsar.
   * @return A new {@link PulsarIngressBuilder}.
   */
  public static <T> PulsarIngressBuilder<T> forIdentifier(IngressIdentifier<T> id) {
    return new PulsarIngressBuilder<>(id);
  }

  /** @param consumerGroupId the consumer group id to use. */
  public PulsarIngressBuilder<T> withSubscription(String subscription) {
    this.subscription.set(subscription);
    return this;
  }

  /** @param serviceUrl The service URL of the Pulsar cluster. */
  public PulsarIngressBuilder<T> withServiceUrl(String serviceUrl) {
    this.serviceUrl.set(serviceUrl);
    return this;
  }

  /** @param adminUrl The admin URL of the Pulsar cluster. */
  public PulsarIngressBuilder<T> withAdminUrl(String adminUrl) {
    this.adminUrl.set(adminUrl);
    return this;
  }

  /** @param topic The name of the topic that should be consumed. */
  public PulsarIngressBuilder<T> withTopic(String topic) {
    return withProperty(PulsarConfig.TOPIC_SINGLE_OPTION_KEY, topic);
  }

  /** @param topicsList A list of topics that should be consumed. */
  public PulsarIngressBuilder<T> withTopics(List<String> topicsList) {
    String topics = String.join(",", topicsList);
    return withProperty(PulsarConfig.TOPIC_MULTI_OPTION_KEY, topics);
  }

  /** A configuration property for the PulsarConsumer. */
  public PulsarIngressBuilder<T> withProperties(Properties properties) {
    this.properties.putAll(properties);
    return this;
  }

  /** A configuration property for the PulsarProducer. */
  public PulsarIngressBuilder<T> withProperty(String name, String value) {
    Objects.requireNonNull(name);
    Objects.requireNonNull(value);
    this.properties.setProperty(name, value);
    return this;
  }

  /**
   * @param deserializerClass The deserializer used to convert between Pulsar's byte messages and
   *     java objects.
   */
  public PulsarIngressBuilder<T> withDeserializer(
      Class<? extends PulsarIngressDeserializer<T>> deserializerClass) {
    Objects.requireNonNull(deserializerClass);
    this.deserializer.set(instantiateDeserializer(deserializerClass));
    return this;
  }

  /**
   * Configures the position that the ingress should start consuming from. By default, the startup
   * position is {@link PulsarIngressStartupPosition#fromLatest()}.
   *
   * <p>Note that this configuration only affects the position when starting the application from a
   * fresh start. When restoring the application from a savepoint, the ingress will always start
   * consuming from the offsets persisted in the savepoint.
   *
   * @param startupPosition the position that the Pulsar ingress should start consuming from.
   * @see PulsarIngressStartupPosition
   */
  public PulsarIngressBuilder<T> withStartupPosition(PulsarIngressStartupPosition startupPosition) {
    this.startupPosition.set(startupPosition);
    return this;
  }

  /** @return A new {@link PulsarIngressSpec}. */
  public PulsarIngressSpec<T> build() {
    Properties properties = resolvePulsarProperties();
    return new PulsarIngressSpec<>(
        id,
        properties,
        serviceUrl.get(),
        adminUrl.get(),
        deserializer.get(),
        startupPosition.get());
  }

  private Properties resolvePulsarProperties() {
    Properties resultProps = new Properties();
    resultProps.putAll(properties);

    // for all String parameters passed using named methods, overwrite corresponding properties
    serviceUrl.overwritePropertiesIfPresent(resultProps, PulsarConfig.SERVICE_URL);
    adminUrl.overwritePropertiesIfPresent(resultProps, PulsarConfig.ADMIN_URL);
    subscription.overwritePropertiesIfPresent(resultProps, PulsarConfig.SUBSCRIPTION_NAME);

    //    TODO: which options need to be considered here?
    //     .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
    //    autoResetPosition.overwritePropertiesIfPresent(
    //        resultProps, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);

    return resultProps;
  }

  private static <T extends PulsarIngressDeserializer<?>> T instantiateDeserializer(
      Class<T> deserializerClass) {
    try {
      Constructor<T> defaultConstructor = deserializerClass.getDeclaredConstructor();
      defaultConstructor.setAccessible(true);
      return defaultConstructor.newInstance();
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(
          "Unable to create an instance of deserializer "
              + deserializerClass.getName()
              + "; has no default constructor",
          e);
    } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
      throw new IllegalStateException(
          "Unable to create an instance of deserializer " + deserializerClass.getName(), e);
    }
  }

  // ========================================================================================
  //  Methods for runtime usage
  // ========================================================================================

  @ForRuntime
  PulsarIngressBuilder<T> withDeserializer(PulsarIngressDeserializer<T> deserializer) {
    this.deserializer.set(deserializer);
    return this;
  }
}
