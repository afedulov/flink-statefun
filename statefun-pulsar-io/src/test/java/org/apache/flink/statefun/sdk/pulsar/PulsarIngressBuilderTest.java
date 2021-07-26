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
package org.apache.flink.statefun.sdk.pulsar;

import static org.apache.flink.statefun.sdk.pulsar.testutils.Matchers.hasProperty;
import static org.apache.flink.statefun.sdk.pulsar.testutils.Matchers.isMapOfSize;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Properties;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Test;

public class PulsarIngressBuilderTest {

  private static final IngressIdentifier<String> DUMMY_ID =
      new IngressIdentifier<>(String.class, "ns", "name");

  @Test
  public void idIsCorrect() {
    PulsarIngressBuilder<String> builder =
        PulsarIngressBuilder.forIdentifier(DUMMY_ID)
            .withAdminUrl("http://localhost:8080")
            .withServiceUrl("pulsar://localhost:6650")
            .withTopic("topic")
            .withSubscription("test-subscription")
            .withDeserializer(NoOpDeserializer.class);

    PulsarIngressSpec<String> spec = builder.build();

    assertThat(spec.id(), is(DUMMY_ID));
  }

  @Test
  public void ingressTypeIsCorrect() {
    PulsarIngressBuilder<String> builder =
        PulsarIngressBuilder.forIdentifier(DUMMY_ID)
            .withAdminUrl("http://localhost:8080")
            .withServiceUrl("pulsar://localhost:6650")
            .withTopic("topic")
            .withSubscription("test-subscription")
            .withDeserializer(NoOpDeserializer.class);

    PulsarIngressSpec<String> spec = builder.build();

    assertThat(spec.type(), is(Constants.PULSAR_INGRESS_TYPE));
  }

  @Test
  public void topicsIsCorrect() {
    PulsarIngressBuilder<String> builder =
        PulsarIngressBuilder.forIdentifier(DUMMY_ID)
            .withAdminUrl("http://localhost:8080")
            .withServiceUrl("pulsar://localhost:6650")
            .withTopic("topic")
            .withSubscription("test-subscription")
            .withDeserializer(NoOpDeserializer.class);

    PulsarIngressSpec<String> spec = builder.build();

    // TODO: test for patterns and multiple topics setting.
    String topicSetting =
        spec.properties().getProperty(PulsarConsumerConfig.TOPIC_SINGLE_OPTION_KEY);
    assertThat(topicSetting, is("topic"));
  }

  @Test
  public void deserializerIsCorrect() {
    PulsarIngressBuilder<String> builder =
        PulsarIngressBuilder.forIdentifier(DUMMY_ID)
            .withAdminUrl("http://localhost:8080")
            .withServiceUrl("pulsar://localhost:6650")
            .withTopic("topic")
            .withSubscription("test-subscription")
            .withDeserializer(NoOpDeserializer.class);

    PulsarIngressSpec<String> spec = builder.build();

    assertThat(spec.deserializer(), instanceOf(NoOpDeserializer.class));
  }

  @Test
  public void startupPositionIsCorrect() {
    PulsarIngressBuilder<String> builder =
        PulsarIngressBuilder.forIdentifier(DUMMY_ID)
            .withAdminUrl("http://localhost:8080")
            .withServiceUrl("pulsar://localhost:6650")
            .withTopic("topic")
            .withSubscription("test-subscription")
            .withDeserializer(NoOpDeserializer.class);

    PulsarIngressSpec<String> spec = builder.build();

    assertThat(spec.startupPosition(), is(PulsarIngressStartupPosition.fromLatest()));
  }

  @Test
  public void propertiesIsCorrect() {
    PulsarIngressBuilder<String> builder =
        PulsarIngressBuilder.forIdentifier(DUMMY_ID)
            .withAdminUrl("http://localhost:8080")
            .withServiceUrl("pulsar://localhost:6650")
            .withTopic("topic")
            .withSubscription("test-subscription")
            .withDeserializer(NoOpDeserializer.class);

    PulsarIngressSpec<String> spec = builder.build();

    assertThat(
        spec.properties(),
        allOf(
            isMapOfSize(3),
            hasProperty(PulsarConsumerConfig.SERVICE_URL, "pulsar://localhost:6650")));

    // TODO: add required parameters for pulsar
    //   hasProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-group"),
    //   hasProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"))
  }

  @Test
  public void namedMethodConfigValuesOverwriteProperties() {
    Properties properties = new Properties();
    properties.setProperty(PulsarConsumerConfig.SERVICE_URL, "should-be-overwritten");

    PulsarIngressBuilder<String> builder =
        PulsarIngressBuilder.forIdentifier(DUMMY_ID)
            .withAdminUrl("http://localhost:8080")
            .withServiceUrl("pulsar://localhost:6650")
            .withTopic("topic")
            .withSubscription("test-subscription")
            .withDeserializer(NoOpDeserializer.class)
            .withProperties(properties);

    PulsarIngressSpec<String> spec = builder.build();

    assertThat(
        spec.properties(),
        hasProperty(PulsarConsumerConfig.SERVICE_URL, "pulsar://localhost:6650"));
  }

  @Test
  public void defaultNamedMethodConfigValuesShouldNotOverwriteProperties() {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    PulsarIngressBuilder<String> builder =
        PulsarIngressBuilder.forIdentifier(DUMMY_ID)
            .withAdminUrl("http://localhost:8080")
            .withServiceUrl("pulsar://localhost:6650")
            .withTopic("topic")
            .withSubscription("test-subscription")
            .withDeserializer(NoOpDeserializer.class)
            .withProperties(properties);

    PulsarIngressSpec<String> spec = builder.build();

    assertThat(spec.properties(), hasProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
  }

  private static class NoOpDeserializer implements PulsarIngressDeserializer<String> {
    @Override
    public String deserialize(org.apache.pulsar.client.api.Message<String> input) {
      return null;
    }
  }
}
