package org.apache.flink.statefun.sdk.pulsar;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public class Temp {

  public static void main(String[] args) throws PulsarClientException {

    PulsarClient client =
        PulsarClient.builder()
            .serviceUrl("pulsar://localhost:6650,localhost:6651,localhost:6652")
            .build();

    Producer<byte[]> producer = client.newProducer().topic("my-topic").create();

    producer
        .newMessage()
        .key("my-message-key")
        .value("my-async-message".getBytes())
        .property("my-key", "my-value")
        .property("my-other-key", "my-other-value")
        .send();

    producer.newMessage().value("bla".getBytes()).send();
  }
}
