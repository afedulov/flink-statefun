package org.apache.flink.statefun.e2e.remote;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public class PulsarTemp {

  public static void main(String[] args)
      throws PulsarClientException, ExecutionException, InterruptedException {
    PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();
    CompletableFuture<List<String>> partitionsForTopic = client
        .getPartitionsForTopic("partitioned-topic");
    List<String> strings = partitionsForTopic.get();
    System.out.println(strings);
  }
}
