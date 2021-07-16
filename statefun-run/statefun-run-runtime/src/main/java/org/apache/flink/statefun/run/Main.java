package org.apache.flink.statefun.run;

// import com.google.protobuf.Message;
import org.apache.flink.statefun.flink.harness.Harness;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public class Main {

  public static void main(String[] args) throws Exception {
    //    List<Message> inputMessages =
    //        asList(
    //            ofUtf8String("com.knaufk/demo", "foo", "Hello"),
    //            ofUtf8String("com.knaufk/demo", "bar", "World"),
    //            ofUtf8String("com.knaufk/demo", "baz", "!"));

    new Harness()
        //
        // .withSupplyingIngress(com.apache.flink.statefun.e2e.remote.Constants.IN_MEMORY_TEST_INGRESS, new
        //                         Utils.CyclingInMemoryIngress(inputMessages))
        .withPrintingEgress(
            new EgressIdentifier("com.github.knaufk.demo", "kafka-egress", TypedValue.class))
        .withConfiguration("restart-strategy", "failure-rate")
        .withConfiguration("restart-strategy.failure-rate.max-failures-per-interval", "1000")
        .withConfiguration("restart-strategy.failure-rate.failure-rate-interval", "5 min")
        .withConfiguration("restart-strategy.failure-rate.delay", "10 s")
        .start();
  }
}
