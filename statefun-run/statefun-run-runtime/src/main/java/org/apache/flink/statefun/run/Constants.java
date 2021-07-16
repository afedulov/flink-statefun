package org.apache.flink.statefun.run;

import com.google.protobuf.Message;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public class Constants {

  public static final IngressIdentifier<Message> IN_MEMORY_TEST_INGRESS =
      new IngressIdentifier<>(Message.class, "com.github.knaufk.demo", "kafka-ingress");
  public static final EgressIdentifier<TypedValue> IN_MEMORY_TEST_EGRESS =
      new EgressIdentifier("com.github.knaufk.demo", "kafka-egress", TypedValue.class);
}
