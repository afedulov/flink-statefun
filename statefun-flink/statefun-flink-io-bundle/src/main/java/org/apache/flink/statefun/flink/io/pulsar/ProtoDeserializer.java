package org.apache.flink.statefun.flink.io.pulsar;

import com.google.protobuf.Message;
import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class ProtoDeserializer implements DeserializationSchema<Message> {
  @Override
  public TypeInformation<Message> getProducedType() {
    return TypeInformation.of(Message.class);
  }

  @Override
  public Message deserialize(byte[] input) throws IOException {
    return null;
    //        try {
    //            return parser().parseFrom(input.getData());
    //        } catch (InvalidProtocolBufferException e) {
    //            throw new IllegalStateException(e);
    //        }
  }

  @Override
  public boolean isEndOfStream(Message nextElement) {
    return false;
  }
}
