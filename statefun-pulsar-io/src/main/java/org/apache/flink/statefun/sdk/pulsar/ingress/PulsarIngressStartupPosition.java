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

import java.time.ZonedDateTime;
import java.util.Map;
import org.apache.flink.statefun.sdk.pulsar.PulsarTopicPartition;

/** Position for the ingress to start consuming Pulsar partitions. */
@SuppressWarnings("WeakerAccess, unused")
public class PulsarIngressStartupPosition {

  /** Private constructor to prevent instantiation. */
  private PulsarIngressStartupPosition() {}

  /**
   * Start consuming from committed subscription offsets in Pulsar.
   *
   * <p>Note that a subscription name must be provided for this startup mode. Please see {@link
   * PulsarIngressBuilder#withSubscription(String)} (String)}. If a specified offset does not exist
   * for a partition, the position for that partition will * fallback to //TODO check default!
   */
  public static PulsarIngressStartupPosition fromExternalSubscription() {
    return ExternalSubscriptionPosition.INSTANCE;
  }

  /** Start consuming from the earliest offset possible. */
  public static PulsarIngressStartupPosition fromEarliest() {
    return EarliestPosition.INSTANCE;
  }

  /** Start consuming from the latest offset, i.e. head of the topic partitions. */
  public static PulsarIngressStartupPosition fromLatest() {
    return LatestPosition.INSTANCE;
  }

  /**
   * Start consuming from a specified set of offsets.
   *
   * <p>If a specified offset does not exist for a partition, the position for that partition will
   * fallback to //TODO check default!
   *
   * @param specificOffsets map of specific set of offsets.
   */
  public static PulsarIngressStartupPosition fromSpecificOffsets(
      Map<PulsarTopicPartition, Long> specificOffsets) {
    if (specificOffsets == null || specificOffsets.isEmpty()) {
      throw new IllegalArgumentException("Provided specific offsets must not be empty.");
    }
    return new SpecificOffsetsPosition(specificOffsets);
  }

  /** Checks whether this position is configured using committed subscription offsets in Pulsar. */
  public boolean isExternalSubscription() {
    return getClass() == ExternalSubscriptionPosition.class;
  }

  /** Checks whether this position is configured using the earliest offset. */
  public boolean isEarliest() {
    return getClass() == EarliestPosition.class;
  }

  /** Checks whether this position is configured using the latest offset. */
  public boolean isLatest() {
    return getClass() == LatestPosition.class;
  }

  /** Checks whether this position is configured using specific offsets. */
  public boolean isSpecificOffsets() {
    return getClass() == SpecificOffsetsPosition.class;
  }

  /** Returns this position as a {@link SpecificOffsetsPosition}. */
  public SpecificOffsetsPosition asSpecificOffsets() {
    if (!isSpecificOffsets()) {
      throw new IllegalStateException(
          "This is not a startup position configured using specific offsets.");
    }
    return (SpecificOffsetsPosition) this;
  }

  public static final class ExternalSubscriptionPosition extends PulsarIngressStartupPosition {

    private static final ExternalSubscriptionPosition INSTANCE = new ExternalSubscriptionPosition();

    private ExternalSubscriptionPosition() {}

    @Override
    public boolean equals(Object obj) {
      return obj != null && obj instanceof ExternalSubscriptionPosition;
    }

    @Override
    public int hashCode() {
      return getClass().hashCode();
    }
  }

  public static final class EarliestPosition extends PulsarIngressStartupPosition {

    private static final EarliestPosition INSTANCE = new EarliestPosition();

    private EarliestPosition() {}

    @Override
    public boolean equals(Object obj) {
      return obj != null && obj instanceof EarliestPosition;
    }

    @Override
    public int hashCode() {
      return getClass().hashCode();
    }
  }

  public static final class LatestPosition extends PulsarIngressStartupPosition {

    private static final LatestPosition INSTANCE = new LatestPosition();

    private LatestPosition() {}

    @Override
    public boolean equals(Object obj) {
      return obj != null && obj instanceof LatestPosition;
    }

    @Override
    public int hashCode() {
      return getClass().hashCode();
    }
  }

  public static final class SpecificOffsetsPosition extends PulsarIngressStartupPosition {

    private final Map<PulsarTopicPartition, Long> specificOffsets;

    private SpecificOffsetsPosition(Map<PulsarTopicPartition, Long> specificOffsets) {
      this.specificOffsets = specificOffsets;
    }

    public Map<PulsarTopicPartition, Long> specificOffsets() {
      return specificOffsets;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof SpecificOffsetsPosition)) {
        return false;
      }

      SpecificOffsetsPosition that = (SpecificOffsetsPosition) obj;
      return that.specificOffsets.equals(specificOffsets);
    }

    @Override
    public int hashCode() {
      return specificOffsets.hashCode();
    }
  }

  public static final class DatePosition extends PulsarIngressStartupPosition {

    private final ZonedDateTime date;

    private DatePosition(ZonedDateTime date) {
      this.date = date;
    }

    public long epochMilli() {
      return date.toInstant().toEpochMilli();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof DatePosition)) {
        return false;
      }

      DatePosition that = (DatePosition) obj;
      return that.date.equals(date);
    }

    @Override
    public int hashCode() {
      return date.hashCode();
    }
  }
}
