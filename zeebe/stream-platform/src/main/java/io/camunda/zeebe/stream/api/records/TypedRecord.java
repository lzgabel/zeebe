/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.stream.api.records;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.camunda.zeebe.protocol.Protocol;
import io.camunda.zeebe.protocol.impl.record.UnifiedRecordValue;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.RecordMetadataEncoder;
import io.camunda.zeebe.protocol.record.RecordType;

public interface TypedRecord<T extends UnifiedRecordValue> extends Record<T> {

  @Override
  long getKey();

  @Override
  T getValue();

  int getRequestStreamId();

  long getRequestId();

  int getLength();

  default boolean hasRequestMetadata() {
    return getRequestId() != RecordMetadataEncoder.requestIdNullValue()
        && getRequestStreamId() != RecordMetadataEncoder.requestStreamIdNullValue();
  }

  /**
   * Returns whether the record was distributed by a different partition. It can do so by decoding
   * the partition from the key and comparing it against the partition id of this record. If these
   * match the key was generated by the current partition. If not, the key was generated by a
   * different partition and then distributed to the current partition. Some commands will not have
   * a key, indicated by -1. This cannot occur when the command is distributed.
   *
   * @return a boolean indicating if the key was generated on the current partition
   */
  @JsonIgnore
  default boolean isCommandDistributed() {
    final boolean isCommand = getRecordType().equals(RecordType.COMMAND);
    final long key = getKey();
    return isCommand && key != -1 && Protocol.decodePartitionId(key) != getPartitionId();
  }
}