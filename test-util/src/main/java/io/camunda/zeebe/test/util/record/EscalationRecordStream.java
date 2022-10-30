/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.test.util.record;

import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.value.EscalationRecordValue;
import java.util.Objects;
import java.util.stream.Stream;

public final class EscalationRecordStream
    extends ExporterRecordStream<EscalationRecordValue, EscalationRecordStream> {

  public EscalationRecordStream(final Stream<Record<EscalationRecordValue>> wrappedStream) {
    super(wrappedStream);
  }

  @Override
  protected EscalationRecordStream supply(
      final Stream<Record<EscalationRecordValue>> wrappedStream) {
    return new EscalationRecordStream(wrappedStream);
  }

  public EscalationRecordStream withCatchElementId(final String elementId) {
    return valueFilter(v -> Objects.equals(v.getCatchElementId(), elementId));
  }

  public EscalationRecordStream withThrowElementId(final String elementId) {
    return valueFilter(v -> Objects.equals(v.getThrowElementId(), elementId));
  }

  public EscalationRecordStream withEscalationCode(final String escalationCode) {
    return valueFilter(v -> Objects.equals(v.getEscalationCode(), escalationCode));
  }

  public EscalationRecordStream withProcessInstanceKey(final long processInstanceKey) {
    return valueFilter(v -> v.getProcessInstanceKey() == processInstanceKey);
  }
}
