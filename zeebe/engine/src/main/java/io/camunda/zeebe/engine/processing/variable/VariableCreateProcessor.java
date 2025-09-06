/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */

package io.camunda.zeebe.engine.processing.variable;

import io.camunda.zeebe.engine.processing.bpmn.behavior.BpmnConditionalEventBehavior;
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecordProcessor;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.Writers;
import io.camunda.zeebe.protocol.impl.record.value.variable.VariableRecord;
import io.camunda.zeebe.protocol.record.intent.VariableIntent;
import io.camunda.zeebe.stream.api.records.TypedRecord;
import io.camunda.zeebe.stream.api.state.KeyGenerator;

public final class VariableCreateProcessor implements TypedRecordProcessor<VariableRecord> {

  private final KeyGenerator keyGenerator;
  private final BpmnConditionalEventBehavior conditionalEventBehavior;
  private final Writers writers;

  public VariableCreateProcessor(
      final KeyGenerator keyGenerator,
      final BpmnConditionalEventBehavior conditionalEventBehavior,
      final Writers writers) {
    this.conditionalEventBehavior = conditionalEventBehavior;
    this.keyGenerator = keyGenerator;
    this.writers = writers;
  }

  @Override
  public void processRecord(final TypedRecord<VariableRecord> command) {
    final var value = command.getValue();
    writers.state().appendFollowUpEvent(keyGenerator.nextKey(), VariableIntent.CREATED, value);

    // trigger conditional events
    conditionalEventBehavior.triggerConditionalEvents(
        value.getTenantId(), value.getProcessDefinitionKey(), value.getScopeKey());
  }
}
