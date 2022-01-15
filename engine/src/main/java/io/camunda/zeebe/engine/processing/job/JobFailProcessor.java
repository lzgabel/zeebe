/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.job;

import static io.camunda.zeebe.util.buffer.BufferUtil.wrapString;

import io.camunda.zeebe.engine.metrics.JobMetrics;
import io.camunda.zeebe.engine.processing.streamprocessor.CommandProcessor;
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecord;
import io.camunda.zeebe.engine.processing.streamprocessor.sideeffect.SideEffectProducer;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.StateWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedCommandWriter;
import io.camunda.zeebe.engine.state.KeyGenerator;
import io.camunda.zeebe.engine.state.immutable.JobState;
import io.camunda.zeebe.engine.state.immutable.ZeebeState;
import io.camunda.zeebe.protocol.impl.record.value.incident.IncidentRecord;
import io.camunda.zeebe.protocol.impl.record.value.job.JobRecord;
import io.camunda.zeebe.protocol.record.intent.IncidentIntent;
import io.camunda.zeebe.protocol.record.intent.Intent;
import io.camunda.zeebe.protocol.record.intent.JobIntent;
import io.camunda.zeebe.protocol.record.value.ErrorType;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;

public final class JobFailProcessor implements CommandProcessor<JobRecord> {

  private static final DirectBuffer DEFAULT_ERROR_MESSAGE = wrapString("No more retries left.");
  private final IncidentRecord incidentEvent = new IncidentRecord();

  private final JobState jobState;
  private final DefaultJobCommandPreconditionGuard defaultProcessor;
  private final KeyGenerator keyGenerator;
  private final JobMetrics jobMetrics;
  private final JobBackoffChecker jobBackoffChecker;

  public JobFailProcessor(
      final ZeebeState state,
      final KeyGenerator keyGenerator,
      final JobMetrics jobMetrics,
      final JobBackoffChecker jobBackoffChecker) {
    jobState = state.getJobState();
    this.keyGenerator = keyGenerator;
    this.jobBackoffChecker = jobBackoffChecker;
    defaultProcessor =
        new DefaultJobCommandPreconditionGuard("fail", jobState, this::acceptCommand);
    this.jobMetrics = jobMetrics;
  }

  @Override
  public boolean onCommand(
      final TypedRecord<JobRecord> command,
      final CommandControl<JobRecord> commandControl,
      final Consumer<SideEffectProducer> sideEffect) {
    return defaultProcessor.onCommand(command, commandControl, sideEffect);
  }

  @Override
  public void afterAccept(
      final TypedCommandWriter commandWriter,
      final StateWriter stateWriter,
      final long key,
      final Intent intent,
      final JobRecord value) {
    if (value.getRetries() <= 0) {
      final DirectBuffer jobErrorMessage = value.getErrorMessageBuffer();
      DirectBuffer incidentErrorMessage = DEFAULT_ERROR_MESSAGE;
      if (jobErrorMessage.capacity() > 0) {
        incidentErrorMessage = jobErrorMessage;
      }

      incidentEvent.reset();
      incidentEvent
          .setErrorType(ErrorType.JOB_NO_RETRIES)
          .setErrorMessage(incidentErrorMessage)
          .setBpmnProcessId(value.getBpmnProcessIdBuffer())
          .setProcessDefinitionKey(value.getProcessDefinitionKey())
          .setProcessInstanceKey(value.getProcessInstanceKey())
          .setElementId(value.getElementIdBuffer())
          .setElementInstanceKey(value.getElementInstanceKey())
          .setJobKey(key)
          .setVariableScopeKey(value.getElementInstanceKey());

      stateWriter.appendFollowUpEvent(
          keyGenerator.nextKey(), IncidentIntent.CREATED, incidentEvent);
    }
  }

  private void acceptCommand(
      final TypedRecord<JobRecord> command,
      final CommandControl<JobRecord> commandControl,
      final Consumer<SideEffectProducer> sideEffect) {
    final long key = command.getKey();
    final JobRecord failedJob = jobState.getJob(key);
    final var retries = command.getValue().getRetries();
    final var retryBackOff = command.getValue().getRetryBackoff();
    failedJob.setRetries(retries);
    failedJob.setErrorMessage(command.getValue().getErrorMessageBuffer());
    failedJob.setRetryBackoff(retryBackOff);
    if (retries > 0 && retryBackOff > 0) {
      final long receivedTime = command.getTimestamp();
      failedJob.setRecurringTime(receivedTime + retryBackOff);
      sideEffect.accept(
          () -> {
            jobBackoffChecker.scheduleBackOff(retryBackOff + receivedTime);
            return true;
          });
    }
    commandControl.accept(JobIntent.FAILED, failedJob);
    jobMetrics.jobFailed(failedJob.getType());
  }
}
