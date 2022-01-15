/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.job;

import io.camunda.zeebe.engine.metrics.JobMetrics;
import io.camunda.zeebe.engine.processing.bpmn.BpmnElementContext;
import io.camunda.zeebe.engine.processing.bpmn.behavior.BpmnStateBehavior;
import io.camunda.zeebe.engine.processing.common.EventHandle;
import io.camunda.zeebe.engine.processing.streamprocessor.CommandProcessor;
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecord;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.StateWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedCommandWriter;
import io.camunda.zeebe.engine.state.instance.ElementInstance;
import io.camunda.zeebe.engine.state.mutable.MutableElementInstanceState;
import io.camunda.zeebe.engine.state.mutable.MutableEventScopeInstanceState;
import io.camunda.zeebe.engine.state.mutable.MutableJobState;
import io.camunda.zeebe.engine.state.mutable.MutableZeebeState;
import io.camunda.zeebe.protocol.impl.record.value.job.JobRecord;
import io.camunda.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceRecord;
import io.camunda.zeebe.protocol.record.intent.Intent;
import io.camunda.zeebe.protocol.record.intent.JobIntent;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;

public final class JobCompleteProcessor implements CommandProcessor<JobRecord> {

  private final MutableJobState jobState;
  private final MutableEventScopeInstanceState eventScopeInstanceState;
  private final MutableElementInstanceState elementInstanceState;
  private final DefaultJobCommandPreconditionGuard defaultProcessor;
  private final JobMetrics jobMetrics;
  private final EventHandle eventHandle;
  private final BpmnStateBehavior stateBehavior;

  public JobCompleteProcessor(
      final MutableZeebeState state,
      final JobMetrics jobMetrics,
      final EventHandle eventHandle,
      final BpmnStateBehavior stateBehavior) {
    jobState = state.getJobState();
    eventScopeInstanceState = state.getEventScopeInstanceState();
    elementInstanceState = state.getElementInstanceState();
    defaultProcessor =
        new DefaultJobCommandPreconditionGuard(
            "complete",
            jobState,
            (record, commandControl, sideEffect) -> acceptCommand(record, commandControl));
    this.jobMetrics = jobMetrics;
    this.eventHandle = eventHandle;
    this.stateBehavior = stateBehavior;
  }

  @Override
  public boolean onCommand(
      final TypedRecord<JobRecord> command, final CommandControl<JobRecord> commandControl) {
    return defaultProcessor.onCommand(command, commandControl);
  }

  @Override
  public void afterAccept(
      final TypedCommandWriter commandWriter,
      final StateWriter stateWriter,
      final long key,
      final Intent intent,
      final JobRecord value) {

    final var serviceTaskKey = value.getElementInstanceKey();

    final ElementInstance serviceTask = elementInstanceState.getInstance(serviceTaskKey);

    if (serviceTask != null) {
      final long scopeKey = serviceTask.getValue().getFlowScopeKey();
      final ElementInstance scopeInstance = elementInstanceState.getInstance(scopeKey);

      if (scopeInstance != null && scopeInstance.isActive()) {
        eventHandle.triggeringProcessEvent(value);
        commandWriter.appendFollowUpCommand(
            serviceTaskKey, ProcessInstanceIntent.COMPLETE_ELEMENT, serviceTask.getValue());
      }

      final var parentKey = serviceTask.getParentKey();
      if (parentKey > 0) {
        final BpmnElementContext elementContext =
            stateBehavior.getElementContext(
                serviceTaskKey, serviceTask.getValue(), serviceTask.getState());

        stateBehavior.terminateChildElementInstanceWithCompletionCondition(
            elementContext,
            (childContext) -> {
              final ElementInstance childElementInstance =
                  elementInstanceState.getInstance(childContext.getElementInstanceKey());
              if (childElementInstance.isActive()) {

                final long childKey = childElementInstance.getKey();
                final long jobKey = childElementInstance.getJobKey();

                if (jobState.exists(jobKey)) {
                  final JobRecord jobRecord = jobState.getJob(jobKey);
                  if (jobRecord != null) {
                    stateWriter.appendFollowUpEvent(jobKey, JobIntent.TERMINATED, jobRecord);
                    jobState.delete(jobKey, jobRecord);
                  }

                  final ProcessInstanceRecord processInstanceRecord =
                      childElementInstance.getValue();
                  childElementInstance.setState(ProcessInstanceIntent.ELEMENT_TERMINATING);
                  elementInstanceState.updateInstance(childElementInstance);
                  stateWriter.appendFollowUpEvent(
                      childKey, ProcessInstanceIntent.ELEMENT_TERMINATING, processInstanceRecord);

                  childElementInstance.setState(ProcessInstanceIntent.ELEMENT_TERMINATED);
                  elementInstanceState.updateInstance(childElementInstance);
                  stateWriter.appendFollowUpEvent(
                      childKey, ProcessInstanceIntent.ELEMENT_TERMINATED, processInstanceRecord);

                  eventScopeInstanceState.deleteInstance(childKey);
                  elementInstanceState.removeInstance(childKey);
                }
              }
            });
      }
    }
  }

  private void acceptCommand(
      final TypedRecord<JobRecord> command, final CommandControl<JobRecord> commandControl) {

    final long jobKey = command.getKey();

    final JobRecord job = jobState.getJob(jobKey);

    job.setVariables(command.getValue().getVariablesBuffer());

    commandControl.accept(JobIntent.COMPLETED, job);
    jobMetrics.jobCompleted(job.getType());
  }
}
