/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.bpmn.event;

import static io.camunda.zeebe.util.EnsureUtil.ensureNotNull;
import static io.camunda.zeebe.util.EnsureUtil.ensureNotNullOrEmpty;

import io.camunda.zeebe.engine.processing.bpmn.BpmnElementContext;
import io.camunda.zeebe.engine.processing.bpmn.BpmnElementProcessor;
import io.camunda.zeebe.engine.processing.bpmn.behavior.BpmnBehaviors;
import io.camunda.zeebe.engine.processing.bpmn.behavior.BpmnEventPublicationBehavior;
import io.camunda.zeebe.engine.processing.bpmn.behavior.BpmnIncidentBehavior;
import io.camunda.zeebe.engine.processing.bpmn.behavior.BpmnJobBehavior;
import io.camunda.zeebe.engine.processing.bpmn.behavior.BpmnStateTransitionBehavior;
import io.camunda.zeebe.engine.processing.bpmn.behavior.BpmnVariableMappingBehavior;
import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableIntermediateThrowEvent;
import io.camunda.zeebe.engine.state.analyzers.CatchEventAnalyzer.CatchEventTuple;
import io.camunda.zeebe.protocol.impl.record.value.escalation.EscalationRecord;
import io.camunda.zeebe.util.buffer.BufferUtil;
import java.util.List;
import java.util.Optional;

public class IntermediateThrowEventProcessor
    implements BpmnElementProcessor<ExecutableIntermediateThrowEvent> {

  private final List<IntermediateThrowEventBehavior> throwEventBehaviors =
      List.of(
          new NoneIntermediateThrowEventBehavior(),
          new MessageIntermediateThrowEventBehavior(),
          new LinkIntermediateThrowEventBehavior(),
          new EscalationIntermediateThrowEventBehavior());

  private final BpmnVariableMappingBehavior variableMappingBehavior;
  private final BpmnStateTransitionBehavior stateTransitionBehavior;
  private final BpmnIncidentBehavior incidentBehavior;
  private final BpmnJobBehavior jobBehavior;
  private final BpmnEventPublicationBehavior eventPublicationBehavior;

  public IntermediateThrowEventProcessor(
      final BpmnBehaviors bpmnBehaviors,
      final BpmnStateTransitionBehavior stateTransitionBehavior) {
    variableMappingBehavior = bpmnBehaviors.variableMappingBehavior();
    this.stateTransitionBehavior = stateTransitionBehavior;
    incidentBehavior = bpmnBehaviors.incidentBehavior();
    jobBehavior = bpmnBehaviors.jobBehavior();
    eventPublicationBehavior = bpmnBehaviors.eventPublicationBehavior();
  }

  @Override
  public Class<ExecutableIntermediateThrowEvent> getType() {
    return ExecutableIntermediateThrowEvent.class;
  }

  @Override
  public void onActivate(
      final ExecutableIntermediateThrowEvent element, final BpmnElementContext activating) {
    eventBehaviorOf(element).onActivate(element, activating);
  }

  @Override
  public void onComplete(
      final ExecutableIntermediateThrowEvent element, final BpmnElementContext completing) {
    eventBehaviorOf(element).onComplete(element, completing);
  }

  @Override
  public void onTerminate(
      final ExecutableIntermediateThrowEvent element, final BpmnElementContext terminating) {
    eventBehaviorOf(element).onTerminate(element, terminating);

    // common behavior for all intermediate throw events
    final var terminated = stateTransitionBehavior.transitionToTerminated(terminating);
    incidentBehavior.resolveIncidents(terminated);
    stateTransitionBehavior.onElementTerminated(element, terminated);
  }

  private IntermediateThrowEventBehavior eventBehaviorOf(
      final ExecutableIntermediateThrowEvent element) {
    return throwEventBehaviors.stream()
        .filter(behavior -> behavior.isSuitableForEvent(element))
        .findFirst()
        .orElseThrow(
            () ->
                new UnsupportedOperationException(
                    "This kind of intermediate throw event is not supported."));
  }

  private interface IntermediateThrowEventBehavior {

    boolean isSuitableForEvent(final ExecutableIntermediateThrowEvent element);

    void onActivate(
        final ExecutableIntermediateThrowEvent element, final BpmnElementContext activating);

    default void onComplete(
        final ExecutableIntermediateThrowEvent element, final BpmnElementContext completing) {}

    default void onTerminate(
        final ExecutableIntermediateThrowEvent element, final BpmnElementContext terminating) {}
  }

  private class NoneIntermediateThrowEventBehavior implements IntermediateThrowEventBehavior {

    @Override
    public boolean isSuitableForEvent(final ExecutableIntermediateThrowEvent element) {
      return element.isNoneThrowEvent();
    }

    @Override
    public void onActivate(
        final ExecutableIntermediateThrowEvent element, final BpmnElementContext activating) {
      final var activated = stateTransitionBehavior.transitionToActivated(activating);
      stateTransitionBehavior.completeElement(activated);
    }

    @Override
    public void onComplete(
        final ExecutableIntermediateThrowEvent element, final BpmnElementContext completing) {
      variableMappingBehavior
          .applyOutputMappings(completing, element)
          .flatMap(ok -> stateTransitionBehavior.transitionToCompleted(element, completing))
          .ifRightOrLeft(
              completed -> stateTransitionBehavior.takeOutgoingSequenceFlows(element, completed),
              failure -> incidentBehavior.createIncident(failure, completing));
    }
  }

  private class MessageIntermediateThrowEventBehavior implements IntermediateThrowEventBehavior {

    @Override
    public boolean isSuitableForEvent(final ExecutableIntermediateThrowEvent element) {
      return element.isMessageThrowEvent();
    }

    @Override
    public void onActivate(
        final ExecutableIntermediateThrowEvent element, final BpmnElementContext activating) {
      if (element.getJobWorkerProperties() != null) {
        variableMappingBehavior
            .applyInputMappings(activating, element)
            .flatMap(ok -> jobBehavior.createNewJob(activating, element))
            .ifRightOrLeft(
                ok -> stateTransitionBehavior.transitionToActivated(activating),
                failure -> incidentBehavior.createIncident(failure, activating));
      }
    }

    @Override
    public void onComplete(
        final ExecutableIntermediateThrowEvent element, final BpmnElementContext completing) {
      variableMappingBehavior
          .applyOutputMappings(completing, element)
          .flatMap(ok -> stateTransitionBehavior.transitionToCompleted(element, completing))
          .ifRightOrLeft(
              completed -> stateTransitionBehavior.takeOutgoingSequenceFlows(element, completed),
              failure -> incidentBehavior.createIncident(failure, completing));
    }

    @Override
    public void onTerminate(
        final ExecutableIntermediateThrowEvent element, final BpmnElementContext terminating) {
      if (element.getJobWorkerProperties() != null) {
        jobBehavior.cancelJob(terminating);
      }
    }
  }

  private class LinkIntermediateThrowEventBehavior implements IntermediateThrowEventBehavior {
    @Override
    public boolean isSuitableForEvent(final ExecutableIntermediateThrowEvent element) {
      return element.isLinkThrowEvent();
    }

    @Override
    public void onActivate(
        final ExecutableIntermediateThrowEvent element, final BpmnElementContext activating) {
      final var activated = stateTransitionBehavior.transitionToActivated(activating);
      stateTransitionBehavior.completeElement(activated);
    }

    @Override
    public void onComplete(
        final ExecutableIntermediateThrowEvent element, final BpmnElementContext completing) {
      final var link = element.getLink();
      variableMappingBehavior
          .applyOutputMappings(completing, element)
          .flatMap(ok -> stateTransitionBehavior.transitionToCompleted(element, completing))
          .ifRightOrLeft(
              completed ->
                  stateTransitionBehavior.activateElementInstanceInFlowScope(
                      completed, link.getCatchEventElement()),
              failure -> incidentBehavior.createIncident(failure, completing));
    }
  }

  private class EscalationIntermediateThrowEventBehavior implements IntermediateThrowEventBehavior {
    @Override
    public boolean isSuitableForEvent(final ExecutableIntermediateThrowEvent element) {
      return element.isEscalationThrowEvent();
    }

    @Override
    public void onActivate(
        final ExecutableIntermediateThrowEvent element, final BpmnElementContext activating) {
      final var escalation = element.getEscalation();
      ensureNotNull("escalation", escalation);

      final var escalationCode = escalation.getEscalationCode();
      ensureNotNullOrEmpty("escalationCode", escalationCode);

      final var activated = stateTransitionBehavior.transitionToActivated(activating);
      final Optional<CatchEventTuple> escalationCatchEvent =
          eventPublicationBehavior.findEscalationCatchEvent(escalationCode, activating);

      final EscalationRecord record = new EscalationRecord();
      record.setCatchElementId(BufferUtil.wrapString(""));
      record.setThrowElementId(element.getId());
      record.setEscalationCode(BufferUtil.bufferAsString(escalationCode));
      record.setProcessInstanceKey(activated.getProcessInstanceKey());
      final boolean canBeCompleted =
          eventPublicationBehavior.throwEscalationEvent(record, escalationCatchEvent);

      if (canBeCompleted) {
        stateTransitionBehavior.completeElement(activated);
      }
    }

    @Override
    public void onComplete(
        final ExecutableIntermediateThrowEvent element, final BpmnElementContext completing) {
      variableMappingBehavior
          .applyOutputMappings(completing, element)
          .flatMap(ok -> stateTransitionBehavior.transitionToCompleted(element, completing))
          .ifRightOrLeft(
              completed -> stateTransitionBehavior.takeOutgoingSequenceFlows(element, completed),
              failure -> incidentBehavior.createIncident(failure, completing));
    }
  }
}
