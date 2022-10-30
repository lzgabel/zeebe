/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.bpmn.behavior;

import io.camunda.zeebe.engine.processing.bpmn.BpmnElementContext;
import io.camunda.zeebe.engine.processing.common.EventHandle;
import io.camunda.zeebe.engine.processing.common.EventTriggerBehavior;
import io.camunda.zeebe.engine.processing.common.Failure;
import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableCatchEvent;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.StateWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.Writers;
import io.camunda.zeebe.engine.state.KeyGenerator;
import io.camunda.zeebe.engine.state.analyzers.CatchEventAnalyzer;
import io.camunda.zeebe.engine.state.analyzers.CatchEventAnalyzer.CatchEventTuple;
import io.camunda.zeebe.engine.state.immutable.ElementInstanceState;
import io.camunda.zeebe.engine.state.immutable.ZeebeState;
import io.camunda.zeebe.engine.state.instance.ElementInstance;
import io.camunda.zeebe.protocol.impl.record.value.escalation.EscalationRecord;
import io.camunda.zeebe.protocol.record.intent.EscalationIntent;
import io.camunda.zeebe.util.Either;
import java.util.Optional;
import org.agrona.DirectBuffer;

public final class BpmnEventPublicationBehavior {

  private final ElementInstanceState elementInstanceState;
  private final EventHandle eventHandle;
  private final CatchEventAnalyzer catchEventAnalyzer;

  private final StateWriter stateWriter;
  private final KeyGenerator keyGenerator;

  public BpmnEventPublicationBehavior(
      final ZeebeState zeebeState,
      final KeyGenerator keyGenerator,
      final EventTriggerBehavior eventTriggerBehavior,
      final Writers writers) {
    elementInstanceState = zeebeState.getElementInstanceState();
    eventHandle =
        new EventHandle(
            keyGenerator,
            zeebeState.getEventScopeInstanceState(),
            writers,
            zeebeState.getProcessState(),
            eventTriggerBehavior);
    catchEventAnalyzer = new CatchEventAnalyzer(zeebeState.getProcessState(), elementInstanceState);
    stateWriter = writers.state();
    this.keyGenerator = keyGenerator;
  }

  /**
   * Throws an error event to the given element instance/catch event pair. Only throws the event if
   * the given element instance is exists and is accepting events, e.g. isn't terminating, wasn't
   * interrupted, etc.
   *
   * @param catchEventTuple a tuple representing a catch event and its current instance
   */
  public void throwErrorEvent(final CatchEventAnalyzer.CatchEventTuple catchEventTuple) {
    final ElementInstance eventScopeInstance = catchEventTuple.getElementInstance();
    final ExecutableCatchEvent catchEvent = catchEventTuple.getCatchEvent();

    if (eventHandle.canTriggerElement(eventScopeInstance, catchEvent.getId())) {
      eventHandle.activateElement(
          catchEvent, eventScopeInstance.getKey(), eventScopeInstance.getValue());
    }
  }

  /**
   * Finds the right catch event for the given error. This is done by going up through the scope
   * hierarchy recursively until a matching catch event is found. If none are found, a failure is
   * returned.
   *
   * <p>The returned {@link CatchEventTuple} can be used to throw the event via {@link
   * #throwErrorEvent(CatchEventTuple)}.
   *
   * @param errorCode the error code of the error event
   * @param context the current element context
   * @return a valid {@link CatchEventTuple} if a catch event is found, or a failure otherwise
   */
  public Either<Failure, CatchEventTuple> findErrorCatchEvent(
      final DirectBuffer errorCode, final BpmnElementContext context) {
    final var flowScopeInstance = elementInstanceState.getInstance(context.getFlowScopeKey());
    return catchEventAnalyzer.findCatchEvent(errorCode, flowScopeInstance, Optional.empty());
  }

  public Optional<CatchEventTuple> findEscalationCatchEvent(
      final DirectBuffer escalationCode, final BpmnElementContext context) {
    final var flowScopeInstance = elementInstanceState.getInstance(context.getFlowScopeKey());
    return catchEventAnalyzer.findEscalationCatchEvent(escalationCode, flowScopeInstance);
  }

  /**
   * Throws an escalation event to the given element instance/catch event pair. Only throws the
   * event if the given element instance is exists and is accepting events, e.g. isn't terminating,
   * wasn't interrupted, etc.
   *
   * @param record the record of the escalation
   * @param escalationCatchEvent a tuple representing a catch event and its current instance
   * @return returns true if the escalation throw event can be completed, false otherwise
   */
  public boolean throwEscalationEvent(
      final EscalationRecord record, final Optional<CatchEventTuple> escalationCatchEvent) {
    boolean canBeCompleted = true;
    boolean escalated = false;
    final var key = keyGenerator.nextKey();

    if (escalationCatchEvent.isPresent()) {
      final CatchEventTuple catchEventTuple = escalationCatchEvent.get();
      final ElementInstance eventScopeInstance = catchEventTuple.getElementInstance();
      final ExecutableCatchEvent catchEvent = catchEventTuple.getCatchEvent();
      // update catch element id
      record.setCatchElementId(catchEvent.getId());
      // if the escalation catch event is interrupt event, then throw event is not allowed to
      // complete.
      canBeCompleted = !catchEvent.isInterrupting();

      if (eventHandle.canTriggerElement(eventScopeInstance, catchEvent.getId())) {
        eventHandle.activateElement(
            catchEvent, eventScopeInstance.getKey(), eventScopeInstance.getValue());
        stateWriter.appendFollowUpEvent(key, EscalationIntent.ESCALATED, record);
        escalated = true;
      }
    }

    if (!escalated) {
      stateWriter.appendFollowUpEvent(key, EscalationIntent.NOT_ESCALATED, record);
    }

    return canBeCompleted;
  }
}
