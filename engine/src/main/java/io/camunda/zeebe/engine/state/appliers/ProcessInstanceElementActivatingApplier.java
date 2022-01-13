/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.state.appliers;

import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableCallActivity;
import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableCatchEventElement;
import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableCatchEventSupplier;
import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableFlowElementContainer;
import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableFlowNode;
import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableJobWorkerElement;
import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableLoopCharacteristics;
import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableMultiInstanceBody;
import io.camunda.zeebe.engine.state.TypedEventApplier;
import io.camunda.zeebe.engine.state.immutable.ProcessState;
import io.camunda.zeebe.engine.state.instance.ElementInstance;
import io.camunda.zeebe.engine.state.instance.EventTrigger;
import io.camunda.zeebe.engine.state.mutable.MutableElementInstanceState;
import io.camunda.zeebe.engine.state.mutable.MutableEventScopeInstanceState;
import io.camunda.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceRecord;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.camunda.zeebe.util.buffer.BufferUtil;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.IntStream;
import org.agrona.DirectBuffer;

/** Applies state changes for `ProcessInstance:Element_Activating` */
final class ProcessInstanceElementActivatingApplier
    implements TypedEventApplier<ProcessInstanceIntent, ProcessInstanceRecord> {

  private final MutableElementInstanceState elementInstanceState;
  private final ProcessState processState;
  private final MutableEventScopeInstanceState eventScopeInstanceState;

  public ProcessInstanceElementActivatingApplier(
      final MutableElementInstanceState elementInstanceState,
      final ProcessState processState,
      final MutableEventScopeInstanceState eventScopeInstanceState) {
    this.elementInstanceState = elementInstanceState;
    this.processState = processState;
    this.eventScopeInstanceState = eventScopeInstanceState;
  }

  @Override
  public void applyState(final long elementInstanceKey, final ProcessInstanceRecord value) {

    createEventScope(elementInstanceKey, value);
    cleanupSequenceFlowsTaken(value);

    final var flowScopeInstance = elementInstanceState.getInstance(value.getFlowScopeKey());
    elementInstanceState.newInstance(
        flowScopeInstance, elementInstanceKey, value, ProcessInstanceIntent.ELEMENT_ACTIVATING);

    if (flowScopeInstance == null) {
      applyRootProcessState(elementInstanceKey, value);
      return;
    }

    final var flowScopeElementType = flowScopeInstance.getValue().getBpmnElementType();
    final var currentElementType = value.getBpmnElementType();

    decrementActiveSequenceFlow(value, flowScopeInstance, flowScopeElementType, currentElementType);

    if (currentElementType == BpmnElementType.START_EVENT
        && flowScopeElementType == BpmnElementType.EVENT_SUB_PROCESS) {
      final EventTrigger flowScopeEventTrigger =
          eventScopeInstanceState.peekEventTrigger(flowScopeInstance.getParentKey());
      moveVariablesToNewEventScope(
          flowScopeEventTrigger, flowScopeInstance.getParentKey(), elementInstanceKey);
    }

    manageMultiInstanceLoopCounter(elementInstanceKey, flowScopeInstance, flowScopeElementType);
    manageMultiInstanceCompletionCondition(flowScopeInstance, flowScopeElementType);
  }

  private void cleanupSequenceFlowsTaken(final ProcessInstanceRecord value) {
    if (value.getBpmnElementType() != BpmnElementType.PARALLEL_GATEWAY) {
      return;
    }

    final var parallelGateway =
        processState.getFlowElement(
            value.getProcessDefinitionKey(), value.getElementIdBuffer(), ExecutableFlowNode.class);

    // before a parallel gateway is activated, all incoming sequence flows of the gateway must
    // be taken at least once. decrement the number of the taken sequence flows for each incoming
    // sequence flow but keep the remaining numbers for the next activation of the gateway.
    // (Tetris principle)
    elementInstanceState.decrementNumberOfTakenSequenceFlows(
        value.getFlowScopeKey(), parallelGateway.getId());
  }

  private void moveVariablesToNewEventScope(
      final EventTrigger eventTrigger, final long oldEventScopeKey, final long newEventScopeKey) {
    // In order to pass the variables of the event trigger to the element instance, we need to
    // create a new event trigger, using the element instance key as the event scope key.
    if (eventTrigger != null) {
      eventScopeInstanceState.triggerEvent(
          newEventScopeKey,
          eventTrigger.getEventKey(),
          eventTrigger.getElementId(),
          eventTrigger.getVariables());
      eventScopeInstanceState.deleteTrigger(oldEventScopeKey, eventTrigger.getEventKey());
    }
  }

  private void applyRootProcessState(
      final long elementInstanceKey, final ProcessInstanceRecord value) {
    final var parentElementInstance =
        elementInstanceState.getInstance(value.getParentElementInstanceKey());
    if (parentElementInstance != null) {
      // this check is not really necessary: if parentElementInstance exists,
      // it should always be a call-activity, but let's try to be safe
      final var parentElementType = parentElementInstance.getValue().getBpmnElementType();
      if (parentElementType == BpmnElementType.CALL_ACTIVITY) {
        parentElementInstance.setCalledChildInstanceKey(elementInstanceKey);
        elementInstanceState.updateInstance(parentElementInstance);
      }
    }
  }

  private void decrementActiveSequenceFlow(
      final ProcessInstanceRecord value,
      final ElementInstance flowScopeInstance,
      final BpmnElementType flowScopeElementType,
      final BpmnElementType currentElementType) {

    // We don't want to decrement the active sequence flow for elements which have no incoming
    // sequence flow and for interrupting event sub processes we reset the count completely.
    // Furthermore some elements are special and need to be handled separately.
    switch (currentElementType) {
      case START_EVENT:
      case BOUNDARY_EVENT:
        break;
      case INTERMEDIATE_CATCH_EVENT:
        decrementIntermediateCatchEventSequenceFlow(value, flowScopeInstance);
        break;
      case PARALLEL_GATEWAY:
        decrementParallelGatewaySequenceFlow(value, flowScopeInstance);
        break;
      case EVENT_SUB_PROCESS:
        decrementEventSubProcessSequenceFlow(value, flowScopeInstance);
        break;
      default:
        if (flowScopeElementType != BpmnElementType.MULTI_INSTANCE_BODY) {
          flowScopeInstance.decrementActiveSequenceFlows();
          elementInstanceState.updateInstance(flowScopeInstance);
        }
        break;
    }
  }

  private void decrementIntermediateCatchEventSequenceFlow(
      final ProcessInstanceRecord value, final ElementInstance flowScopeInstance) {
    // If we are an intermediate catch event and our previous element is an event based gateway,
    // then we don't want to decrement the active flow, since based on the BPMN spec we DON'T take
    // the sequence flow.

    final var executableCatchEventElement =
        processState.getFlowElement(
            value.getProcessDefinitionKey(),
            value.getElementIdBuffer(),
            ExecutableCatchEventElement.class);

    final var incomingSequenceFlow = executableCatchEventElement.getIncoming().get(0);
    final var previousElement = incomingSequenceFlow.getSource();
    if (previousElement.getElementType() != BpmnElementType.EVENT_BASED_GATEWAY) {
      flowScopeInstance.decrementActiveSequenceFlows();
      elementInstanceState.updateInstance(flowScopeInstance);
    }
  }

  private void decrementParallelGatewaySequenceFlow(
      final ProcessInstanceRecord value, final ElementInstance flowScopeInstance) {
    // Parallel gateways can have more than one incoming sequence flow, we need to decrement the
    // active sequence flows based on the incoming count.

    final var executableFlowNode =
        processState.getFlowElement(
            value.getProcessDefinitionKey(), value.getElementIdBuffer(), ExecutableFlowNode.class);
    final var size = executableFlowNode.getIncoming().size();
    IntStream.range(0, size).forEach(i -> flowScopeInstance.decrementActiveSequenceFlows());
    elementInstanceState.updateInstance(flowScopeInstance);
  }

  private void decrementEventSubProcessSequenceFlow(
      final ProcessInstanceRecord value, final ElementInstance flowScopeInstance) {
    // For interrupting event sub processes we need to reset the active sequence flows, because
    // we might have interrupted multiple sequence flows.
    // For non interrupting we do nothing, since we had no incoming sequence flow.

    final var executableFlowElementContainer = getExecutableFlowElementContainer(value);
    final var executableStartEvent = executableFlowElementContainer.getStartEvents().get(0);
    if (executableStartEvent.isInterrupting()) {
      flowScopeInstance.resetActiveSequenceFlows();
    }
    elementInstanceState.updateInstance(flowScopeInstance);
  }

  private ExecutableFlowElementContainer getExecutableFlowElementContainer(
      final ProcessInstanceRecord value) {
    return processState.getFlowElement(
        value.getProcessDefinitionKey(),
        value.getElementIdBuffer(),
        ExecutableFlowElementContainer.class);
  }

  private void manageMultiInstanceLoopCounter(
      final long elementInstanceKey,
      final ElementInstance flowScopeInstance,
      final BpmnElementType flowScopeElementType) {
    if (flowScopeElementType == BpmnElementType.MULTI_INSTANCE_BODY) {
      // update the loop counter of the multi-instance body (starting by 1)
      flowScopeInstance.incrementMultiInstanceLoopCounter();
      elementInstanceState.updateInstance(flowScopeInstance);

      // set the loop counter of the inner instance
      final var loopCounter = flowScopeInstance.getMultiInstanceLoopCounter();
      elementInstanceState.updateInstance(
          elementInstanceKey, instance -> instance.setMultiInstanceLoopCounter(loopCounter));
    }
  }

  private void manageMultiInstanceCompletionCondition(
      final ElementInstance flowScopeInstance, final BpmnElementType flowScopeElementType) {
    if (flowScopeElementType == BpmnElementType.MULTI_INSTANCE_BODY) {

      // set completion conditon of the multi-instance body
      if (flowScopeInstance.getCompletionCondition().isEmpty()) {
        final ProcessInstanceRecord value = flowScopeInstance.getValue();
        final var flowElement =
            processState.getFlowElement(
                value.getProcessDefinitionKey(),
                value.getElementIdBuffer(),
                ExecutableMultiInstanceBody.class);

        final ExecutableLoopCharacteristics loopCharacteristics =
            flowElement.getLoopCharacteristics();
        final Optional<DirectBuffer> completionCondition =
            loopCharacteristics.getCompletionCondition();

        completionCondition
            .map(BufferUtil::bufferAsString)
            .ifPresent(
                expression -> {
                  flowScopeInstance.setCompletionCondition(expression);
                  elementInstanceState.updateInstance(flowScopeInstance);
                });
      }
    }
  }

  private void createEventScope(
      final long elementInstanceKey, final ProcessInstanceRecord elementRecord) {
    Class<? extends ExecutableFlowNode> flowElementClass = ExecutableFlowNode.class;

    // in the case of the multi instance body, it shares the same element ID as that of its
    // contained activity; this means when doing a processState.getFlowElement, you don't know which
    // you will get, and the boundary events will only be bound to the multi instance
    if (elementRecord.getBpmnElementType() == BpmnElementType.MULTI_INSTANCE_BODY) {
      flowElementClass = ExecutableMultiInstanceBody.class;
    }

    final var flowElement =
        processState.getFlowElement(
            elementRecord.getProcessDefinitionKey(),
            elementRecord.getElementIdBuffer(),
            flowElementClass);

    if (flowElement instanceof ExecutableCatchEventSupplier) {
      final var eventSupplier = (ExecutableCatchEventSupplier) flowElement;

      final var hasEvents = !eventSupplier.getEvents().isEmpty();
      if (hasEvents
          || flowElement instanceof ExecutableJobWorkerElement
          || flowElement instanceof ExecutableCallActivity) {
        eventScopeInstanceState.createInstance(
            elementInstanceKey, eventSupplier.getInterruptingElementIds());
      }
    } else if (flowElement instanceof ExecutableJobWorkerElement) {
      eventScopeInstanceState.createInstance(elementInstanceKey, Collections.emptyList());
    }
  }
}
