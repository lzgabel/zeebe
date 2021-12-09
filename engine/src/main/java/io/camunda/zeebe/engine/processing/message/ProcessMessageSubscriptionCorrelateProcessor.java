/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.message;

import static io.camunda.zeebe.util.buffer.BufferUtil.bufferAsString;

import io.camunda.zeebe.engine.processing.bpmn.BpmnElementContext;
import io.camunda.zeebe.engine.processing.bpmn.behavior.BpmnStateBehavior;
import io.camunda.zeebe.engine.processing.common.EventHandle;
import io.camunda.zeebe.engine.processing.common.EventTriggerBehavior;
import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableFlowElement;
import io.camunda.zeebe.engine.processing.message.command.SubscriptionCommandSender;
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecord;
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecordProcessor;
import io.camunda.zeebe.engine.processing.streamprocessor.sideeffect.SideEffectProducer;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.StateWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedRejectionWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedResponseWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedStreamWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.Writers;
import io.camunda.zeebe.engine.state.immutable.ElementInstanceState;
import io.camunda.zeebe.engine.state.immutable.ProcessMessageSubscriptionState;
import io.camunda.zeebe.engine.state.immutable.ProcessState;
import io.camunda.zeebe.engine.state.instance.ElementInstance;
import io.camunda.zeebe.engine.state.message.ProcessMessageSubscription;
import io.camunda.zeebe.engine.state.mutable.MutableZeebeState;
import io.camunda.zeebe.protocol.impl.record.value.message.ProcessMessageSubscriptionRecord;
import io.camunda.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceRecord;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.intent.ProcessMessageSubscriptionIntent;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;

public final class ProcessMessageSubscriptionCorrelateProcessor
    implements TypedRecordProcessor<ProcessMessageSubscriptionRecord> {

  private static final String NO_EVENT_OCCURRED_MESSAGE =
      "Expected to correlate a process message subscription with element key '%d' and message name '%s', "
          + "but the subscription is not active anymore";
  private static final String NO_SUBSCRIPTION_FOUND_MESSAGE =
      "Expected to correlate process message subscription with element key '%d' and message name '%s', "
          + "but no such subscription was found";
  private static final String ALREADY_CLOSING_MESSAGE =
      "Expected to correlate process message subscription with element key '%d' and message name '%s', "
          + "but it is already closing";

  private final ProcessMessageSubscriptionState subscriptionState;
  private final SubscriptionCommandSender subscriptionCommandSender;
  private final ProcessState processState;
  private final ElementInstanceState elementInstanceState;
  private final StateWriter stateWriter;
  private final TypedRejectionWriter rejectionWriter;
  private final EventHandle eventHandle;
  private final BpmnStateBehavior stateBehavior;

  public ProcessMessageSubscriptionCorrelateProcessor(
      final ProcessMessageSubscriptionState subscriptionState,
      final SubscriptionCommandSender subscriptionCommandSender,
      final MutableZeebeState zeebeState,
      final EventTriggerBehavior eventTriggerBehavior,
      final BpmnStateBehavior stateBehavior,
      final Writers writers) {
    this.subscriptionState = subscriptionState;
    this.subscriptionCommandSender = subscriptionCommandSender;
    this.stateBehavior = stateBehavior;
    processState = zeebeState.getProcessState();
    elementInstanceState = zeebeState.getElementInstanceState();
    stateWriter = writers.state();
    rejectionWriter = writers.rejection();

    eventHandle =
        new EventHandle(
            zeebeState.getKeyGenerator(),
            zeebeState.getEventScopeInstanceState(),
            writers,
            processState,
            eventTriggerBehavior);
  }

  @Override
  public void processRecord(
      final TypedRecord<ProcessMessageSubscriptionRecord> command,
      final TypedResponseWriter responseWriter,
      final TypedStreamWriter streamWriter,
      final Consumer<SideEffectProducer> sideEffect) {

    final var record = command.getValue();
    final var elementInstanceKey = record.getElementInstanceKey();

    final ProcessMessageSubscription subscription =
        subscriptionState.getSubscription(elementInstanceKey, record.getMessageNameBuffer());

    if (subscription == null) {
      rejectCommand(command, RejectionType.NOT_FOUND, NO_SUBSCRIPTION_FOUND_MESSAGE);

    } else if (subscription.isClosing()) {
      rejectCommand(command, RejectionType.INVALID_STATE, ALREADY_CLOSING_MESSAGE);

    } else {
      final var elementInstance = elementInstanceState.getInstance(elementInstanceKey);
      final var canTriggerElement = eventHandle.canTriggerElement(elementInstance);

      if (!canTriggerElement) {
        rejectCommand(command, RejectionType.INVALID_STATE, NO_EVENT_OCCURRED_MESSAGE);

      } else {
        acceptCommand(subscription, elementInstanceKey, elementInstance, record);

        final var parentKey = elementInstance.getParentKey();
        if (parentKey > 0) {

          final BpmnElementContext elementContext =
              stateBehavior.getElementContext(
                  elementInstanceKey, elementInstance.getValue(), elementInstance.getState());
          stateBehavior.terminateChildElementInstanceWithCompletionCondition(
              elementContext,
              (childContext) -> {
                final ElementInstance childElementInstance =
                    elementInstanceState.getInstance(childContext.getElementInstanceKey());
                if (childElementInstance.isActive()) {
                  final var key = childElementInstance.getKey();
                  final ElementInstance instance = elementInstanceState.getInstance(key);
                  final ProcessMessageSubscription messageSubscription =
                      subscriptionState.getSubscription(key, record.getMessageNameBuffer());

                  if (messageSubscription != null) {
                    if (eventHandle.canTriggerElement(instance)) {
                      acceptCommand(messageSubscription, key, instance, record);
                    }
                  }
                }
              });
        }
      }
    }
  }

  private void acceptCommand(
      final ProcessMessageSubscription subscription,
      final long elementInstanceKey,
      final ElementInstance elementInstance,
      final ProcessMessageSubscriptionRecord record) {
    // avoid reusing the subscription record directly as any access to the state (e.g. as #get)
    // will overwrite it - safer to just copy its values into an one-time-use record
    final ProcessMessageSubscriptionRecord subscriptionRecord = subscription.getRecord();
    record
        .setElementId(subscriptionRecord.getElementIdBuffer())
        .setInterrupting(subscriptionRecord.isInterrupting());

    stateWriter.appendFollowUpEvent(
        subscription.getKey(), ProcessMessageSubscriptionIntent.CORRELATED, record);

    final var catchEvent = getCatchEvent(elementInstance.getValue(), record.getElementIdBuffer());
    eventHandle.activateElement(
        catchEvent, elementInstanceKey, elementInstance.getValue(), record.getVariablesBuffer());

    sendAcknowledgeCommand(record);
  }

  private ExecutableFlowElement getCatchEvent(
      final ProcessInstanceRecord elementRecord, final DirectBuffer elementId) {
    return processState.getFlowElement(
        elementRecord.getProcessDefinitionKey(), elementId, ExecutableFlowElement.class);
  }

  private void rejectCommand(
      final TypedRecord<ProcessMessageSubscriptionRecord> command,
      final RejectionType rejectionType,
      final String reasonTemplate) {

    final var subscription = command.getValue();
    final var reason =
        String.format(
            reasonTemplate,
            subscription.getElementInstanceKey(),
            bufferAsString(subscription.getMessageNameBuffer()));

    rejectionWriter.appendRejection(command, rejectionType, reason);

    sendRejectionCommand(subscription);
  }

  private void sendAcknowledgeCommand(final ProcessMessageSubscriptionRecord subscription) {
    subscriptionCommandSender.correlateMessageSubscription(
        subscription.getSubscriptionPartitionId(),
        subscription.getProcessInstanceKey(),
        subscription.getElementInstanceKey(),
        subscription.getBpmnProcessIdBuffer(),
        subscription.getMessageNameBuffer());
  }

  private void sendRejectionCommand(final ProcessMessageSubscriptionRecord subscription) {
    subscriptionCommandSender.rejectCorrelateMessageSubscription(
        subscription.getProcessInstanceKey(),
        subscription.getBpmnProcessIdBuffer(),
        subscription.getMessageKey(),
        subscription.getMessageNameBuffer(),
        subscription.getCorrelationKeyBuffer());
  }
}
