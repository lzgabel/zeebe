/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine.processing.bpmn.behavior;

import io.camunda.zeebe.el.Expression;
import io.camunda.zeebe.engine.processing.common.EventHandle;
import io.camunda.zeebe.engine.processing.common.EventTriggerBehavior;
import io.camunda.zeebe.engine.processing.common.ExpressionProcessor;
import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableCatchEvent;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.Writers;
import io.camunda.zeebe.engine.state.immutable.ConditionalSubscriptionState;
import io.camunda.zeebe.engine.state.immutable.ElementInstanceState;
import io.camunda.zeebe.engine.state.immutable.ProcessState;
import io.camunda.zeebe.engine.state.immutable.ProcessingState;
import io.camunda.zeebe.engine.state.instance.ElementInstance;
import io.camunda.zeebe.stream.api.state.KeyGenerator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import org.agrona.DirectBuffer;

public class BpmnConditionalEventBehavior {
  private final EventHandle eventHandle;
  private final ElementInstanceState elementInstanceState;
  private final ProcessState processState;
  private final ExpressionProcessor expressionProcessor;
  private final ConditionalSubscriptionState conditionalSubscriptionState;

  public BpmnConditionalEventBehavior(
      final ProcessingState processingState,
      final KeyGenerator keyGenerator,
      final EventTriggerBehavior eventTriggerBehavior,
      final BpmnStateBehavior stateBehavior,
      final ExpressionProcessor expressionProcessor,
      final Writers writers) {

    this.expressionProcessor = expressionProcessor;
    processState = processingState.getProcessState();
    elementInstanceState = processingState.getElementInstanceState();
    conditionalSubscriptionState = processingState.getConditionalSubscriptionState();
    eventHandle =
        new EventHandle(
            keyGenerator,
            processingState.getEventScopeInstanceState(),
            writers,
            processingState.getProcessState(),
            eventTriggerBehavior,
            stateBehavior);
  }

  public void triggerConditionalEvents(
      final String tenantId, final long processDefinitionKey, final long scopeKey) {
    final Queue<Long> queue = new LinkedBlockingQueue<>();
    queue.offer(scopeKey);

    while (!queue.isEmpty()) {
      final var scopeKeys = new ArrayList<>(queue);
      queue.clear();
      final var interruptedCatchEvents = new ArrayList<ExecutableCatchEvent>();
      for (final var key : scopeKeys) {
        triggerConditionalEvent(tenantId, processDefinitionKey, key, interruptedCatchEvents);
      }

      // going down through the scope hierarchy recursively until a matching interrupting catch
      // event is found.
      if (interruptedCatchEvents.isEmpty()) {
        scopeKeys.stream()
            .map(elementInstanceState::getChildren)
            .flatMap(Collection::stream)
            .filter(ElementInstance::isActive)
            .map(ElementInstance::getKey)
            .forEach(queue::offer);
      }
    }
  }

  private void triggerConditionalEvent(
      final String tenantId,
      final long processDefinitionKey,
      final long scopeKey,
      final List<ExecutableCatchEvent> interruptedCatchEvents) {
    conditionalSubscriptionState.visitBySubscriptionKey(
        scopeKey,
        condition -> {
          final var conditionRecord = condition.getRecord();
          final DirectBuffer catchEventId = conditionRecord.getCatchEventIdBuffer();
          final var catchEventInstanceKey = conditionRecord.getCatchEventInstanceKey();
          final var catchEvent =
              processState.getFlowElement(
                  processDefinitionKey, tenantId, catchEventId, ExecutableCatchEvent.class);

          final Expression expression = catchEvent.getConditional().getConditionExpression();
          expressionProcessor
              .evaluateBooleanExpression(expression, scopeKey)
              .ifRight(
                  ok -> {
                    if (ok) {
                      final ElementInstance instance =
                          elementInstanceState.getInstance(
                              conditionRecord.getCatchEventInstanceKey());

                      if (eventHandle.canTriggerElement(instance, catchEventId)) {
                        eventHandle.activateElement(
                            catchEvent, catchEventInstanceKey, instance.getValue());
                      }

                      if (catchEvent.isInterrupting()) {
                        interruptedCatchEvents.add(catchEvent);
                      }
                    }
                  });
        });
  }
}
