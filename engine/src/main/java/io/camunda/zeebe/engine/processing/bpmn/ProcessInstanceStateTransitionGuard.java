/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.bpmn;

import io.camunda.zeebe.engine.processing.bpmn.behavior.BpmnStateBehavior;
import io.camunda.zeebe.engine.processing.common.Failure;
import io.camunda.zeebe.engine.processing.deployment.model.element.AbstractFlowElement;
import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableActivity;
import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableBoundaryEvent;
import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableFlowElement;
import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableFlowNode;
import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableProcess;
import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableSequenceFlow;
import io.camunda.zeebe.engine.state.deployment.DeployedProcess;
import io.camunda.zeebe.engine.state.instance.ElementInstance;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.camunda.zeebe.util.Either;
import io.camunda.zeebe.util.buffer.BufferUtil;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.StringUtils;

/**
 * Checks the preconditions of a state transition command.
 *
 * <p>A process instance can be have concurrent state transitions if a user command is received
 * (e.g. cancel process instance) or if an internal/external event is triggered (e.g. timer boundary
 * event). In this case, the current process instance processing needs to be interrupted be avoid an
 * inconsistent state.
 */
public final class ProcessInstanceStateTransitionGuard {

  private final BpmnStateBehavior stateBehavior;

  public ProcessInstanceStateTransitionGuard(final BpmnStateBehavior stateBehavior) {
    this.stateBehavior = stateBehavior;
  }

  /**
   * Checks if the preconditions of the given command are met.
   *
   * @return {@code true} if the preconditions are met and the transition command is valid.
   */
  public Either<Failure, ?> isValidStateTransition(
      final BpmnElementContext context, final ExecutableFlowElement element) {
    return checkStateTransition(context, element).mapLeft(Failure::new);
  }

  private Either<String, ?> checkStateTransition(
      final BpmnElementContext context, final ExecutableFlowElement element) {
    return switch (context.getIntent()) {
      case ACTIVATE_ELEMENT -> hasActiveFlowScopeInstance(context)
          .flatMap(ok -> canActivateParallelGateway(context, element))
          .flatMap(ok -> canActivateInclusiveGateway(context, element));
      case COMPLETE_ELEMENT ->
      // an incident is resolved by writing a COMPLETE command when the element instance is in
      // state COMPLETING
      hasElementInstanceWithState(
              context,
              ProcessInstanceIntent.ELEMENT_ACTIVATED,
              ProcessInstanceIntent.ELEMENT_COMPLETING)
          .flatMap(ok -> hasActiveFlowScopeInstance(context));
      case TERMINATE_ELEMENT -> hasElementInstanceWithState(
          context,
          ProcessInstanceIntent.ELEMENT_ACTIVATING,
          ProcessInstanceIntent.ELEMENT_ACTIVATED,
          ProcessInstanceIntent.ELEMENT_COMPLETING);
      default -> Either.left(
          String.format(
              "Expected the check of the preconditions of a command with intent [activate,complete,terminate] but the intent was '%s'",
              context.getIntent()));
    };
  }

  private Either<String, ElementInstance> getElementInstance(final BpmnElementContext context) {
    final var elementInstance = stateBehavior.getElementInstance(context);
    if (elementInstance != null) {
      return Either.right(elementInstance);

    } else {
      return Either.left(
          String.format(
              "Expected element instance with key '%d' to be present in state but not found.",
              context.getElementInstanceKey()));
    }
  }

  private Either<String, ElementInstance> getFlowScopeInstance(final BpmnElementContext context) {
    final var flowScopeInstance = stateBehavior.getFlowScopeInstance(context);
    if (flowScopeInstance != null) {
      return Either.right(flowScopeInstance);

    } else {
      return Either.left(
          String.format(
              "Expected flow scope instance with key '%d' to be present in state but not found.",
              context.getFlowScopeKey()));
    }
  }

  private Either<String, ElementInstance> hasElementInstanceInState(
      final ElementInstance elementInstance,
      final ProcessInstanceIntent expectedState,
      final ProcessInstanceIntent... otherExpected) {
    final var currentState = elementInstance.getState();
    if (expectedState != currentState && !Arrays.asList(otherExpected).contains(currentState)) {
      return Either.left(
          String.format(
              "Expected element instance to be in state '%s' or one of '%s' but was '%s'.",
              expectedState, Arrays.toString(otherExpected), currentState));
    } else {
      return Either.right(elementInstance);
    }
  }

  private Either<String, ?> hasElementInstanceWithState(
      final BpmnElementContext context,
      final ProcessInstanceIntent expectedState,
      final ProcessInstanceIntent... otherExpected) {
    // a shortcut to improve readability
    return getElementInstance(context)
        .flatMap(
            elementInstance ->
                hasElementInstanceInState(elementInstance, expectedState, otherExpected));
  }

  private Either<String, ElementInstance> hasFlowScopeInstanceInState(
      final ElementInstance flowScopeInstance, final ProcessInstanceIntent expectedState) {
    final var currentState = flowScopeInstance.getState();
    if (currentState != expectedState) {
      return Either.left(
          String.format(
              "Expected flow scope instance to be in state '%s' but was '%s'.",
              expectedState, currentState));

    } else {
      return Either.right(flowScopeInstance);
    }
  }

  private Either<String, ElementInstance> hasNonInterruptedFlowScope(
      final ElementInstance flowScopeInstance, final BpmnElementContext context) {
    final var interruptingElementId = flowScopeInstance.getInterruptingElementId();
    if (flowScopeInstance.isInterrupted()
        && !interruptingElementId.equals(context.getElementId())) {
      return Either.left(
          String.format(
              "Expected flow scope instance to be not interrupted but was interrupted by an event with id '%s'.",
              BufferUtil.bufferAsString(interruptingElementId)));

    } else {
      return Either.right(flowScopeInstance);
    }
  }

  private Either<String, ?> hasActiveFlowScopeInstance(final BpmnElementContext context) {
    // a shortcut to improve readability
    if (context.getBpmnElementType() == BpmnElementType.PROCESS) {
      // a process has no flow scope instance
      return Either.right(null);

    } else {
      return getFlowScopeInstance(context)
          .flatMap(
              flowScopeInstance ->
                  hasFlowScopeInstanceInState(
                      flowScopeInstance, ProcessInstanceIntent.ELEMENT_ACTIVATED))
          .flatMap(flowScopeInstance -> hasNonInterruptedFlowScope(flowScopeInstance, context));
    }
  }

  private Either<String, ?> canActivateParallelGateway(
      final BpmnElementContext context, final ExecutableFlowElement executableFlowElement) {
    if (context.getBpmnElementType() != BpmnElementType.PARALLEL_GATEWAY) {
      return Either.right(null);
    } else {
      System.out.println("调用 canActivateParallelGateway ");
      final var element = (ExecutableFlowNode) executableFlowElement;
      final int numberOfIncomingSequenceFlows = element.getIncoming().size();
      final int numberOfTakenSequenceFlows =
          stateBehavior.getNumberOfTakenSequenceFlows(context.getFlowScopeKey(), element.getId());
      final Either<String, ?> res = numberOfTakenSequenceFlows >= numberOfIncomingSequenceFlows
          ? Either.right(null)
          : Either.left(
              String.format(
                  "Expected to be able to activate parallel gateway '%s',"
                      + " but not all sequence flows have been taken.",
                  BufferUtil.bufferAsString(element.getId())));
      System.out.println("看下是什么值: " + res.isRight() + " [numberOfTakenSequenceFlows: "
          + numberOfTakenSequenceFlows + " numberOfIncomingSequenceFlows : "
          + numberOfIncomingSequenceFlows + " ]" + (
          numberOfTakenSequenceFlows >= numberOfIncomingSequenceFlows));
      return res;
    }
  }

  private Either<String, ?> canActivateInclusiveGateway(
      final BpmnElementContext context, final ExecutableFlowElement executableFlowElement) {
    if (context.getBpmnElementType() != BpmnElementType.INCLUSIVE_GATEWAY) {
      return Either.right(null);
    } else {
      final List<String> elementIds = stateBehavior.getActivateElementInstance(context);
      final Optional<DeployedProcess> deployedProcess = stateBehavior.getProcess(
          context.getProcessDefinitionKey());

      boolean found = deployedProcess.map(process -> {
        final var target = BufferUtil.bufferAsString(executableFlowElement.getId());
        final List<String> sources = stateBehavior.getActivateElementInstance(context);
        for (String source : sources) {
          final boolean r = dfs(process, source, target);
          if (r) {
            return true;
          }
        }
        return false;
      }).orElse(false);

      final var element = (ExecutableFlowNode) executableFlowElement;
      System.out.println("!found : ======> " + !found);
      Either<String, ?> res = !found
          ? Either.right(null)
          : Either.left(
              String.format(
                  "Expected to be able to activate inclusive gateway '%s',"
                      + " but there are still sequence flows that have not been taken.",
                  BufferUtil.bufferAsString(element.getId())));
      return res;
    }
  }

  public boolean dfs(DeployedProcess instance, String source, String target) {
    if (StringUtils.equals(source, target)) {
      return true;
    }
    ExecutableProcess process = instance.getProcess();
    ExecutableFlowNode sourceNode = process.getElementById(source, ExecutableFlowNode.class);
    List<ExecutableSequenceFlow> outgoingSequenceFlows = sourceNode.getOutgoing();
    // 直接后继
    for (ExecutableSequenceFlow sequenceFlow: outgoingSequenceFlows) {
      String s = BufferUtil.bufferAsString(sequenceFlow.getTarget().getId());
      if (dfs(instance, s, target)) {
        return true;
      }
    }

    // boundary
    if (sourceNode instanceof ExecutableActivity) {
      final List<ExecutableBoundaryEvent> boundaryEvents = ((ExecutableActivity) sourceNode).getBoundaryEvents();
      for (ExecutableBoundaryEvent boundaryEvent: boundaryEvents) {
        final String s = BufferUtil.bufferAsString(boundaryEvent.getId());
        if (dfs(instance, s, target)) {
          return true;
        }
      }
    }
    return false;
  }
}
