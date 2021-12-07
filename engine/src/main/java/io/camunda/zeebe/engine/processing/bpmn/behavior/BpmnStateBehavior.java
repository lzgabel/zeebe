/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.bpmn.behavior;

import io.camunda.zeebe.engine.processing.bpmn.BpmnElementContext;
import io.camunda.zeebe.engine.processing.bpmn.BpmnElementContextImpl;
import io.camunda.zeebe.engine.processing.bpmn.BpmnProcessingException;
import io.camunda.zeebe.engine.processing.common.ExpressionProcessor;
import io.camunda.zeebe.engine.processing.common.Failure;
import io.camunda.zeebe.engine.processing.variable.VariableBehavior;
import io.camunda.zeebe.engine.state.deployment.DeployedProcess;
import io.camunda.zeebe.engine.state.immutable.ElementInstanceState;
import io.camunda.zeebe.engine.state.immutable.JobState;
import io.camunda.zeebe.engine.state.immutable.ProcessState;
import io.camunda.zeebe.engine.state.immutable.VariableState;
import io.camunda.zeebe.engine.state.immutable.ZeebeState;
import io.camunda.zeebe.engine.state.instance.ElementInstance;
import io.camunda.zeebe.msgpack.spec.MsgPackReader;
import io.camunda.zeebe.msgpack.spec.MsgPackWriter;
import io.camunda.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceRecord;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.camunda.zeebe.util.Either;
import io.camunda.zeebe.util.buffer.BufferUtil;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public final class BpmnStateBehavior {

  private static final DirectBuffer NR_OF_COMPLETED_ELEMENT_INSTANCES =
      BufferUtil.wrapString("nrOfCompletedElementInstances");

  private final ElementInstanceState elementInstanceState;
  private final VariableState variablesState;
  private final JobState jobState;
  private final ProcessState processState;
  private final VariableBehavior variableBehavior;
  private final ExpressionProcessor expressionProcessor;

  private final MsgPackReader variableReader = new MsgPackReader();
  private final MsgPackWriter variableWriter = new MsgPackWriter();

  private final MutableDirectBuffer nrOfCompletedElementInstancesVariableBuffer =
      new UnsafeBuffer(new byte[Long.BYTES + 1]);
  private final DirectBuffer nrOfCompletedElementInstancesVariableView = new UnsafeBuffer(0, 0);

  public BpmnStateBehavior(
      final ZeebeState zeebeState,
      final VariableBehavior variableBehavior,
      final ExpressionProcessor expressionProcessor) {
    this.variableBehavior = variableBehavior;
    this.expressionProcessor = expressionProcessor;
    processState = zeebeState.getProcessState();
    elementInstanceState = zeebeState.getElementInstanceState();
    variablesState = zeebeState.getVariableState();
    jobState = zeebeState.getJobState();
  }

  public ElementInstance getElementInstance(final BpmnElementContext context) {
    return getElementInstance(context.getElementInstanceKey());
  }

  public ElementInstance getElementInstance(final long elementInstanceKey) {
    return elementInstanceState.getInstance(elementInstanceKey);
  }

  public JobState getJobState() {
    return jobState;
  }

  // used by canceling, since we don't care about active sequence flows
  public boolean canBeTerminated(final BpmnElementContext context) {
    final ElementInstance flowScopeInstance = getFlowScopeInstance(context);

    if (flowScopeInstance == null) {
      return false;
    }

    final long activePaths = flowScopeInstance.getNumberOfActiveElementInstances();
    if (activePaths < 0) {
      throw new BpmnProcessingException(
          context,
          String.format(
              "Expected number of active paths to be positive but got %d for instance %s",
              activePaths, flowScopeInstance));
    }

    return activePaths == 0;
  }

  public boolean canBeCompleted(final BpmnElementContext context) {
    final ElementInstance flowScopeInstance = getFlowScopeInstance(context);

    if (flowScopeInstance == null) {
      return false;
    }

    if (canBeCompletedWithCompletionCondition(context)) {
      return true;
    }

    final long activePaths =
        flowScopeInstance.getNumberOfActiveElementInstances()
            + flowScopeInstance.getActiveSequenceFlows();
    if (activePaths < 0) {
      throw new BpmnProcessingException(
          context,
          String.format(
              "Expected number of active paths to be positive but got %d for instance %s",
              activePaths, flowScopeInstance));
    }

    return activePaths == 0;
  }

  private boolean isMultiInstance(final ElementInstance elementInstance) {
    final ProcessInstanceRecord value = elementInstance.getValue();
    return elementInstance.getKey() > 0
        && value.getBpmnElementType() == BpmnElementType.MULTI_INSTANCE_BODY;
  }

  public boolean canBeCompletedWithCompletionCondition(final BpmnElementContext context) {
    final ElementInstance elementInstance = getFlowScopeInstance(context);
    if (isMultiInstance(elementInstance)) {
      return canBeCompletedWithCompletionCondition(elementInstance);
    } else {
      return false;
    }
  }

  public boolean canBeCompletedWithCompletionCondition(final ElementInstance elementInstance) {
    if (elementInstance.isActive()) {
      final var parentElementInstanceKey = elementInstance.getKey();
      final String completionCondition = elementInstance.getCompletionCondition();
      if (!completionCondition.isEmpty()) {
        final Either<Failure, Boolean> failureBooleanEither =
            expressionProcessor.evaluateBooleanExpression(
                completionCondition, parentElementInstanceKey);
        if (failureBooleanEither.isLeft()) {
          return false;
        } else if (failureBooleanEither.isRight()) {
          return failureBooleanEither.get();
        }
      }
    }
    return false;
  }

  public void incrementCompletedElementInstance(final ElementInstance elementInstance) {
    long numberOfCompiletedElementInstances = 0;
    final BpmnElementContext flowScopeContext =
        getElementContext(
            elementInstance.getKey(), elementInstance.getValue(), elementInstance.getState());

    final DirectBuffer numberOfCompiletedElementInstancesBuffer =
        getLocalVariable(flowScopeContext, NR_OF_COMPLETED_ELEMENT_INSTANCES);

    if (numberOfCompiletedElementInstancesBuffer != null) {
      variableReader.wrap(
          numberOfCompiletedElementInstancesBuffer,
          0,
          numberOfCompiletedElementInstancesBuffer.capacity());
      numberOfCompiletedElementInstances = variableReader.readInteger();
    }

    setLocalVariable(
        flowScopeContext,
        NR_OF_COMPLETED_ELEMENT_INSTANCES,
        wrapNumberOfCompletedElementInstances(numberOfCompiletedElementInstances + 1));
  }

  public void terminateChildElementInstanceWithCompletionCondition(
      final BpmnElementContext context, final Consumer<BpmnElementContext> contextConsumer) {
    final ElementInstance parentElementInstance = getFlowScopeInstance(context);
    if (isMultiInstance(parentElementInstance)) {
      incrementCompletedElementInstance(parentElementInstance);

      if (canBeCompletedWithCompletionCondition(parentElementInstance)) {
        // terminate all child element instance
        final BpmnElementContext elementContext = getParentElementInstanceContext(context);
        final List<BpmnElementContext> childInstances = getChildInstances(elementContext);
        childInstances.forEach(
            childContext -> {
              final ElementInstance elementInstance =
                  getElementInstance(childContext.getElementInstanceKey());
              if (elementInstance.isActive()) {
                contextConsumer.accept(childContext);
              }
            });
      }
    }
  }

  private DirectBuffer wrapNumberOfCompletedElementInstances(
      final long nrOfCompletedElementInstances) {
    variableWriter.wrap(nrOfCompletedElementInstancesVariableBuffer, 0);

    variableWriter.writeInteger(nrOfCompletedElementInstances);
    final var length = variableWriter.getOffset();

    nrOfCompletedElementInstancesVariableView.wrap(
        nrOfCompletedElementInstancesVariableBuffer, 0, length);
    return nrOfCompletedElementInstancesVariableView;
  }

  public ElementInstance getFlowScopeInstance(final BpmnElementContext context) {
    return elementInstanceState.getInstance(context.getFlowScopeKey());
  }

  public List<BpmnElementContext> getChildInstances(final BpmnElementContext context) {
    return elementInstanceState.getChildren(context.getElementInstanceKey()).stream()
        .map(
            childInstance ->
                context.copy(
                    childInstance.getKey(), childInstance.getValue(), childInstance.getState()))
        .collect(Collectors.toList());
  }

  public BpmnElementContext getFlowScopeContext(final BpmnElementContext context) {
    final var flowScope = getFlowScopeInstance(context);
    return context.copy(flowScope.getKey(), flowScope.getValue(), flowScope.getState());
  }

  public BpmnElementContext getParentElementInstanceContext(final BpmnElementContext context) {
    final var parentElementInstance =
        elementInstanceState.getInstance(context.getParentElementInstanceKey());
    return context.copy(
        parentElementInstance.getKey(),
        parentElementInstance.getValue(),
        parentElementInstance.getState());
  }

  public BpmnElementContext getElementContext(
      final long elementInstanceKey,
      final ProcessInstanceRecord recordValue,
      final ProcessInstanceIntent intent) {
    final var copy = new BpmnElementContextImpl();
    copy.init(elementInstanceKey, recordValue, intent);
    return copy;
  }

  public Optional<DeployedProcess> getProcess(final long processDefinitionKey) {
    return Optional.ofNullable(processState.getProcessByKey(processDefinitionKey));
  }

  public Optional<DeployedProcess> getLatestProcessVersion(final DirectBuffer processId) {
    final var process = processState.getLatestProcessVersionByProcessId(processId);
    return Optional.ofNullable(process);
  }

  public Optional<ElementInstance> getCalledChildInstance(final BpmnElementContext context) {
    final var elementInstance = getElementInstance(context);
    final var calledChildInstanceKey = elementInstance.getCalledChildInstanceKey();
    return Optional.ofNullable(elementInstanceState.getInstance(calledChildInstanceKey));
  }

  public DirectBuffer getLocalVariable(
      final BpmnElementContext context, final DirectBuffer variableName) {
    return variablesState.getVariableLocal(context.getElementInstanceKey(), variableName);
  }

  public void setLocalVariable(
      final BpmnElementContext context,
      final DirectBuffer variableName,
      final DirectBuffer variableValue) {
    setLocalVariable(context, variableName, variableValue, 0, variableValue.capacity());
  }

  public void setLocalVariable(
      final BpmnElementContext context,
      final DirectBuffer variableName,
      final DirectBuffer variableValue,
      final int valueOffset,
      final int valueLength) {
    variableBehavior.setLocalVariable(
        context.getElementInstanceKey(),
        context.getProcessDefinitionKey(),
        context.getProcessInstanceKey(),
        variableName,
        variableValue,
        valueOffset,
        valueLength);
  }

  public void propagateVariable(final BpmnElementContext context, final DirectBuffer variableName) {

    final var sourceScope = context.getElementInstanceKey();
    final var targetScope = context.getFlowScopeKey();

    final var variablesAsDocument =
        variablesState.getVariablesAsDocument(sourceScope, List.of(variableName));

    variableBehavior.mergeDocument(
        targetScope,
        context.getProcessDefinitionKey(),
        context.getProcessInstanceKey(),
        variablesAsDocument);
  }

  public void copyVariablesToProcessInstance(
      final long sourceScopeKey,
      final long targetProcessInstanceKey,
      final DeployedProcess targetProcess) {
    final var variables = variablesState.getVariablesAsDocument(sourceScopeKey);
    variableBehavior.mergeDocument(
        targetProcessInstanceKey, targetProcess.getKey(), targetProcessInstanceKey, variables);
  }

  public boolean isInterrupted(final BpmnElementContext flowScopeContext) {
    final var flowScopeInstance =
        elementInstanceState.getInstance(flowScopeContext.getElementInstanceKey());
    return flowScopeInstance.getNumberOfActiveElementInstances() == 0
        && flowScopeInstance.isInterrupted()
        && flowScopeInstance.isActive();
  }
}
