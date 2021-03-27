/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.zeebe.test.util.bpmn.random.blocks;

import io.zeebe.model.bpmn.builder.AbstractFlowNodeBuilder;
import io.zeebe.test.util.bpmn.random.BlockBuilder;
import io.zeebe.test.util.bpmn.random.BlockBuilderFactory;
import io.zeebe.test.util.bpmn.random.ConstructionContext;
import io.zeebe.test.util.bpmn.random.ExecutionPathSegment;
import io.zeebe.test.util.bpmn.random.IDGenerator;
import io.zeebe.test.util.bpmn.random.steps.StepPublishMessage;
import java.util.Random;

/**
 * Generates a receive task. It waits for a message with name {@code message_[id]} and a correlation
 * key of {@code CORRELATION_KEY_VALUE}
 */
public class ReceiveTaskBlockBuilder implements BlockBuilder {

  private static final String CORRELATION_KEY_FIELD = "correlationKey";
  private static final String CORRELATION_KEY_VALUE = "default_correlation_key";

  private final String id;
  private final String messageName;

  public ReceiveTaskBlockBuilder(final IDGenerator idGenerator) {
    id = idGenerator.nextId();
    messageName = "message_" + id;
  }

  @Override
  public AbstractFlowNodeBuilder<?, ?> buildFlowNodes(
      final AbstractFlowNodeBuilder<?, ?> nodeBuilder) {

    final var receiveTask = nodeBuilder.receiveTask(id);
    receiveTask.message(
        messageBuilder -> {
          messageBuilder.zeebeCorrelationKeyExpression(CORRELATION_KEY_FIELD);
          messageBuilder.name(messageName);
        });

    return receiveTask;
  }

  @Override
  public ExecutionPathSegment findRandomExecutionPath(final Random random) {
    final ExecutionPathSegment result = new ExecutionPathSegment();

    result.append(
        new StepPublishMessage(messageName, CORRELATION_KEY_FIELD, CORRELATION_KEY_VALUE));

    return result;
  }

  public static class Factory implements BlockBuilderFactory {

    @Override
    public BlockBuilder createBlockBuilder(final ConstructionContext context) {
      return new ReceiveTaskBlockBuilder(context.getIdGenerator());
    }

    @Override
    public boolean isAddingDepth() {
      return false;
    }
  }
}
