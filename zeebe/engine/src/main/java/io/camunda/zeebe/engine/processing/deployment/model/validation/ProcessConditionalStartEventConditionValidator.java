/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine.processing.deployment.model.validation;

import io.camunda.zeebe.el.EvaluationResult;
import io.camunda.zeebe.el.Expression;
import io.camunda.zeebe.el.ExpressionLanguage;
import io.camunda.zeebe.el.ResultType;
import io.camunda.zeebe.model.bpmn.instance.Condition;
import io.camunda.zeebe.model.bpmn.instance.ConditionalEventDefinition;
import io.camunda.zeebe.model.bpmn.instance.Process;
import io.camunda.zeebe.model.bpmn.instance.StartEvent;
import org.camunda.bpm.model.xml.validation.ModelElementValidator;
import org.camunda.bpm.model.xml.validation.ValidationResultCollector;

/**
 * This class validates that the condition of conditional start event can be evaluated without a
 * context (that is, the expressions do not refer to variables) and evaluate to a boolean
 */
final class ProcessConditionalStartEventConditionValidator
    implements ModelElementValidator<StartEvent> {

  private final ExpressionLanguage expressionLanguage;

  ProcessConditionalStartEventConditionValidator(final ExpressionLanguage expressionLanguage) {
    this.expressionLanguage = expressionLanguage;
  }

  @Override
  public Class<StartEvent> getElementType() {
    return StartEvent.class;
  }

  @Override
  public void validate(
      final StartEvent element, final ValidationResultCollector validationResultCollector) {
    if (element.getScope() instanceof Process) {
      element.getEventDefinitions().stream()
          .filter(ConditionalEventDefinition.class::isInstance)
          .map(ConditionalEventDefinition.class::cast)
          .forEach(
              definition -> validateConditionExpression(definition, validationResultCollector));
    }
  }

  private void validateConditionExpression(
      final ConditionalEventDefinition definition,
      final ValidationResultCollector resultCollector) {

    final Condition condition = definition.getCondition();
    if (condition == null) {
      return;
    }
    final String expression = condition.getTextContent();
    if (expression == null) {
      return;
    }
    final Expression parseResult = expressionLanguage.parseExpression(expression);

    final EvaluationResult evaluationResult =
        expressionLanguage.evaluateExpression(parseResult, var -> null);

    if (evaluationResult.isFailure()) {
      resultCollector.addError(
          0,
          String.format(
              "Expected constant expression but found '%s', which could not be evaluated without context: %s",
              expression, evaluationResult.getFailureMessage()));
    } else if (evaluationResult.getType() != ResultType.BOOLEAN) {
      resultCollector.addError(
          0,
          String.format(
              "Expected constant expression of type Boolean for condition '%s', but was %s",
              expression, evaluationResult.getType()));
    }
  }
}
