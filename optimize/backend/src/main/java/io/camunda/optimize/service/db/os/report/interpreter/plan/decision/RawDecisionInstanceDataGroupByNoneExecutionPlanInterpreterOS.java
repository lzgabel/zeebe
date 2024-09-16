/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.optimize.service.db.os.report.interpreter.plan.decision;

import io.camunda.optimize.dto.optimize.query.report.CommandEvaluationResult;
import io.camunda.optimize.dto.optimize.query.report.single.decision.DecisionReportDataDto;
import io.camunda.optimize.service.db.os.OptimizeOpenSearchClient;
import io.camunda.optimize.service.db.os.report.filter.DecisionQueryFilterEnhancerOS;
import io.camunda.optimize.service.db.os.report.interpreter.groupby.decision.DecisionGroupByInterpreterFacadeOS;
import io.camunda.optimize.service.db.os.report.interpreter.view.decision.DecisionViewInterpreterFacadeOS;
import io.camunda.optimize.service.db.reader.DecisionDefinitionReader;
import io.camunda.optimize.service.db.report.ExecutionContext;
import io.camunda.optimize.service.db.report.interpreter.plan.decision.RawDecisionInstanceDataGroupByNoneExecutionPlanInterpreter;
import io.camunda.optimize.service.db.report.plan.decision.DecisionExecutionPlan;
import io.camunda.optimize.service.util.configuration.condition.OpenSearchCondition;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Conditional(OpenSearchCondition.class)
public class RawDecisionInstanceDataGroupByNoneExecutionPlanInterpreterOS
    extends AbstractDecisionExecutionPlanInterpreterOS
    implements RawDecisionInstanceDataGroupByNoneExecutionPlanInterpreter {
  @Getter private final DecisionDefinitionReader decisionDefinitionReader;
  @Getter private final DecisionQueryFilterEnhancerOS queryFilterEnhancer;
  @Getter private final DecisionGroupByInterpreterFacadeOS groupByInterpreter;
  @Getter private final DecisionViewInterpreterFacadeOS viewInterpreter;
  @Getter private final OptimizeOpenSearchClient osClient;

  @Override
  public CommandEvaluationResult<Object> interpret(
      final ExecutionContext<DecisionReportDataDto, DecisionExecutionPlan> executionContext) {
    final CommandEvaluationResult<Object> commandResult = super.interpret(executionContext);
    addNewVariablesAndDtoFieldsToTableColumnConfig(executionContext, commandResult);
    return commandResult;
  }
}