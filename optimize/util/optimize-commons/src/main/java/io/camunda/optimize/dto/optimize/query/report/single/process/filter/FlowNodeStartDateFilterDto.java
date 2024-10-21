/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.optimize.dto.optimize.query.report.single.process.filter;

import io.camunda.optimize.dto.optimize.query.report.single.filter.data.date.flownode.FlowNodeDateFilterDataDto;
import java.util.List;

public class FlowNodeStartDateFilterDto extends ProcessFilterDto<FlowNodeDateFilterDataDto<?>> {

  public FlowNodeStartDateFilterDto() {}

  @Override
  public List<FilterApplicationLevel> validApplicationLevels() {
    return List.of(FilterApplicationLevel.VIEW, FilterApplicationLevel.INSTANCE);
  }

  @Override
  protected boolean canEqual(final Object other) {
    return other instanceof FlowNodeStartDateFilterDto;
  }

  @Override
  public int hashCode() {
    return org.apache.commons.lang3.builder.HashCodeBuilder.reflectionHashCode(this);
  }

  @Override
  public boolean equals(final Object o) {
    return org.apache.commons.lang3.builder.EqualsBuilder.reflectionEquals(this, o);
  }
}
