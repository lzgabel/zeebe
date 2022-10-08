/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.deployment.model.element;

import org.agrona.DirectBuffer;

public class ExecutableLink extends AbstractFlowElement {

  private DirectBuffer name;

  public ExecutableLink(final String id) {
    super(id);
  }

  public DirectBuffer getName() {
    return name;
  }

  public void setName(final DirectBuffer name) {
    this.name = name;
  }
}
