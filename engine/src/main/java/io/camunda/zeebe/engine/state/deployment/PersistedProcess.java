/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.state.deployment;

import io.camunda.zeebe.db.DbValue;
import io.camunda.zeebe.msgpack.UnpackedObject;
import io.camunda.zeebe.msgpack.property.BinaryProperty;
import io.camunda.zeebe.msgpack.property.IntegerProperty;
import io.camunda.zeebe.msgpack.property.LongProperty;
import io.camunda.zeebe.msgpack.property.StringProperty;
import io.camunda.zeebe.protocol.impl.record.value.deployment.ProcessRecord;
import org.agrona.DirectBuffer;

public final class PersistedProcess extends UnpackedObject implements DbValue {

  private final IntegerProperty versionProp = new IntegerProperty("version", -1);
  private final LongProperty keyProp = new LongProperty("key", -1L);
  private final StringProperty bpmnProcessIdProp = new StringProperty("bpmnProcessId");
  private final StringProperty resourceNameProp = new StringProperty("resourceName");
  private final BinaryProperty resourceProp = new BinaryProperty("resource");
  private final StringProperty candidateStarterGroupsProp =
      new StringProperty("candidateStarterGroups", "");
  private final StringProperty candidateStarterUsersProp =
      new StringProperty("candidateStarterUsers", "");

  public PersistedProcess() {
    declareProperty(versionProp)
        .declareProperty(keyProp)
        .declareProperty(bpmnProcessIdProp)
        .declareProperty(resourceNameProp)
        .declareProperty(resourceProp)
        .declareProperty(candidateStarterGroupsProp)
        .declareProperty(candidateStarterUsersProp);
  }

  public void wrap(final ProcessRecord processRecord, final long processDefinitionKey) {
    bpmnProcessIdProp.setValue(processRecord.getBpmnProcessIdBuffer());
    resourceNameProp.setValue(processRecord.getResourceNameBuffer());
    resourceProp.setValue(processRecord.getResourceBuffer());
    candidateStarterGroupsProp.setValue(processRecord.getCandidateStarterGroups());
    candidateStarterUsersProp.setValue(processRecord.getCandidateStarterUsers());

    versionProp.setValue(processRecord.getVersion());
    keyProp.setValue(processDefinitionKey);
  }

  public int getVersion() {
    return versionProp.getValue();
  }

  public long getKey() {
    return keyProp.getValue();
  }

  public DirectBuffer getBpmnProcessId() {
    return bpmnProcessIdProp.getValue();
  }

  public DirectBuffer getResourceName() {
    return resourceNameProp.getValue();
  }

  public DirectBuffer getResource() {
    return resourceProp.getValue();
  }

  public DirectBuffer getCandidateStarterGroups() {
    return candidateStarterGroupsProp.getValue();
  }

  public DirectBuffer getCandidateStarterUsers() {
    return candidateStarterUsersProp.getValue();
  }
}
