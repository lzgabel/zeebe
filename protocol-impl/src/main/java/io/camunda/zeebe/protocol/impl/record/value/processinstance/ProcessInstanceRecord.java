/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.protocol.impl.record.value.processinstance;

import static io.camunda.zeebe.util.buffer.BufferUtil.bufferAsString;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.camunda.zeebe.msgpack.property.DocumentProperty;
import io.camunda.zeebe.msgpack.property.EnumProperty;
import io.camunda.zeebe.msgpack.property.IntegerProperty;
import io.camunda.zeebe.msgpack.property.LongProperty;
import io.camunda.zeebe.msgpack.property.StringProperty;
import io.camunda.zeebe.protocol.impl.encoding.MsgPackConverter;
import io.camunda.zeebe.protocol.impl.record.UnifiedRecordValue;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.camunda.zeebe.protocol.record.value.BpmnEventType;
import io.camunda.zeebe.protocol.record.value.ProcessInstanceRecordValue;
import java.util.Map;
import org.agrona.DirectBuffer;

public final class ProcessInstanceRecord extends UnifiedRecordValue
    implements ProcessInstanceRecordValue {

  public static final String PROP_PROCESS_BPMN_PROCESS_ID = "bpmnProcessId";
  public static final String PROP_PROCESS_INSTANCE_KEY = "processInstanceKey";
  public static final String PROP_PROCESS_ELEMENT_ID = "elementId";
  public static final String PROP_PROCESS_VERSION = "version";
  public static final String PROP_PROCESS_KEY = "processDefinitionKey";
  public static final String PROP_PROCESS_BPMN_TYPE = "bpmnElementType";
  public static final String PROP_PROCESS_SCOPE_KEY = "flowScopeKey";
  public static final String PROP_PROCESS_EVENT_TYPE = "bpmnEventType";

  private final StringProperty bpmnProcessIdProp =
      new StringProperty(PROP_PROCESS_BPMN_PROCESS_ID, "");
  private final IntegerProperty versionProp = new IntegerProperty(PROP_PROCESS_VERSION, -1);
  private final LongProperty processDefinitionKeyProp = new LongProperty(PROP_PROCESS_KEY, -1L);

  private final LongProperty processInstanceKeyProp =
      new LongProperty(PROP_PROCESS_INSTANCE_KEY, -1L);
  private final StringProperty elementIdProp = new StringProperty(PROP_PROCESS_ELEMENT_ID, "");

  private final LongProperty flowScopeKeyProp = new LongProperty(PROP_PROCESS_SCOPE_KEY, -1L);

  private final EnumProperty<BpmnElementType> bpmnElementTypeProp =
      new EnumProperty<>(
          PROP_PROCESS_BPMN_TYPE, BpmnElementType.class, BpmnElementType.UNSPECIFIED);

  private final EnumProperty<BpmnEventType> bpmnEventTypeProp =
      new EnumProperty<>(PROP_PROCESS_EVENT_TYPE, BpmnEventType.class, BpmnEventType.UNSPECIFIED);

  private final LongProperty parentProcessInstanceKeyProp =
      new LongProperty("parentProcessInstanceKey", -1L);
  private final LongProperty parentElementInstanceKeyProp =
      new LongProperty("parentElementInstanceKey", -1L);

  private final DocumentProperty variablesProp = new DocumentProperty("variables");

  public ProcessInstanceRecord() {
    declareProperty(bpmnElementTypeProp)
        .declareProperty(elementIdProp)
        .declareProperty(bpmnProcessIdProp)
        .declareProperty(versionProp)
        .declareProperty(processDefinitionKeyProp)
        .declareProperty(processInstanceKeyProp)
        .declareProperty(flowScopeKeyProp)
        .declareProperty(bpmnEventTypeProp)
        .declareProperty(parentProcessInstanceKeyProp)
        .declareProperty(variablesProp)
        .declareProperty(parentElementInstanceKeyProp);
  }

  public void wrap(final ProcessInstanceRecord record) {
    elementIdProp.setValue(record.getElementIdBuffer());
    bpmnProcessIdProp.setValue(record.getBpmnProcessIdBuffer());
    flowScopeKeyProp.setValue(record.getFlowScopeKey());
    versionProp.setValue(record.getVersion());
    processDefinitionKeyProp.setValue(record.getProcessDefinitionKey());
    processInstanceKeyProp.setValue(record.getProcessInstanceKey());
    bpmnElementTypeProp.setValue(record.getBpmnElementType());
    bpmnEventTypeProp.setValue(record.getBpmnEventType());
    parentProcessInstanceKeyProp.setValue(record.getParentProcessInstanceKey());
    parentElementInstanceKeyProp.setValue(record.getParentElementInstanceKey());
  }

  @JsonIgnore
  public DirectBuffer getBpmnProcessIdBuffer() {
    return bpmnProcessIdProp.getValue();
  }

  @JsonIgnore
  public DirectBuffer getElementIdBuffer() {
    return elementIdProp.getValue();
  }

  public ProcessInstanceRecord setBpmnProcessId(
      final DirectBuffer directBuffer, final int offset, final int length) {
    bpmnProcessIdProp.setValue(directBuffer, offset, length);
    return this;
  }

  @Override
  public String getBpmnProcessId() {
    return bufferAsString(bpmnProcessIdProp.getValue());
  }

  @Override
  public int getVersion() {
    return versionProp.getValue();
  }

  @Override
  public long getProcessDefinitionKey() {
    return processDefinitionKeyProp.getValue();
  }

  @Override
  public long getProcessInstanceKey() {
    return processInstanceKeyProp.getValue();
  }

  public ProcessInstanceRecord setProcessInstanceKey(final long processInstanceKey) {
    processInstanceKeyProp.setValue(processInstanceKey);
    return this;
  }

  @Override
  public String getElementId() {
    return bufferAsString(elementIdProp.getValue());
  }

  @Override
  public long getFlowScopeKey() {
    return flowScopeKeyProp.getValue();
  }

  @Override
  public BpmnElementType getBpmnElementType() {
    return bpmnElementTypeProp.getValue();
  }

  @Override
  public long getParentProcessInstanceKey() {
    return parentProcessInstanceKeyProp.getValue();
  }

  @Override
  public long getParentElementInstanceKey() {
    return parentElementInstanceKeyProp.getValue();
  }

  public ProcessInstanceRecord setParentElementInstanceKey(final long parentElementInstanceKey) {
    parentElementInstanceKeyProp.setValue(parentElementInstanceKey);
    return this;
  }

  @Override
  public Map<String, Object> getVariables() {
    return MsgPackConverter.convertToMap(variablesProp.getValue());
  }

  public DirectBuffer getVariablesBuffer() {
    return variablesProp.getValue();
  }

  public ProcessInstanceRecord setVariables(final DirectBuffer variables) {
    variablesProp.setValue(variables);
    return this;
  }

  @Override
  public BpmnEventType getBpmnEventType() {
    return bpmnEventTypeProp.getValue();
  }

  public ProcessInstanceRecord setBpmnEventType(final BpmnEventType bpmnEventType) {
    bpmnEventTypeProp.setValue(bpmnEventType);
    return this;
  }

  public ProcessInstanceRecord setParentProcessInstanceKey(final long parentProcessInstanceKey) {
    parentProcessInstanceKeyProp.setValue(parentProcessInstanceKey);
    return this;
  }

  public ProcessInstanceRecord setBpmnElementType(final BpmnElementType bpmnType) {
    bpmnElementTypeProp.setValue(bpmnType);
    return this;
  }

  public ProcessInstanceRecord setFlowScopeKey(final long flowScopeKey) {
    flowScopeKeyProp.setValue(flowScopeKey);
    return this;
  }

  public ProcessInstanceRecord setElementId(final String elementId) {
    elementIdProp.setValue(elementId);
    return this;
  }

  public ProcessInstanceRecord setElementId(final DirectBuffer elementId) {
    return setElementId(elementId, 0, elementId.capacity());
  }

  public ProcessInstanceRecord setProcessDefinitionKey(final long processDefinitionKey) {
    processDefinitionKeyProp.setValue(processDefinitionKey);
    return this;
  }

  public ProcessInstanceRecord setVersion(final int version) {
    versionProp.setValue(version);
    return this;
  }

  public ProcessInstanceRecord setBpmnProcessId(final String bpmnProcessId) {
    bpmnProcessIdProp.setValue(bpmnProcessId);
    return this;
  }

  public ProcessInstanceRecord setBpmnProcessId(final DirectBuffer directBuffer) {
    bpmnProcessIdProp.setValue(directBuffer);
    return this;
  }

  public boolean hasParentProcess() {
    return getParentProcessInstanceKey() != -1L;
  }

  public ProcessInstanceRecord setElementId(
      final DirectBuffer elementId, final int offset, final int length) {
    elementIdProp.setValue(elementId, offset, length);
    return this;
  }
}
