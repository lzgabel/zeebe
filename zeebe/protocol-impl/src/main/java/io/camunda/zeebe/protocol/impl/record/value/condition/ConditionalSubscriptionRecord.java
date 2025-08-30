/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.protocol.impl.record.value.condition;

import static io.camunda.zeebe.util.buffer.BufferUtil.bufferAsString;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.camunda.zeebe.msgpack.property.LongProperty;
import io.camunda.zeebe.msgpack.property.StringProperty;
import io.camunda.zeebe.msgpack.value.StringValue;
import io.camunda.zeebe.protocol.impl.record.UnifiedRecordValue;
import io.camunda.zeebe.protocol.record.value.ConditionalSubscriptionRecordValue;
import io.camunda.zeebe.protocol.record.value.TenantOwned;
import org.agrona.DirectBuffer;

public final class ConditionalSubscriptionRecord extends UnifiedRecordValue
    implements ConditionalSubscriptionRecordValue {

  // Static StringValue keys for property names
  private static final StringValue PROCESS_DEFINITION_KEY_KEY =
      new StringValue("processDefinitionKey");
  private static final StringValue BPMN_PROCESS_ID_KEY = new StringValue("bpmnProcessId");
  private static final StringValue CONDITION_KEY = new StringValue("condition");
  private static final StringValue CATCH_EVENT_ID_KEY = new StringValue("catchEventId");
  private static final StringValue CATCH_EVENT_INSTANCE_KEY_KEY =
      new StringValue("catchEventInstanceKey");
  private static final StringValue TENANT_ID_KEY = new StringValue("tenantId");

  private final LongProperty processDefinitionKeyProp =
      new LongProperty(PROCESS_DEFINITION_KEY_KEY, -1L);
  private final StringProperty bpmnProcessIdProp = new StringProperty(BPMN_PROCESS_ID_KEY, "");
  private final StringProperty conditionProp = new StringProperty(CONDITION_KEY, "");
  private final StringProperty catchEventIdProp = new StringProperty(CATCH_EVENT_ID_KEY, "");
  private final LongProperty catchEventInstanceKeyProp =
      new LongProperty(CATCH_EVENT_INSTANCE_KEY_KEY, -1L);
  private final StringProperty tenantIdProp =
      new StringProperty(TENANT_ID_KEY, TenantOwned.DEFAULT_TENANT_IDENTIFIER);

  public ConditionalSubscriptionRecord() {
    super(6);
    declareProperty(processDefinitionKeyProp)
        .declareProperty(conditionProp)
        .declareProperty(catchEventIdProp)
        .declareProperty(bpmnProcessIdProp)
        .declareProperty(catchEventInstanceKeyProp)
        .declareProperty(tenantIdProp);
  }

  public void wrap(final ConditionalSubscriptionRecord record) {
    processDefinitionKeyProp.setValue(record.getProcessDefinitionKey());
    bpmnProcessIdProp.setValue(record.getBpmnProcessIdBuffer());
    conditionProp.setValue(record.getConditionBuffer());
    catchEventIdProp.setValue(record.getCatchEventId());
    catchEventInstanceKeyProp.setValue(record.getCatchEventInstanceKey());
    tenantIdProp.setValue(record.getConditionBuffer());
  }

  @JsonIgnore
  public DirectBuffer getConditionBuffer() {
    return conditionProp.getValue();
  }

  @JsonIgnore
  public DirectBuffer getCatchEventIdBuffer() {
    return catchEventIdProp.getValue();
  }

  @Override
  public long getProcessDefinitionKey() {
    return processDefinitionKeyProp.getValue();
  }

  @Override
  public String getBpmnProcessId() {
    return bufferAsString(bpmnProcessIdProp.getValue());
  }

  @Override
  public String getCatchEventId() {
    return bufferAsString(catchEventIdProp.getValue());
  }

  @Override
  public long getCatchEventInstanceKey() {
    return catchEventInstanceKeyProp.getValue();
  }

  @Override
  public String getCondition() {
    return bufferAsString(conditionProp.getValue());
  }

  public ConditionalSubscriptionRecord setCondition(final DirectBuffer signalName) {
    conditionProp.setValue(signalName);
    return this;
  }

  public ConditionalSubscriptionRecord setCatchEventInstanceKey(final long catchEventInstanceKey) {
    catchEventInstanceKeyProp.setValue(catchEventInstanceKey);
    return this;
  }

  public ConditionalSubscriptionRecord setCatchEventId(final DirectBuffer catchEventId) {
    catchEventIdProp.setValue(catchEventId);
    return this;
  }

  public ConditionalSubscriptionRecord setBpmnProcessId(final DirectBuffer bpmnProcessId) {
    bpmnProcessIdProp.setValue(bpmnProcessId);
    return this;
  }

  public ConditionalSubscriptionRecord setProcessDefinitionKey(final long key) {
    processDefinitionKeyProp.setValue(key);
    return this;
  }

  @JsonIgnore
  public DirectBuffer getBpmnProcessIdBuffer() {
    return bpmnProcessIdProp.getValue();
  }

  @JsonIgnore
  public long getSubscriptionKey() {
    final long catchEventInstanceKey = catchEventInstanceKeyProp.getValue();
    return catchEventInstanceKey > -1 ? catchEventInstanceKey : processDefinitionKeyProp.getValue();
  }

  @Override
  public String getTenantId() {
    return bufferAsString(tenantIdProp.getValue());
  }

  public ConditionalSubscriptionRecord setTenantId(final String tenantId) {
    tenantIdProp.setValue(tenantId);
    return this;
  }
}
