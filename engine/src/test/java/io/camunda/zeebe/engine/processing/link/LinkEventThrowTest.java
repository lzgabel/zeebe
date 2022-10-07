/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.link;

import static org.assertj.core.api.Assertions.assertThat;

import io.camunda.zeebe.engine.util.EngineRule;
import io.camunda.zeebe.protocol.record.Assertions;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.intent.DeploymentIntent;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.camunda.zeebe.protocol.record.value.DeploymentRecordValue;
import io.camunda.zeebe.test.util.record.RecordingExporter;
import org.assertj.core.groups.Tuple;
import org.junit.ClassRule;
import org.junit.Test;

public class LinkEventThrowTest {

  @ClassRule public static final EngineRule ENGINE = EngineRule.singlePartition();

  @Test
  public void shouldDeployProcess() {
    final Record<DeploymentRecordValue> deploy =
        ENGINE
            .deployment()
            .withXmlResource(
                """
        <?xml version="1.0" encoding="UTF-8"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI" xmlns:dc="http://www.omg.org/spec/DD/20100524/DC" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" xmlns:di="http://www.omg.org/spec/DD/20100524/DI" xmlns:modeler="http://camunda.org/schema/modeler/1.0" id="Definitions_1nzddlq" targetNamespace="http://bpmn.io/schema/bpmn" exporter="Camunda Modeler" exporterVersion="5.3.0" modeler:executionPlatform="Camunda Cloud" modeler:executionPlatformVersion="8.0.0">
          <bpmn:process id="Process_14apudk" isExecutable="true">
            <bpmn:startEvent id="StartEvent">
              <bpmn:outgoing>Flow_173hni8</bpmn:outgoing>
            </bpmn:startEvent>
            <bpmn:sequenceFlow id="Flow_173hni8" sourceRef="StartEvent" targetRef="Event_Throw" />
            <bpmn:intermediateThrowEvent id="Event_Throw">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="aa" retries="=aaaaa" />
              </bpmn:extensionElements>
              <bpmn:incoming>Flow_173hni8</bpmn:incoming>
              <bpmn:linkEventDefinition id="MessageEventDefinition_1sem3aj" name="aaa" />
            </bpmn:intermediateThrowEvent>
            <bpmn:endEvent id="EndEvent">
              <bpmn:incoming>Flow_0ckkyp5</bpmn:incoming>
            </bpmn:endEvent>
            <bpmn:sequenceFlow id="Flow_0ckkyp5" sourceRef="Event_Start" targetRef="EndEvent" />
            <bpmn:intermediateCatchEvent id="Event_Start">
              <bpmn:outgoing>Flow_0ckkyp5</bpmn:outgoing>
              <bpmn:linkEventDefinition id="MessageEventDefinition_1yua2np" name="aaa" />
            </bpmn:intermediateCatchEvent>
          </bpmn:process>
          <bpmn:message id="Message_279jc7t" name="bbb">
            <bpmn:extensionElements>
              <zeebe:subscription correlationKey="=bbb" />
            </bpmn:extensionElements>
          </bpmn:message>
          <bpmndi:BPMNDiagram id="BPMNDiagram_1">
            <bpmndi:BPMNPlane id="BPMNPlane_1" bpmnElement="Process_14apudk">
              <bpmndi:BPMNShape id="_BPMNShape_StartEvent_2" bpmnElement="StartEvent">
                <dc:Bounds x="179" y="79" width="36" height="36" />
              </bpmndi:BPMNShape>
              <bpmndi:BPMNShape id="Event_0fs1vyz_di" bpmnElement="Event_Throw">
                <dc:Bounds x="382" y="79" width="36" height="36" />
              </bpmndi:BPMNShape>
              <bpmndi:BPMNShape id="Event_03ohp7j_di" bpmnElement="EndEvent">
                <dc:Bounds x="822" y="79" width="36" height="36" />
              </bpmndi:BPMNShape>
              <bpmndi:BPMNShape id="Event_0ehdp1m_di" bpmnElement="Event_Start">
                <dc:Bounds x="612" y="79" width="36" height="36" />
              </bpmndi:BPMNShape>
              <bpmndi:BPMNEdge id="Flow_173hni8_di" bpmnElement="Flow_173hni8">
                <di:waypoint x="215" y="97" />
                <di:waypoint x="382" y="97" />
              </bpmndi:BPMNEdge>
              <bpmndi:BPMNEdge id="Flow_0ckkyp5_di" bpmnElement="Flow_0ckkyp5">
                <di:waypoint x="648" y="97" />
                <di:waypoint x="822" y="97" />
              </bpmndi:BPMNEdge>
            </bpmndi:BPMNPlane>
          </bpmndi:BPMNDiagram>
        </bpmn:definitions>

            """
                    .getBytes())
            .deploy();

    Assertions.assertThat(deploy).hasIntent(DeploymentIntent.CREATED);
  }

  @Test
  public void shouldCompleteProcess() {
    ENGINE
        .deployment()
        .withXmlResource(
            """
        <?xml version="1.0" encoding="UTF-8"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI" xmlns:dc="http://www.omg.org/spec/DD/20100524/DC" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" xmlns:di="http://www.omg.org/spec/DD/20100524/DI" xmlns:modeler="http://camunda.org/schema/modeler/1.0" id="Definitions_1nzddlq" targetNamespace="http://bpmn.io/schema/bpmn" exporter="Camunda Modeler" exporterVersion="5.3.0" modeler:executionPlatform="Camunda Cloud" modeler:executionPlatformVersion="8.0.0">
          <bpmn:process id="Process_14apudk" isExecutable="true">
            <bpmn:startEvent id="StartEvent">
              <bpmn:outgoing>Flow_173hni8</bpmn:outgoing>
            </bpmn:startEvent>
            <bpmn:sequenceFlow id="Flow_173hni8" sourceRef="StartEvent" targetRef="Event_Throw" />
            <bpmn:intermediateThrowEvent id="Event_Throw">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="aa" retries="=aaaaa" />
              </bpmn:extensionElements>
              <bpmn:incoming>Flow_173hni8</bpmn:incoming>
              <bpmn:linkEventDefinition id="MessageEventDefinition_1sem3aj" name="aaa" />
            </bpmn:intermediateThrowEvent>
            <bpmn:endEvent id="EndEvent">
              <bpmn:incoming>Flow_0ckkyp5</bpmn:incoming>
            </bpmn:endEvent>
            <bpmn:sequenceFlow id="Flow_0ckkyp5" sourceRef="Event_Start" targetRef="EndEvent" />
            <bpmn:intermediateCatchEvent id="Event_Start">
              <bpmn:outgoing>Flow_0ckkyp5</bpmn:outgoing>
              <bpmn:linkEventDefinition id="MessageEventDefinition_1yua2np" name="aaa" />
            </bpmn:intermediateCatchEvent>
          </bpmn:process>
          <bpmn:message id="Message_279jc7t" name="bbb">
            <bpmn:extensionElements>
              <zeebe:subscription correlationKey="=bbb" />
            </bpmn:extensionElements>
          </bpmn:message>
          <bpmndi:BPMNDiagram id="BPMNDiagram_1">
            <bpmndi:BPMNPlane id="BPMNPlane_1" bpmnElement="Process_14apudk">
              <bpmndi:BPMNShape id="_BPMNShape_StartEvent_2" bpmnElement="StartEvent">
                <dc:Bounds x="179" y="79" width="36" height="36" />
              </bpmndi:BPMNShape>
              <bpmndi:BPMNShape id="Event_0fs1vyz_di" bpmnElement="Event_Throw">
                <dc:Bounds x="382" y="79" width="36" height="36" />
              </bpmndi:BPMNShape>
              <bpmndi:BPMNShape id="Event_03ohp7j_di" bpmnElement="EndEvent">
                <dc:Bounds x="822" y="79" width="36" height="36" />
              </bpmndi:BPMNShape>
              <bpmndi:BPMNShape id="Event_0ehdp1m_di" bpmnElement="Event_Start">
                <dc:Bounds x="612" y="79" width="36" height="36" />
              </bpmndi:BPMNShape>
              <bpmndi:BPMNEdge id="Flow_173hni8_di" bpmnElement="Flow_173hni8">
                <di:waypoint x="215" y="97" />
                <di:waypoint x="382" y="97" />
              </bpmndi:BPMNEdge>
              <bpmndi:BPMNEdge id="Flow_0ckkyp5_di" bpmnElement="Flow_0ckkyp5">
                <di:waypoint x="648" y="97" />
                <di:waypoint x="822" y="97" />
              </bpmndi:BPMNEdge>
            </bpmndi:BPMNPlane>
          </bpmndi:BPMNDiagram>
        </bpmn:definitions>


            """
                .getBytes())
        .deploy();

    final long processInstanceKey =
        ENGINE.processInstance().ofBpmnProcessId("Process_14apudk").create();

    assertThat(
            RecordingExporter.processInstanceRecords()
                .withProcessInstanceKey(processInstanceKey)
                .limitToProcessInstanceCompleted())
        .extracting(record -> record.getValue().getBpmnElementType(), Record::getIntent)
        .containsSubsequence(
            Tuple.tuple(BpmnElementType.END_EVENT, ProcessInstanceIntent.ELEMENT_COMPLETED),
            Tuple.tuple(BpmnElementType.PROCESS, ProcessInstanceIntent.ELEMENT_COMPLETED));
  }
}
