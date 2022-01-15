/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.incident;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.tuple;

import io.camunda.zeebe.engine.util.EngineRule;
import io.camunda.zeebe.engine.util.RecordToWrite;
import io.camunda.zeebe.engine.util.Records;
import io.camunda.zeebe.engine.util.client.IncidentClient.ResolveIncidentClient;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceRecord;
import io.camunda.zeebe.protocol.record.Assertions;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.intent.IncidentIntent;
import io.camunda.zeebe.protocol.record.intent.JobIntent;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.protocol.record.intent.VariableDocumentIntent;
import io.camunda.zeebe.protocol.record.intent.VariableIntent;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.camunda.zeebe.protocol.record.value.ErrorType;
import io.camunda.zeebe.protocol.record.value.IncidentRecordValue;
import io.camunda.zeebe.protocol.record.value.JobRecordValue;
import io.camunda.zeebe.protocol.record.value.ProcessInstanceRecordValue;
import io.camunda.zeebe.protocol.record.value.VariableRecordValue;
import io.camunda.zeebe.test.util.BrokerClassRuleHelper;
import io.camunda.zeebe.test.util.collection.Maps;
import io.camunda.zeebe.test.util.record.RecordingExporter;
import io.camunda.zeebe.test.util.record.RecordingExporterTestWatcher;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public final class MultiInstanceIncidentTest {

  @ClassRule public static final EngineRule ENGINE = EngineRule.singlePartition();

  private static final String MULTI_TASK_PROCESS = "multi-task-process";
  private static final String MULTI_SUB_PROC_PROCESS = "multi-sub-process-process";
  private static final String ELEMENT_ID = "task";
  private static final String INPUT_COLLECTION = "items";
  private static final String INPUT_ELEMENT = "item";

  @Rule
  public final RecordingExporterTestWatcher recordingExporterTestWatcher =
      new RecordingExporterTestWatcher();

  @Rule public final BrokerClassRuleHelper helper = new BrokerClassRuleHelper();
  private String jobType;

  @Before
  public void init() {
    jobType = helper.getJobType();
    ENGINE
        .deployment()
        .withXmlResource(
            Bpmn.createExecutableProcess(MULTI_TASK_PROCESS)
                .startEvent()
                .serviceTask(
                    ELEMENT_ID,
                    t ->
                        t.zeebeJobType(jobType)
                            .multiInstance(
                                b ->
                                    b.zeebeInputCollectionExpression(INPUT_COLLECTION)
                                        .zeebeInputElement(INPUT_ELEMENT)
                                        .zeebeOutputElementExpression("{x: undefined_var}")
                                        .zeebeOutputCollection("results")))
                .endEvent()
                .done())
        .deploy();
  }

  @Test
  public void shouldCreateIncidentIfInputVariableNotFound() {
    // when
    final long processInstanceKey =
        ENGINE.processInstance().ofBpmnProcessId(MULTI_TASK_PROCESS).create();

    // then
    final Record<IncidentRecordValue> incident =
        RecordingExporter.incidentRecords(IncidentIntent.CREATED)
            .withProcessInstanceKey(processInstanceKey)
            .getFirst();

    final Record<ProcessInstanceRecordValue> elementInstance =
        RecordingExporter.processInstanceRecords(ProcessInstanceIntent.ELEMENT_ACTIVATING)
            .withProcessInstanceKey(processInstanceKey)
            .withElementId(ELEMENT_ID)
            .getFirst();

    Assertions.assertThat(incident.getValue())
        .hasElementInstanceKey(elementInstance.getKey())
        .hasElementId(elementInstance.getValue().getElementId())
        .hasErrorType(ErrorType.EXTRACT_VALUE_ERROR)
        .hasErrorMessage(
            "failed to evaluate expression '"
                + INPUT_COLLECTION
                + "': no variable found for name '"
                + INPUT_COLLECTION
                + "'");
  }

  @Test
  public void shouldCreateIncidentIfInputVariableIsNotAnArray() {
    // when
    final long processInstanceKey =
        ENGINE
            .processInstance()
            .ofBpmnProcessId(MULTI_TASK_PROCESS)
            .withVariable(INPUT_COLLECTION, "not-an-array-but-a-string")
            .create();

    // then
    final Record<IncidentRecordValue> incident =
        RecordingExporter.incidentRecords(IncidentIntent.CREATED)
            .withProcessInstanceKey(processInstanceKey)
            .getFirst();

    final Record<ProcessInstanceRecordValue> elementInstance =
        RecordingExporter.processInstanceRecords(ProcessInstanceIntent.ELEMENT_ACTIVATING)
            .withProcessInstanceKey(processInstanceKey)
            .withElementId(ELEMENT_ID)
            .getFirst();

    Assertions.assertThat(incident.getValue())
        .hasElementInstanceKey(elementInstance.getKey())
        .hasElementId(elementInstance.getValue().getElementId())
        .hasErrorType(ErrorType.EXTRACT_VALUE_ERROR)
        .hasErrorMessage(
            "Expected result of the expression '"
                + INPUT_COLLECTION
                + "' to be 'ARRAY', but was 'STRING'.");
  }

  @Test
  public void shouldCreateIncidentIfOutputElementExpressionEvaluationFailed() {
    // given
    final long processInstanceKey =
        ENGINE
            .processInstance()
            .ofBpmnProcessId(MULTI_TASK_PROCESS)
            .withVariable(INPUT_COLLECTION, List.of(1, 2, 3))
            .create();

    // when
    ENGINE.job().withType(jobType).ofInstance(processInstanceKey).complete();

    // then
    final Record<IncidentRecordValue> incident =
        RecordingExporter.incidentRecords(IncidentIntent.CREATED)
            .withProcessInstanceKey(processInstanceKey)
            .getFirst();

    final Record<ProcessInstanceRecordValue> elementInstance =
        RecordingExporter.processInstanceRecords(ProcessInstanceIntent.ELEMENT_COMPLETING)
            .withProcessInstanceKey(processInstanceKey)
            .withElementId(ELEMENT_ID)
            .getFirst();

    Assertions.assertThat(incident.getValue())
        .hasElementInstanceKey(elementInstance.getKey())
        .hasElementId(elementInstance.getValue().getElementId())
        .hasErrorType(ErrorType.EXTRACT_VALUE_ERROR)
        .hasErrorMessage(
            "failed to evaluate expression '{x: undefined_var}': "
                + "no variable found for name 'undefined_var'");
  }

  @Test
  public void shouldCollectOutputResultsForResolvedIncidentOfOutputElementExpression() {
    // given an instance of a process with an output mapping referring to a variable 'undefined_var'
    final long processInstanceKey =
        ENGINE
            .processInstance()
            .ofBpmnProcessId(MULTI_TASK_PROCESS)
            .withVariable(INPUT_COLLECTION, List.of(1, 2, 3))
            .create();

    completeNthJob(processInstanceKey, 1);

    // an incident is created because the variable `undefined_var` is missing
    final Record<IncidentRecordValue> incident =
        RecordingExporter.incidentRecords(IncidentIntent.CREATED)
            .withProcessInstanceKey(processInstanceKey)
            .getFirst();

    // when the missing variable is provided
    ENGINE
        .variables()
        .ofScope(processInstanceKey)
        .withDocument(Maps.of(entry("undefined_var", 1)))
        .update();

    // and we resolve the incident
    ENGINE.incident().ofInstance(processInstanceKey).withKey(incident.getKey()).resolve();

    // and complete the other jobs
    completeNthJob(processInstanceKey, 2);
    completeNthJob(processInstanceKey, 3);

    // then the process is able to complete
    assertThat(
            RecordingExporter.processInstanceRecords(ProcessInstanceIntent.ELEMENT_COMPLETED)
                .withElementType(BpmnElementType.PROCESS)
                .withProcessInstanceKey(processInstanceKey)
                .limitToProcessInstanceCompleted()
                .exists())
        .describedAs("the process has completed")
        .isTrue();

    // and all results can be collected
    // note, that this failed in a bug where the task was completed at the same time the incident
    // was created. If the problem was resolved and the other tasks completed, the multi instance
    // would still complete normally, but would not have collected the output of the first task.
    // for more information see: https://github.com/camunda-cloud/zeebe/issues/6546
    assertThat(
            RecordingExporter.variableRecords()
                .withProcessInstanceKey(processInstanceKey)
                .withName("results")
                .limit(4)
                .getLast())
        .extracting(Record::getValue)
        .extracting(VariableRecordValue::getValue)
        .describedAs("the results have been collected")
        .isEqualTo("[{\"x\":1},{\"x\":1},{\"x\":1}]");
  }

  @Test
  public void shouldResolveIncident() {
    // given
    final long processInstanceKey =
        ENGINE.processInstance().ofBpmnProcessId(MULTI_TASK_PROCESS).create();

    final Record<IncidentRecordValue> incident =
        RecordingExporter.incidentRecords(IncidentIntent.CREATED)
            .withProcessInstanceKey(processInstanceKey)
            .getFirst();

    // when
    ENGINE
        .variables()
        .ofScope(incident.getValue().getVariableScopeKey())
        .withDocument(Collections.singletonMap(INPUT_COLLECTION, Arrays.asList(10, 20, 30)))
        .update();

    ENGINE.incident().ofInstance(processInstanceKey).withKey(incident.getKey()).resolve();

    // then
    assertThat(
            RecordingExporter.processInstanceRecords()
                .withRecordKey(incident.getValue().getElementInstanceKey())
                .limit(3))
        .extracting(Record::getIntent)
        .contains(ProcessInstanceIntent.ELEMENT_ACTIVATED);
  }

  @Test
  public void shouldUseTheSameLoopVariablesWhenIncidentResolved() {
    // given
    ENGINE
        .deployment()
        .withXmlResource(
            Bpmn.createExecutableProcess(MULTI_SUB_PROC_PROCESS)
                .startEvent()
                .subProcess("sub-process")
                .zeebeInputExpression("y", "y")
                .multiInstance(
                    b ->
                        b.parallel()
                            .zeebeInputCollectionExpression(INPUT_COLLECTION)
                            .zeebeInputElement(INPUT_ELEMENT))
                .embeddedSubProcess()
                .startEvent("sub-process-start")
                .endEvent("sub-process-end")
                .moveToNode("sub-process")
                .endEvent()
                .done())
        .deploy();
    final var processInstanceKey =
        ENGINE
            .processInstance()
            .ofBpmnProcessId(MULTI_SUB_PROC_PROCESS)
            .withVariables("{\"items\":[1,2,3]}")
            .create();

    // when
    final List<Long> incidents =
        RecordingExporter.incidentRecords(IncidentIntent.CREATED)
            .withProcessInstanceKey(processInstanceKey)
            .limit(3)
            .map(Record::getKey)
            .collect(Collectors.toList());
    ENGINE.variables().ofScope(processInstanceKey).withDocument(Map.of("y", 1)).update();
    incidents.stream()
        .map(key -> ENGINE.incident().ofInstance(processInstanceKey).withKey(key))
        .forEach(ResolveIncidentClient::resolve);

    // then
    final var variableNames = Set.of("item", "loopCounter");
    assertThat(
            RecordingExporter.records()
                .limitToProcessInstance(processInstanceKey)
                .variableRecords()
                .filter(v -> variableNames.contains(v.getValue().getName())))
        .extracting(v -> tuple(v.getIntent(), v.getValue().getName(), v.getValue().getValue()))
        .containsExactly(
            tuple(VariableIntent.CREATED, "item", "1"),
            tuple(VariableIntent.CREATED, "loopCounter", "1"),
            tuple(VariableIntent.CREATED, "item", "2"),
            tuple(VariableIntent.CREATED, "loopCounter", "2"),
            tuple(VariableIntent.CREATED, "item", "3"),
            tuple(VariableIntent.CREATED, "loopCounter", "3"));
  }

  @Test
  public void shouldResolveIncidentDueToInputCollection() {
    // given
    ENGINE
        .deployment()
        .withXmlResource(
            Bpmn.createExecutableProcess("multi-task")
                .startEvent()
                .serviceTask(
                    ELEMENT_ID,
                    t ->
                        t.zeebeJobType(jobType)
                            .multiInstance(
                                b ->
                                    b.sequential()
                                        .zeebeInputCollectionExpression(INPUT_COLLECTION)
                                        .zeebeInputElement(INPUT_ELEMENT)))
                .endEvent()
                .done())
        .deploy();

    final var processInstanceKey =
        ENGINE
            .processInstance()
            .ofBpmnProcessId("multi-task")
            .withVariable(INPUT_COLLECTION, List.of(1, 2, 3))
            .create();

    final var activatedTask =
        RecordingExporter.processInstanceRecords(ProcessInstanceIntent.ELEMENT_ACTIVATED)
            .withProcessInstanceKey(processInstanceKey)
            .withElementId(ELEMENT_ID)
            .getFirst();

    ENGINE
        .variables()
        .ofScope(activatedTask.getKey())
        .withDocument(Collections.singletonMap(INPUT_COLLECTION, "not a list"))
        .update();

    completeNthJob(processInstanceKey, 1);

    final var incident =
        RecordingExporter.incidentRecords(IncidentIntent.CREATED)
            .withProcessInstanceKey(processInstanceKey)
            .getFirst();

    // when
    ENGINE
        .variables()
        .ofScope(activatedTask.getKey())
        .withDocument(Collections.singletonMap(INPUT_COLLECTION, List.of(1, 2, 3)))
        .update();

    ENGINE.incident().ofInstance(processInstanceKey).withKey(incident.getKey()).resolve();

    // then
    completeNthJob(processInstanceKey, 2);
    completeNthJob(processInstanceKey, 3);

    RecordingExporter.processInstanceRecords(ProcessInstanceIntent.ELEMENT_COMPLETED)
        .withProcessInstanceKey(processInstanceKey)
        .withElementType(BpmnElementType.PROCESS)
        .limitToProcessInstanceCompleted()
        .await();
  }

  /**
   * This test is a bit more complex then shouldResolveIncidentDueToInputCollection, because it
   * tests a parallel multi-instance body that is about to activate, but while it's activating (and
   * before it's children activate) the input collection is modified. This should result in
   * incidents on each of the children's activations, which can be resolved individually.
   */
  @Test
  public void shouldCreateIncidentWhenInputCollectionModifiedConcurrently() {
    // given
    final var process =
        Bpmn.createExecutableProcess("multi-task")
            .startEvent()
            .serviceTask(ELEMENT_ID, t -> t.zeebeJobType(jobType))
            .sequenceFlowId("from-task-to-multi-instance")
            .serviceTask("multi-instance", t -> t.zeebeJobType(jobType))
            .multiInstance(
                b ->
                    b.parallel()
                        .zeebeInputCollectionExpression(INPUT_COLLECTION)
                        .zeebeInputElement(INPUT_ELEMENT))
            .endEvent()
            .done();
    final var deployment = ENGINE.deployment().withXmlResource(process).deploy();
    final long processInstanceKey =
        ENGINE
            .processInstance()
            .ofBpmnProcessId("multi-task")
            .withVariable(INPUT_COLLECTION, List.of(1, 2, 3))
            .create();
    final var serviceTask =
        RecordingExporter.processInstanceRecords(ProcessInstanceIntent.ELEMENT_ACTIVATED)
            .withProcessInstanceKey(processInstanceKey)
            .withElementType(BpmnElementType.SERVICE_TASK)
            .getFirst();
    final var job = findNthJob(processInstanceKey, 1);
    final var nextKey = ENGINE.getZeebeState().getKeyGenerator().nextKey();
    ENGINE.stop();
    RecordingExporter.reset();

    // when
    final ProcessInstanceRecord sequenceFlow =
        Records.processInstance(processInstanceKey, "multi-task")
            .setBpmnElementType(BpmnElementType.SEQUENCE_FLOW)
            .setElementId("from-task-to-multi-instance")
            .setFlowScopeKey(processInstanceKey)
            .setProcessDefinitionKey(
                deployment.getValue().getProcessesMetadata().get(0).getProcessDefinitionKey());
    final ProcessInstanceRecord multiInstanceBody =
        Records.processInstance(processInstanceKey, "multi-task")
            .setBpmnElementType(BpmnElementType.MULTI_INSTANCE_BODY)
            .setElementId("multi-instance")
            .setFlowScopeKey(processInstanceKey)
            .setProcessDefinitionKey(
                deployment.getValue().getProcessesMetadata().get(0).getProcessDefinitionKey());

    ENGINE.writeRecords(
        RecordToWrite.command().key(job.getKey()).job(JobIntent.COMPLETE, job.getValue()),
        RecordToWrite.event()
            .causedBy(0)
            .key(job.getKey())
            .job(JobIntent.COMPLETED, job.getValue()),
        RecordToWrite.command()
            .causedBy(0)
            .key(serviceTask.getKey())
            .processInstance(ProcessInstanceIntent.COMPLETE_ELEMENT, serviceTask.getValue()),
        RecordToWrite.event()
            .causedBy(2)
            .key(serviceTask.getKey())
            .processInstance(ProcessInstanceIntent.ELEMENT_COMPLETING, serviceTask.getValue()),
        RecordToWrite.event()
            .causedBy(2)
            .key(serviceTask.getKey())
            .processInstance(ProcessInstanceIntent.ELEMENT_COMPLETED, serviceTask.getValue()),
        RecordToWrite.event()
            .causedBy(2)
            .key(nextKey)
            .processInstance(ProcessInstanceIntent.SEQUENCE_FLOW_TAKEN, sequenceFlow),
        RecordToWrite.command()
            .causedBy(2)
            .processInstance(ProcessInstanceIntent.ACTIVATE_ELEMENT, multiInstanceBody),
        RecordToWrite.command()
            .variable(
                VariableDocumentIntent.UPDATE,
                Records.variableDocument(processInstanceKey, "{\"items\":0}")));
    ENGINE.start();

    // then
    final var incidents =
        RecordingExporter.incidentRecords(IncidentIntent.CREATED)
            .withProcessInstanceKey(processInstanceKey)
            .limit(3)
            .asList();
    assertThat(incidents)
        .describedAs(
            "Should create incident for each child when input element cannot be retrieved from input collection")
        .extracting(Record::getValue)
        .extracting(
            IncidentRecordValue::getElementId,
            IncidentRecordValue::getErrorType,
            IncidentRecordValue::getErrorMessage)
        .containsOnly(
            tuple(
                "multi-instance",
                ErrorType.EXTRACT_VALUE_ERROR,
                "Expected result of the expression 'items' to be 'ARRAY', but was 'NUMBER'."));

    ENGINE
        .variables()
        .ofScope(processInstanceKey)
        .withDocument(Collections.singletonMap(INPUT_COLLECTION, List.of(1, 2, 3)))
        .update();

    incidents.forEach(
        i -> ENGINE.incident().ofInstance(processInstanceKey).withKey(i.getKey()).resolve());

    completeNthJob(processInstanceKey, 2);
    completeNthJob(processInstanceKey, 3);
    completeNthJob(processInstanceKey, 4);

    RecordingExporter.processInstanceRecords(ProcessInstanceIntent.ELEMENT_COMPLETED)
        .withProcessInstanceKey(processInstanceKey)
        .withElementType(BpmnElementType.PROCESS)
        .await();
  }

  @Test
  public void shouldCreateIncidentIfCompletionConditionEvaluationFailed() {
    // given
    ENGINE
        .deployment()
        .withXmlResource(
            Bpmn.createExecutableProcess("multi-task")
                .startEvent()
                .serviceTask(
                    ELEMENT_ID,
                    t ->
                        t.zeebeJobType(jobType)
                            .multiInstance(
                                b ->
                                    b.parallel()
                                        .zeebeInputCollectionExpression(INPUT_COLLECTION)
                                        .zeebeInputElement(INPUT_ELEMENT)
                                        .completionCondition("=x")))
                .endEvent()
                .done())
        .deploy();

    // when
    final long processInstanceKey =
        ENGINE
            .processInstance()
            .ofBpmnProcessId("multi-task")
            .withVariable(INPUT_COLLECTION, List.of(1, 2, 3))
            .create();

    completeNthJob(processInstanceKey, 1);

    // then
    final Record<ProcessInstanceRecordValue> activityEvent =
        RecordingExporter.processInstanceRecords()
            .withElementId(ELEMENT_ID)
            .withIntent(ProcessInstanceIntent.ELEMENT_COMPLETING)
            .withProcessInstanceKey(processInstanceKey)
            .getFirst();

    final Record<IncidentRecordValue> incidentEvent =
        RecordingExporter.incidentRecords()
            .withIntent(IncidentIntent.CREATED)
            .withProcessInstanceKey(processInstanceKey)
            .getFirst();

    Assertions.assertThat(incidentEvent.getValue())
        .hasErrorType(ErrorType.EXTRACT_VALUE_ERROR)
        .hasErrorMessage("failed to evaluate expression 'x': no variable found for name 'x'")
        .hasProcessInstanceKey(processInstanceKey)
        .hasElementInstanceKey(activityEvent.getKey())
        .hasVariableScopeKey(activityEvent.getKey());
  }

  @Test
  public void shouldResolveIncidentDueToCompletionCondition() {
    // given
    ENGINE
        .deployment()
        .withXmlResource(
            Bpmn.createExecutableProcess("multi-task")
                .startEvent()
                .serviceTask(
                    ELEMENT_ID,
                    t ->
                        t.zeebeJobType(jobType)
                            .multiInstance(
                                b ->
                                    b.parallel()
                                        .zeebeInputCollectionExpression(INPUT_COLLECTION)
                                        .zeebeInputElement(INPUT_ELEMENT)
                                        .completionCondition("=x")))
                .endEvent()
                .done())
        .deploy();

    final long processInstanceKey =
        ENGINE
            .processInstance()
            .ofBpmnProcessId("multi-task")
            .withVariable(INPUT_COLLECTION, List.of(1, 2, 3))
            .create();

    completeNthJob(processInstanceKey, 1);

    // an incident is created because the variable `x` is missing
    final Record<IncidentRecordValue> incident =
        RecordingExporter.incidentRecords(IncidentIntent.CREATED)
            .withProcessInstanceKey(processInstanceKey)
            .getFirst();

    // when the missing variable is provided
    ENGINE.variables().ofScope(processInstanceKey).withDocument(Maps.of(entry("x", true))).update();

    // and we resolve the incident
    ENGINE.incident().ofInstance(processInstanceKey).withKey(incident.getKey()).resolve();

    // then the process is able to complete
    assertThat(
            RecordingExporter.processInstanceRecords(ProcessInstanceIntent.ELEMENT_COMPLETED)
                .withElementType(BpmnElementType.PROCESS)
                .withProcessInstanceKey(processInstanceKey)
                .limitToProcessInstanceCompleted()
                .exists())
        .describedAs("the process has completed")
        .isTrue();
  }

  private static void completeNthJob(final long processInstanceKey, final int n) {
    final var nthJob = findNthJob(processInstanceKey, n);
    ENGINE.job().withKey(nthJob.getKey()).complete();
  }

  private static Record<JobRecordValue> findNthJob(final long processInstanceKey, final int n) {
    return RecordingExporter.jobRecords(JobIntent.CREATED)
        .withProcessInstanceKey(processInstanceKey)
        .skip(n - 1)
        .getFirst();
  }
}
