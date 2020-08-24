/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */
package org.camunda.optimize.service.es.report.process.single.processinstance.frequency;

import com.google.common.collect.ImmutableList;
import org.assertj.core.groups.Tuple;
import org.camunda.optimize.dto.optimize.query.report.single.configuration.custom_buckets.BucketUnit;
import org.camunda.optimize.dto.optimize.query.report.single.configuration.custom_buckets.CustomBucketDto;
import org.camunda.optimize.dto.optimize.query.report.single.filter.data.date.DurationFilterUnit;
import org.camunda.optimize.dto.optimize.query.report.single.process.ProcessReportDataDto;
import org.camunda.optimize.dto.optimize.query.report.single.process.filter.ProcessFilterDto;
import org.camunda.optimize.dto.optimize.query.report.single.process.filter.util.ProcessFilterBuilder;
import org.camunda.optimize.dto.optimize.query.report.single.process.group.ProcessGroupByType;
import org.camunda.optimize.dto.optimize.query.report.single.process.view.ProcessViewEntity;
import org.camunda.optimize.dto.optimize.query.report.single.process.view.ProcessViewProperty;
import org.camunda.optimize.dto.optimize.query.report.single.result.ReportMapResultDto;
import org.camunda.optimize.dto.optimize.query.report.single.result.hyper.MapResultEntryDto;
import org.camunda.optimize.dto.optimize.query.sorting.ReportSortingDto;
import org.camunda.optimize.dto.optimize.query.sorting.SortOrder;
import org.camunda.optimize.dto.optimize.rest.report.AuthorizedProcessReportEvaluationResultDto;
import org.camunda.optimize.rest.engine.dto.ProcessInstanceEngineDto;
import org.camunda.optimize.service.es.report.process.AbstractProcessDefinitionIT;
import org.camunda.optimize.test.util.DateCreationFreezer;
import org.camunda.optimize.test.util.TemplatedProcessReportDataBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.Lists.newArrayList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.camunda.optimize.dto.optimize.ReportConstants.ALL_VERSIONS;
import static org.camunda.optimize.dto.optimize.query.report.single.filter.data.FilterOperator.GREATER_THAN_EQUALS;
import static org.camunda.optimize.dto.optimize.query.sorting.ReportSortingDto.SORT_BY_KEY;
import static org.camunda.optimize.dto.optimize.query.sorting.ReportSortingDto.SORT_BY_VALUE;
import static org.camunda.optimize.test.util.ProcessReportDataType.COUNT_PROC_INST_FREQ_GROUP_BY_DURATION;
import static org.camunda.optimize.upgrade.es.ElasticsearchConstants.NUMBER_OF_DATA_POINTS_FOR_AUTOMATIC_INTERVAL_SELECTION;

public class CountProcessInstanceFrequencyByDurationReportEvaluationIT extends AbstractProcessDefinitionIT {

  @Test
  public void simpleReportEvaluation() {
    // given
    final ProcessInstanceEngineDto processInstanceDto = deployAndStartSimpleServiceTaskProcess();
    changeProcessInstanceDuration(processInstanceDto, 1000);
    importAllEngineEntitiesFromScratch();

    // when
    final ProcessReportDataDto reportData = createReport(
      processInstanceDto.getProcessDefinitionKey(), processInstanceDto.getProcessDefinitionVersion()
    );
    final AuthorizedProcessReportEvaluationResultDto<ReportMapResultDto> evaluationResponse =
      reportClient.evaluateMapReport(reportData);

    // then
    final ProcessReportDataDto resultReportDataDto = evaluationResponse.getReportDefinition().getData();
    assertThat(resultReportDataDto.getProcessDefinitionKey()).isEqualTo(processInstanceDto.getProcessDefinitionKey());
    assertThat(resultReportDataDto.getDefinitionVersions()).contains(processInstanceDto.getProcessDefinitionVersion());
    assertThat(resultReportDataDto.getView()).isNotNull();
    assertThat(resultReportDataDto.getView().getEntity()).isEqualTo(ProcessViewEntity.PROCESS_INSTANCE);
    assertThat(resultReportDataDto.getView().getProperty()).isEqualTo(ProcessViewProperty.FREQUENCY);
    assertThat(resultReportDataDto.getGroupBy().getType()).isEqualTo(ProcessGroupByType.DURATION);

    final ReportMapResultDto result = evaluationResponse.getResult();
    assertThat(result.getInstanceCount()).isEqualTo(1L);
    assertThat(result.getData())
      .hasSize(1)
      .extracting(MapResultEntryDto::getKey, MapResultEntryDto::getValue)
      .containsExactly(Tuple.tuple(createDurationBucketKey(1000), 1.0D));
  }

  @Test
  public void simpleReportEvaluationById() {
    // given
    final ProcessInstanceEngineDto processInstanceDto = deployAndStartSimpleServiceTaskProcess();
    final int durationInMilliseconds = 1000;
    changeProcessInstanceDuration(processInstanceDto, durationInMilliseconds);
    importAllEngineEntitiesFromScratch();

    final String reportId = createAndStoreDefaultReportDefinition(
      processInstanceDto.getProcessDefinitionKey(), processInstanceDto.getProcessDefinitionVersion()
    );

    // when
    final AuthorizedProcessReportEvaluationResultDto<ReportMapResultDto> evaluationResponse =
      reportClient.evaluateMapReportById(reportId);

    // then
    ProcessReportDataDto resultReportDataDto = evaluationResponse.getReportDefinition().getData();
    assertThat(resultReportDataDto.getProcessDefinitionKey()).isEqualTo(processInstanceDto.getProcessDefinitionKey());
    assertThat(resultReportDataDto.getDefinitionVersions()).contains(processInstanceDto.getProcessDefinitionVersion());
    assertThat(resultReportDataDto.getView()).isNotNull();
    assertThat(resultReportDataDto.getView().getEntity()).isEqualTo(ProcessViewEntity.PROCESS_INSTANCE);
    assertThat(resultReportDataDto.getView().getProperty()).isEqualTo(ProcessViewProperty.FREQUENCY);
    assertThat(resultReportDataDto.getGroupBy().getType()).isEqualTo(ProcessGroupByType.DURATION);

    final ReportMapResultDto result = evaluationResponse.getResult();
    assertThat(result.getInstanceCount()).isEqualTo(1L);
    assertThat(result.getData())
      .hasSize(1)
      .extracting(MapResultEntryDto::getKey, MapResultEntryDto::getValue)
      .containsExactly(Tuple.tuple(createDurationBucketKey(durationInMilliseconds), 1.0D));
  }

  @Test
  public void simpleReportEvaluation_noData() {
    // given
    final ProcessInstanceEngineDto processInstanceDto = deployAndStartSimpleServiceTaskProcess();
    changeProcessInstanceDuration(processInstanceDto, 1000);
    importAllEngineEntitiesFromScratch();

    // when
    final ProcessReportDataDto reportData = createReport(
      processInstanceDto.getProcessDefinitionKey(), processInstanceDto.getProcessDefinitionVersion()
    );
    final List<ProcessFilterDto<?>> durationFilter = ProcessFilterBuilder.filter()
      .duration()
      .operator(GREATER_THAN_EQUALS)
      .unit(DurationFilterUnit.HOURS)
      .value(1L)
      .add()
      .buildList();
    reportData.setFilter(durationFilter);
    final AuthorizedProcessReportEvaluationResultDto<ReportMapResultDto> evaluationResponse =
      reportClient.evaluateMapReport(reportData);

    // then
    final ReportMapResultDto result = evaluationResponse.getResult();
    assertThat(result.getInstanceCount()).isEqualTo(0L);
    assertThat(result.getData()).isEmpty();
  }

  @Test
  public void otherProcessDefinitionsDoNoAffectResult() {
    // given
    final ProcessInstanceEngineDto processInstanceDto = deployAndStartSimpleServiceTaskProcess();
    final int durationInMilliseconds = 1000;
    changeProcessInstanceDuration(processInstanceDto, durationInMilliseconds);
    // create and start another process
    deployAndStartSimpleServiceTaskProcess();
    importAllEngineEntitiesFromScratch();

    // when
    ProcessReportDataDto reportData = createReport(
      processInstanceDto.getProcessDefinitionKey(), processInstanceDto.getProcessDefinitionVersion()
    );
    AuthorizedProcessReportEvaluationResultDto<ReportMapResultDto> evaluationResponse =
      reportClient.evaluateMapReport(reportData);

    // then
    final ReportMapResultDto result = evaluationResponse.getResult();
    assertThat(result.getInstanceCount()).isEqualTo(1L);
    assertThat(result.getData())
      .hasSize(1)
      .extracting(MapResultEntryDto::getKey, MapResultEntryDto::getValue)
      .containsExactly(Tuple.tuple(createDurationBucketKey(durationInMilliseconds), 1.0D));
  }

  @Test
  public void reportEvaluationSingleBucketFilteredBySingleTenant() {
    // given
    final String tenantId1 = "tenantId1";
    final String tenantId2 = "tenantId2";
    final List<String> selectedTenants = newArrayList(tenantId1);
    final String processKey = deployAndStartMultiTenantSimpleServiceTaskProcess(
      newArrayList(null, tenantId1, tenantId2)
    );

    importAllEngineEntitiesFromScratch();

    // when
    final ProcessReportDataDto reportData = createReport(processKey, ALL_VERSIONS);
    reportData.setTenantIds(selectedTenants);
    ReportMapResultDto result = reportClient.evaluateMapReport(reportData).getResult();

    // then
    assertThat(result.getInstanceCount()).isEqualTo(selectedTenants.size());
  }

  @Test
  public void multipleProcessInstances() {
    // given
    final ProcessInstanceEngineDto firstProcessInstance = deployAndStartSimpleServiceTaskProcess();
    changeProcessInstanceDuration(firstProcessInstance, 1000);
    startInstanceAndModifyDuration(firstProcessInstance.getDefinitionId(), 1000);
    importAllEngineEntitiesFromScratch();

    // when
    final ProcessReportDataDto reportData = createReport(
      firstProcessInstance.getProcessDefinitionKey(), firstProcessInstance.getProcessDefinitionVersion()
    );
    AuthorizedProcessReportEvaluationResultDto<ReportMapResultDto> evaluationResponse =
      reportClient.evaluateMapReport(reportData);

    // then
    final ReportMapResultDto resultDto = evaluationResponse.getResult();
    assertThat(resultDto.getIsComplete()).isTrue();
    assertThat(resultDto.getData())
      .hasSize(1)
      .extracting(MapResultEntryDto::getKey, MapResultEntryDto::getValue)
      .contains(
        Tuple.tuple(createDurationBucketKey(1000), 2.0D)
      );
  }

  @Test
  public void multipleProcessInstances_runningInstanceDurationIsCalculated() {
    // given
    final OffsetDateTime startTime = DateCreationFreezer.dateFreezer(OffsetDateTime.now()).freezeDateAndReturn();
    final ProcessInstanceEngineDto completedProcessInstance = deployAndStartSimpleUserTaskProcess();
    engineIntegrationExtension.completeUserTaskWithoutClaim(completedProcessInstance.getId());
    changeProcessInstanceDuration(completedProcessInstance, 1000);

    final ProcessInstanceEngineDto runningProcessInstance = engineIntegrationExtension
      .startProcessInstance(completedProcessInstance.getDefinitionId());
    engineDatabaseExtension.changeProcessInstanceStartDate(runningProcessInstance.getId(), startTime);
    importAllEngineEntitiesFromScratch();

    // when
    final OffsetDateTime currentTime = DateCreationFreezer.dateFreezer(startTime.plusSeconds(5)).freezeDateAndReturn();
    final ProcessReportDataDto reportData = createReport(
      completedProcessInstance.getProcessDefinitionKey(), completedProcessInstance.getProcessDefinitionVersion()
    );
    AuthorizedProcessReportEvaluationResultDto<ReportMapResultDto> evaluationResponse =
      reportClient.evaluateMapReport(reportData);

    // then
    final ReportMapResultDto resultDto = evaluationResponse.getResult();
    assertThat(resultDto.getIsComplete()).isTrue();
    assertThat(resultDto.getData())
      // we expect buckets from 1000ms (finished instance) to 5000ms (running instance in relation to currentTime)
      // in intervals of 100ms (interval us rounded up to nearest power of 10)
      .hasSize(41)
      .extracting(MapResultEntryDto::getKey, MapResultEntryDto::getValue)
      .contains(
        Tuple.tuple(createDurationBucketKey(1000), 1.0D),
        Tuple.tuple(createDurationBucketKey((int) Duration.between(startTime, currentTime).toMillis()), 1.0D)
      );
  }

  @Test
  public void multipleBuckets_singleValueBucket() {
    // given
    final ProcessInstanceEngineDto firstProcessInstance = deployAndStartSimpleServiceTaskProcess();
    changeProcessInstanceDuration(firstProcessInstance, 1000);
    startInstanceAndModifyDuration(firstProcessInstance.getDefinitionId(), 1002);
    startInstanceAndModifyDuration(firstProcessInstance.getDefinitionId(), 1070);
    startInstanceAndModifyDuration(firstProcessInstance.getDefinitionId(), 1079);
    importAllEngineEntitiesFromScratch();

    // when
    final ProcessReportDataDto reportData = createReport(
      firstProcessInstance.getProcessDefinitionKey(), firstProcessInstance.getProcessDefinitionVersion()
    );
    AuthorizedProcessReportEvaluationResultDto<ReportMapResultDto> evaluationResponse =
      reportClient.evaluateMapReport(reportData);

    // then
    final ReportMapResultDto resultDto = evaluationResponse.getResult();
    assertThat(resultDto.getIsComplete()).isTrue();
    assertThat(resultDto.getData())
      .isNotNull()
      // if the data range fits into the default max bucket number of 80, we should see a bucket for each value
      .hasSize(NUMBER_OF_DATA_POINTS_FOR_AUTOMATIC_INTERVAL_SELECTION)
      .isSortedAccordingTo(Comparator.comparing(byDurationEntry -> Double.valueOf(byDurationEntry.getKey())))
      .extracting(MapResultEntryDto::getKey, MapResultEntryDto::getValue)
      .contains(
        Tuple.tuple(createDurationBucketKey(1000), 1.0D),
        Tuple.tuple(createDurationBucketKey(1002), 1.0D),
        Tuple.tuple(createDurationBucketKey(1070), 1.0D),
        Tuple.tuple(createDurationBucketKey(1079), 1.0D)
      );
  }

  @Test
  public void multipleBuckets_automaticRangeBucketsBaseOf10Start() {
    // given
    final ProcessInstanceEngineDto firstProcessInstance = deployAndStartSimpleServiceTaskProcess();
    changeProcessInstanceDuration(firstProcessInstance, 1401);
    startInstanceAndModifyDuration(firstProcessInstance.getDefinitionId(), 2004);
    importAllEngineEntitiesFromScratch();

    // when
    final ProcessReportDataDto reportData = createReport(
      firstProcessInstance.getProcessDefinitionKey(), firstProcessInstance.getProcessDefinitionVersion()
    );
    AuthorizedProcessReportEvaluationResultDto<ReportMapResultDto> evaluationResponse =
      reportClient.evaluateMapReport(reportData);

    // then
    final ReportMapResultDto resultDto = evaluationResponse.getResult();
    assertThat(resultDto.getIsComplete()).isTrue();
    assertThat(resultDto.getData())
      .isNotNull()
      // buckets from 1000ms (nearest lower power of 10 to min value) to 2000ms (start and end inclusive)
      // in intervals of 100ms (nearest power of 10 interval for this range)
      .hasSize(11)
      .isSortedAccordingTo(Comparator.comparing(byDurationEntry -> Double.valueOf(byDurationEntry.getKey())))
      .extracting(MapResultEntryDto::getKey, MapResultEntryDto::getValue)
      .contains(
        Tuple.tuple(createDurationBucketKey(1400), 1.0D),
        Tuple.tuple(createDurationBucketKey(2000), 1.0D)
      );
  }

  @Test
  public void multipleBuckets_customBuckets() {
    // given
    final ProcessInstanceEngineDto firstProcessInstance = deployAndStartSimpleServiceTaskProcess();
    changeProcessInstanceDuration(firstProcessInstance, 100);
    startInstanceAndModifyDuration(firstProcessInstance.getDefinitionId(), 200);
    startInstanceAndModifyDuration(firstProcessInstance.getDefinitionId(), 300);
    importAllEngineEntitiesFromScratch();

    // when
    final ProcessReportDataDto reportData = createReport(
      firstProcessInstance.getProcessDefinitionKey(), firstProcessInstance.getProcessDefinitionVersion()
    );
    reportData.getConfiguration().setCustomBucket(
      CustomBucketDto.builder()
        .active(true)
        .baseline(10.0D)
        .bucketSize(100.0D)
        .build()
    );
    AuthorizedProcessReportEvaluationResultDto<ReportMapResultDto> evaluationResponse =
      reportClient.evaluateMapReport(reportData);

    // then
    final ReportMapResultDto resultDto = evaluationResponse.getResult();
    assertThat(resultDto.getIsComplete()).isTrue();
    assertThat(resultDto.getData())
      .isNotNull()
      .hasSize(3)
      .extracting(MapResultEntryDto::getKey, MapResultEntryDto::getValue)
      .contains(
        Tuple.tuple(createDurationBucketKey(10), 1.0D),
        Tuple.tuple(createDurationBucketKey(110), 1.0D),
        Tuple.tuple(createDurationBucketKey(210), 1.0D)
      );
  }

  private static Stream<Arguments> getCustomBucketUnitScenarios() {
    return Stream.of(
      Arguments.of(BucketUnit.MILLISECOND, Duration.ofMillis(1)),
      Arguments.of(BucketUnit.SECOND, Duration.ofSeconds(1)),
      Arguments.of(BucketUnit.MINUTE, Duration.ofMinutes(1)),
      Arguments.of(BucketUnit.HOUR, Duration.ofHours(1)),
      Arguments.of(BucketUnit.DAY, Duration.ofDays(1)),
      Arguments.of(BucketUnit.WEEK, ChronoUnit.WEEKS.getDuration()),
      Arguments.of(BucketUnit.MONTH, ChronoUnit.MONTHS.getDuration()),
      Arguments.of(BucketUnit.YEAR, ChronoUnit.YEARS.getDuration())
    );
  }

  @ParameterizedTest
  @MethodSource("getCustomBucketUnitScenarios")
  public void multipleBuckets_customBuckets_customBucketUnit(final BucketUnit unit,
                                                             final Duration baseDuration) {
    // given
    final ProcessInstanceEngineDto firstProcessInstance = deployAndStartSimpleServiceTaskProcess();
    changeProcessInstanceDuration(firstProcessInstance, baseDuration.toMillis());
    startInstanceAndModifyDuration(firstProcessInstance.getDefinitionId(), baseDuration.multipliedBy(2).toMillis());
    startInstanceAndModifyDuration(firstProcessInstance.getDefinitionId(), baseDuration.multipliedBy(3).toMillis());
    importAllEngineEntitiesFromScratch();

    // when
    final ProcessReportDataDto reportData = createReport(
      firstProcessInstance.getProcessDefinitionKey(), firstProcessInstance.getProcessDefinitionVersion()
    );
    reportData.getConfiguration().setCustomBucket(
      CustomBucketDto.builder()
        .active(true)
        .baseline(1.0D)
        .baselineUnit(unit)
        .bucketSize(1.0D)
        .bucketSizeUnit(unit)
        .build()
    );
    AuthorizedProcessReportEvaluationResultDto<ReportMapResultDto> evaluationResponse =
      reportClient.evaluateMapReport(reportData);

    // then
    final ReportMapResultDto resultDto = evaluationResponse.getResult();
    assertThat(resultDto.getData())
      .isNotNull()
      .hasSize(3)
      .extracting(MapResultEntryDto::getKey)
      .containsExactlyElementsOf(
        ImmutableList.of(1, 2, 3).stream()
          .map(bucketInUnit -> baseDuration.multipliedBy(bucketInUnit).toMillis())
          .map(this::createDurationBucketKey)
          .collect(Collectors.toList())
      );
  }

  @Test
  public void multipleBuckets_customBuckets_baseLine_biggerThanMax_returnsEmptyResult() {
    // given
    final ProcessInstanceEngineDto firstProcessInstance = deployAndStartSimpleServiceTaskProcess();
    changeProcessInstanceDuration(firstProcessInstance, 100);
    startInstanceAndModifyDuration(firstProcessInstance.getDefinitionId(), 200);
    startInstanceAndModifyDuration(firstProcessInstance.getDefinitionId(), 300);
    importAllEngineEntitiesFromScratch();

    // when
    final ProcessReportDataDto reportData = createReport(
      firstProcessInstance.getProcessDefinitionKey(), firstProcessInstance.getProcessDefinitionVersion()
    );
    reportData.getConfiguration().setCustomBucket(
      CustomBucketDto.builder()
        .active(true)
        .baseline(1000.0D)
        .bucketSize(100.0D)
        .build()
    );
    AuthorizedProcessReportEvaluationResultDto<ReportMapResultDto> evaluationResponse =
      reportClient.evaluateMapReport(reportData);

    // then
    final ReportMapResultDto resultDto = evaluationResponse.getResult();
    assertThat(resultDto.getIsComplete()).isFalse();
    assertThat(resultDto.getData()).isNotNull().isEmpty();
  }

  @Test
  public void multipleBuckets_customBuckets_tooManyBuckets_returnsLimitedResult() {
    // given
    final ProcessInstanceEngineDto firstProcessInstance = deployAndStartSimpleServiceTaskProcess();
    changeProcessInstanceDuration(firstProcessInstance, 1);
    startInstanceAndModifyDuration(firstProcessInstance.getDefinitionId(), 2000);
    startInstanceAndModifyDuration(firstProcessInstance.getDefinitionId(), 3000);
    importAllEngineEntitiesFromScratch();

    // when
    final ProcessReportDataDto reportData = createReport(
      firstProcessInstance.getProcessDefinitionKey(), firstProcessInstance.getProcessDefinitionVersion()
    );
    reportData.getConfiguration().setCustomBucket(
      CustomBucketDto.builder()
        .active(true)
        .baseline(0.0D)
        .bucketSize(1.0D)
        .build()
    );
    AuthorizedProcessReportEvaluationResultDto<ReportMapResultDto> evaluationResponse =
      reportClient.evaluateMapReport(reportData);

    // then
    final ReportMapResultDto resultDto = evaluationResponse.getResult();
    assertThat(resultDto.getIsComplete()).isFalse();
    assertThat(resultDto.getData())
      .isNotNull()
      .hasSize(1000)
      .extracting(MapResultEntryDto::getKey, MapResultEntryDto::getValue)
      .contains(
        Tuple.tuple(createDurationBucketKey(1), 1.0D)
      );
  }

  @Test
  public void multipleBuckets_defaultSorting() {
    // given
    final ProcessInstanceEngineDto firstProcessInstance = deployAndStartSimpleServiceTaskProcess();
    changeProcessInstanceDuration(firstProcessInstance, 900);
    startInstanceAndModifyDuration(firstProcessInstance.getDefinitionId(), 1000);
    startInstanceAndModifyDuration(firstProcessInstance.getDefinitionId(), 1000);
    startInstanceAndModifyDuration(firstProcessInstance.getDefinitionId(), 1100);
    startInstanceAndModifyDuration(firstProcessInstance.getDefinitionId(), 2000);
    importAllEngineEntitiesFromScratch();

    // when
    final ProcessReportDataDto reportData = createReport(
      firstProcessInstance.getProcessDefinitionKey(), firstProcessInstance.getProcessDefinitionVersion()
    );
    AuthorizedProcessReportEvaluationResultDto<ReportMapResultDto> evaluationResponse =
      reportClient.evaluateMapReport(reportData);

    // then
    final ReportMapResultDto resultDto = evaluationResponse.getResult();
    assertThat(resultDto.getIsComplete()).isTrue();
    assertThat(resultDto.getData())
      .hasSize(20)
      .isSortedAccordingTo(Comparator.comparing(byDurationEntry -> Double.valueOf(byDurationEntry.getKey())))
      .extracting(MapResultEntryDto::getKey, MapResultEntryDto::getValue)
      .contains(
        Tuple.tuple(createDurationBucketKey(900), 1.0D),
        Tuple.tuple(createDurationBucketKey(1000), 2.0D),
        Tuple.tuple(createDurationBucketKey(1100), 1.0D),
        Tuple.tuple(createDurationBucketKey(2000), 1.0D)
      );
  }

  @Test
  public void multipleBuckets_customKeySorting() {
    // given
    final ProcessInstanceEngineDto firstProcessInstance = deployAndStartSimpleServiceTaskProcess();
    changeProcessInstanceDuration(firstProcessInstance, 10);
    startInstanceAndModifyDuration(firstProcessInstance.getDefinitionId(), 11);
    importAllEngineEntitiesFromScratch();

    // when
    final ProcessReportDataDto reportData = createReport(
      firstProcessInstance.getProcessDefinitionKey(), firstProcessInstance.getProcessDefinitionVersion()
    );
    reportData.getConfiguration().setSorting(new ReportSortingDto(SORT_BY_KEY, SortOrder.DESC));
    AuthorizedProcessReportEvaluationResultDto<ReportMapResultDto> evaluationResponse =
      reportClient.evaluateMapReport(reportData);

    // then
    final ReportMapResultDto resultDto = evaluationResponse.getResult();
    assertThat(resultDto.getIsComplete()).isTrue();
    assertThat(resultDto.getData())
      .hasSize(2)
      .isSortedAccordingTo(
        Comparator.<MapResultEntryDto, Double>comparing(byDurationEntry -> Double.valueOf(byDurationEntry.getKey()))
          .reversed()
      )
      .extracting(MapResultEntryDto::getKey, MapResultEntryDto::getValue)
      .containsSequence(
        Tuple.tuple(createDurationBucketKey(11), 1.0D),
        Tuple.tuple(createDurationBucketKey(10), 1.0D)
      );
  }

  @Test
  public void multipleBuckets_valueSorting() {
    // given
    final ProcessInstanceEngineDto firstProcessInstance = deployAndStartSimpleServiceTaskProcess();
    changeProcessInstanceDuration(firstProcessInstance, 10);
    startInstanceAndModifyDuration(firstProcessInstance.getDefinitionId(), 10);
    startInstanceAndModifyDuration(firstProcessInstance.getDefinitionId(), 11);
    importAllEngineEntitiesFromScratch();

    // when
    final ProcessReportDataDto reportData = createReport(
      firstProcessInstance.getProcessDefinitionKey(), firstProcessInstance.getProcessDefinitionVersion()
    );
    reportData.getConfiguration().setSorting(new ReportSortingDto(SORT_BY_VALUE, SortOrder.DESC));
    AuthorizedProcessReportEvaluationResultDto<ReportMapResultDto> evaluationResponse =
      reportClient.evaluateMapReport(reportData);

    // then
    final ReportMapResultDto resultDto = evaluationResponse.getResult();
    assertThat(resultDto.getIsComplete()).isTrue();
    assertThat(resultDto.getData())
      .hasSize(2)
      .isSortedAccordingTo(Comparator.comparing(MapResultEntryDto::getValue).reversed())
      .extracting(MapResultEntryDto::getKey, MapResultEntryDto::getValue)
      .containsSequence(
        Tuple.tuple(createDurationBucketKey(10), 2.0D),
        Tuple.tuple(createDurationBucketKey(11), 1.0D)
      );
  }

  private String createDurationBucketKey(final long durationInMs) {
    return Double.valueOf(durationInMs).toString();
  }

  private String createAndStoreDefaultReportDefinition(String processDefinitionKey,
                                                       String processDefinitionVersion) {
    final ProcessReportDataDto reportData = createReport(processDefinitionKey, processDefinitionVersion);
    return createNewReport(reportData);
  }

  private ProcessReportDataDto createReport(final String processKey, final String definitionVersion) {
    return TemplatedProcessReportDataBuilder
      .createReportData()
      .setProcessDefinitionKey(processKey)
      .setProcessDefinitionVersion(definitionVersion)
      .setReportDataType(COUNT_PROC_INST_FREQ_GROUP_BY_DURATION)
      .build();
  }

}
