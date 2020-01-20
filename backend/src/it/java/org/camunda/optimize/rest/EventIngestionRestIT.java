/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */
package org.camunda.optimize.rest;

import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpProcessorBuilder;
import org.apache.http.protocol.RequestTargetHost;
import org.camunda.optimize.AbstractIT;
import org.camunda.optimize.dto.optimize.query.event.EventDto;
import org.camunda.optimize.dto.optimize.rest.CloudEventDto;
import org.camunda.optimize.dto.optimize.rest.ErrorResponseDto;
import org.camunda.optimize.dto.optimize.rest.ValidationErrorResponseDto;
import org.camunda.optimize.jetty.MaxRequestSizeFilter;
import org.camunda.optimize.service.security.util.LocalDateUtil;
import org.camunda.optimize.test.it.extension.IntegrationTestConfigurationUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.camunda.optimize.dto.optimize.rest.CloudEventDto.Fields.id;
import static org.camunda.optimize.dto.optimize.rest.CloudEventDto.Fields.source;
import static org.camunda.optimize.dto.optimize.rest.CloudEventDto.Fields.specversion;
import static org.camunda.optimize.dto.optimize.rest.CloudEventDto.Fields.traceid;
import static org.camunda.optimize.dto.optimize.rest.CloudEventDto.Fields.type;
import static org.camunda.optimize.rest.IngestionRestService.EVENT_BATCH_SUB_PATH;
import static org.camunda.optimize.rest.IngestionRestService.INGESTION_PATH;
import static org.camunda.optimize.rest.IngestionRestService.QUERY_PARAMETER_ACCESS_TOKEN;
import static org.camunda.optimize.rest.providers.BeanConstraintViolationExceptionHandler.THE_REQUEST_BODY_WAS_INVALID;

public class EventIngestionRestIT extends AbstractIT {

  @BeforeEach
  public void before() {
    LocalDateUtil.setCurrentTime(OffsetDateTime.now());
    embeddedOptimizeExtension.getConfigurationService().getEventBasedProcessConfiguration().setEnabled(true);
  }

  @Test
  public void ingestEventBatch() {
    // given
    final List<CloudEventDto> eventDtos = IntStream.range(0, 10)
      .mapToObj(operand -> eventClient.createCloudEventDto())
      .collect(toList());

    // when
    final Response ingestResponse = embeddedOptimizeExtension.getRequestExecutor()
      .buildIngestEventBatch(eventDtos, getAccessToken())
      .execute();

    // then
    assertThat(ingestResponse.getStatus()).isEqualTo(HttpStatus.SC_NO_CONTENT);

    assertEventDtosArePersisted(eventDtos);
  }

  @Test
  public void ingestEventBatch_accessTokenAsQueryParameter() {
    // given
    final List<CloudEventDto> eventDtos = IntStream.range(0, 1)
      .mapToObj(operand -> eventClient.createCloudEventDto())
      .collect(toList());

    // when
    final Response ingestResponse = embeddedOptimizeExtension.getRequestExecutor()
      .buildIngestEventBatch(eventDtos, null)
      .addSingleQueryParam(QUERY_PARAMETER_ACCESS_TOKEN, getAccessToken())
      .execute();

    // then
    assertThat(ingestResponse.getStatus()).isEqualTo(HttpStatus.SC_NO_CONTENT);

    assertEventDtosArePersisted(eventDtos);
  }

  @Test
  public void ingestEventBatch_maxRequestsConfiguredReached() {
    // given
    embeddedOptimizeExtension.getConfigurationService().getEventIngestionConfiguration().setMaxRequests(0);

    final List<CloudEventDto> eventDtos = IntStream.range(0, 1)
      .mapToObj(operand -> eventClient.createCloudEventDto())
      .collect(toList());

    // when
    Response response = embeddedOptimizeExtension.getRequestExecutor()
      .buildIngestEventBatch(eventDtos, null)
      .addSingleQueryParam(QUERY_PARAMETER_ACCESS_TOKEN, getAccessToken())
      .execute();

    // then
    assertThat(response.getStatus()).isEqualTo(HttpStatus.SC_SERVICE_UNAVAILABLE);
  }

  @Test
  public void ingestEventBatch_customSecret() {
    // given
    final CloudEventDto eventDto = eventClient.createCloudEventDto();

    final String customSecret = "mySecret";
    embeddedOptimizeExtension.getConfigurationService().getEventIngestionConfiguration().setAccessToken(customSecret);

    // when
    final Response ingestResponse = embeddedOptimizeExtension.getRequestExecutor()
      .buildIngestEventBatch(Collections.singletonList(eventDto), customSecret)
      .execute();

    // then
    assertThat(ingestResponse.getStatus()).isEqualTo(HttpStatus.SC_NO_CONTENT);

    assertEventDtosArePersisted(Collections.singletonList(eventDto));
  }

  @Test
  public void ingestEventBatch_featureDisabled() {
    // given
    embeddedOptimizeExtension.getConfigurationService().getEventBasedProcessConfiguration().setEnabled(false);

    final List<CloudEventDto> eventDtos = IntStream.range(0, 10)
      .mapToObj(operand -> eventClient.createCloudEventDto())
      .collect(toList());

    // when
    final Response ingestResponse = embeddedOptimizeExtension.getRequestExecutor()
      .buildIngestEventBatch(eventDtos, getAccessToken())
      .execute();

    // then
    assertThat(ingestResponse.getStatus()).isEqualTo(HttpStatus.SC_FORBIDDEN);
  }

  @Test
  public void ingestEventBatch_notAuthorized() {
    // given
    final List<CloudEventDto> eventDtos = IntStream.range(0, 2)
      .mapToObj(operand -> eventClient.createCloudEventDto())
      .collect(toList());

    // when
    final Response ingestResponse = embeddedOptimizeExtension.getRequestExecutor()
      .buildIngestEventBatch(eventDtos, "wroooong")
      .execute();

    // then
    assertThat(ingestResponse.getStatus()).isEqualTo(HttpStatus.SC_UNAUTHORIZED);

    assertEventDtosArePersisted(Collections.emptyList());
  }

  @Test
  public void ingestEventBatch_limitExceeded() {
    // given
    embeddedOptimizeExtension.getConfigurationService().getEventIngestionConfiguration().setMaxBatchRequestBytes(1L);

    final List<CloudEventDto> eventDtos = IntStream.range(0, 2)
      .mapToObj(operand -> eventClient.createCloudEventDto())
      .collect(toList());

    // when
    final ErrorResponseDto ingestResponse = embeddedOptimizeExtension.getRequestExecutor()
      .buildIngestEventBatch(eventDtos, getAccessToken())
      .execute(ErrorResponseDto.class, HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE);

    // then
    assertThat(ingestResponse.getErrorMessage()).contains("Request too large");

    assertEventDtosArePersisted(Collections.emptyList());
  }

  @Test
  public void ingestEventBatch_contentLengthHeaderMissing() throws IOException {
    // this is a custom apache client that does not send the content-length header
    try (final CloseableHttpClient httpClient = HttpClients.custom()
      .setHttpProcessor(HttpProcessorBuilder.create().addAll(new RequestTargetHost()).build())
      .build()) {
      final HttpPut httpPut = new HttpPut(
        IntegrationTestConfigurationUtil.getEmbeddedOptimizeRestApiEndpoint() + INGESTION_PATH + EVENT_BATCH_SUB_PATH
      );
      httpPut.addHeader(HttpHeaders.AUTHORIZATION, getAccessToken());
      final CloseableHttpResponse response = httpClient.execute(httpPut);

      // then
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(HttpStatus.SC_LENGTH_REQUIRED);
      final ErrorResponseDto errorResponseDto = embeddedOptimizeExtension.getObjectMapper()
        .readValue(response.getEntity().getContent(), ErrorResponseDto.class);

      assertThat(errorResponseDto.getErrorMessage()).isEqualTo(MaxRequestSizeFilter.MESSAGE_NO_CONTENT_LENGTH);

      assertEventDtosArePersisted(Collections.emptyList());
    }
  }

  @Test
  public void ingestEventBatch_omitOptionalProperties() {
    // given
    final CloudEventDto eventDto = eventClient.createCloudEventDto();
    eventDto.setGroup(null);
    eventDto.setData(null);
    // time will get dynamically assigned of not present
    LocalDateUtil.setCurrentTime(OffsetDateTime.now());
    eventDto.setTime(null);

    // when
    final Response ingestResponse = embeddedOptimizeExtension.getRequestExecutor()
      .buildIngestEventBatch(Collections.singletonList(eventDto), getAccessToken())
      .execute();

    // then
    assertThat(ingestResponse.getStatus()).isEqualTo(HttpStatus.SC_NO_CONTENT);

    assertEventDtosArePersisted(Collections.singletonList(eventDto));
  }

  @Test
  public void ingestEventBatch_rejectMandatoryPropertiesNull() {
    // given
    final CloudEventDto eventDto = eventClient.createCloudEventDto();
    eventDto.setSpecversion(null);
    eventDto.setId(null);
    eventDto.setType(null);
    eventDto.setSource(null);
    eventDto.setTraceid(null);

    // when
    final ValidationErrorResponseDto ingestErrorResponse = embeddedOptimizeExtension.getRequestExecutor()
      .buildIngestEventBatch(Collections.singletonList(eventDto), getAccessToken())
      .execute(ValidationErrorResponseDto.class, HttpServletResponse.SC_BAD_REQUEST);

    // then
    assertThat(ingestErrorResponse.getErrorMessage()).isEqualTo(THE_REQUEST_BODY_WAS_INVALID);
    assertThat(ingestErrorResponse.getValidationErrors().size()).isEqualTo(5);
    assertThat(
      ingestErrorResponse.getValidationErrors()
        .stream()
        .map(ValidationErrorResponseDto.ValidationError::getProperty)
        .map(property -> property.split("\\.")[1])
        .collect(toList()))
      .contains(specversion, id, type, source, traceid);
    assertThat(
      ingestErrorResponse.getValidationErrors()
        .stream()
        .map(ValidationErrorResponseDto.ValidationError::getErrorMessage)
        .collect(toList())).doesNotContainNull();

    assertEventDtosArePersisted(Collections.emptyList());
  }

  @Test
  public void ingestEventBatch_rejectInvalidPropertyValues() {
    // given
    final CloudEventDto eventDto = eventClient.createCloudEventDto();
    eventDto.setId("  ");
    eventDto.setSpecversion("0");
    eventDto.setType("");
    eventDto.setSource("");
    eventDto.setTraceid("");

    // when
    final ValidationErrorResponseDto ingestErrorResponse = embeddedOptimizeExtension.getRequestExecutor()
      .buildIngestEventBatch(Collections.singletonList(eventDto), getAccessToken())
      .execute(ValidationErrorResponseDto.class, HttpServletResponse.SC_BAD_REQUEST);

    // then
    assertThat(ingestErrorResponse.getErrorMessage()).isEqualTo(THE_REQUEST_BODY_WAS_INVALID);
    assertThat(ingestErrorResponse.getValidationErrors().size()).isEqualTo(5);
    assertThat(
      ingestErrorResponse.getValidationErrors()
        .stream()
        .map(ValidationErrorResponseDto.ValidationError::getProperty)
        .map(property -> property.split("\\.")[1])
        .collect(toList()))
      .contains(id, specversion, type, source, traceid);
    assertThat(
      ingestErrorResponse.getValidationErrors()
        .stream()
        .map(ValidationErrorResponseDto.ValidationError::getErrorMessage)
        .collect(toList())).doesNotContainNull();

    assertEventDtosArePersisted(Collections.emptyList());
  }

  @Test
  public void ingestEventBatch_rejectInvalidPropertyValueOfSpecificEntry() {
    // given
    final List<CloudEventDto> eventDtos = IntStream.range(0, 2)
      .mapToObj(operand -> eventClient.createCloudEventDto())
      .collect(toList());

    final CloudEventDto invalidEventDto1 = eventClient.createCloudEventDto();
    invalidEventDto1.setId(null);
    eventDtos.add(invalidEventDto1);

    // when
    final ValidationErrorResponseDto ingestErrorResponse = embeddedOptimizeExtension.getRequestExecutor()
      .buildIngestEventBatch(eventDtos, getAccessToken())
      .execute(ValidationErrorResponseDto.class, HttpServletResponse.SC_BAD_REQUEST);

    // then
    assertThat(ingestErrorResponse.getErrorMessage()).isEqualTo(THE_REQUEST_BODY_WAS_INVALID);
    assertThat(ingestErrorResponse.getValidationErrors().size()).isEqualTo(1);
    assertThat(
      ingestErrorResponse.getValidationErrors()
        .stream()
        .map(ValidationErrorResponseDto.ValidationError::getProperty)
        .collect(toList()))
      .contains("element[2]." + id);
    assertThat(
      ingestErrorResponse.getValidationErrors()
        .stream()
        .map(ValidationErrorResponseDto.ValidationError::getErrorMessage)
        .collect(toList())).doesNotContainNull();

    assertEventDtosArePersisted(Collections.emptyList());
  }

  private void assertEventDtosArePersisted(final List<CloudEventDto> cloudEventDtos) {
    elasticSearchIntegrationTestExtension.refreshAllOptimizeIndices();
    final List<EventDto> expectedEventDtos = cloudEventDtos.stream()
      .map(cloudEventDto -> EventDto.builder()
        .id(cloudEventDto.getId())
        .eventName(cloudEventDto.getType())
        .timestamp(
          cloudEventDto.getTime()
            .orElse(LocalDateUtil.getCurrentDateTime().toInstant())
            .toEpochMilli()
        )
        .traceId(cloudEventDto.getTraceid())
        .group(cloudEventDto.getGroup().orElse(null))
        .source(cloudEventDto.getSource())
        .data(cloudEventDto.getData().orElse(null))
        .ingestionTimestamp(LocalDateUtil.getCurrentDateTime().toInstant().toEpochMilli())
        .build()
      )
      .collect(Collectors.toList());
    final List<EventDto> indexedEventDtos = eventClient.getAllStoredEvents();
    assertThat(indexedEventDtos).containsExactlyInAnyOrderElementsOf(expectedEventDtos);
  }

  private String getAccessToken() {
    return embeddedOptimizeExtension.getConfigurationService().getEventIngestionConfiguration().getAccessToken();
  }

}
