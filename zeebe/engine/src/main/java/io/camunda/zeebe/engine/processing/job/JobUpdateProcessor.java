/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine.processing.job;

import io.camunda.zeebe.engine.processing.job.behaviour.JobUpdateBehaviour;
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecordProcessor;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.StateWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedRejectionWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedResponseWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.Writers;
import io.camunda.zeebe.protocol.impl.record.value.job.JobRecord;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.intent.JobIntent;
import io.camunda.zeebe.stream.api.records.TypedRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class JobUpdateProcessor implements TypedRecordProcessor<JobRecord> {

  private final JobUpdateBehaviour jobUpdateBehaviour;
  private final TypedRejectionWriter rejectionWriter;
  private final TypedResponseWriter responseWriter;
  private final StateWriter stateWriter;

  public JobUpdateProcessor(final JobUpdateBehaviour jobUpdateBehaviour, final Writers writers) {
    this.jobUpdateBehaviour = jobUpdateBehaviour;
    rejectionWriter = writers.rejection();
    responseWriter = writers.response();
    stateWriter = writers.state();
  }

  @Override
  public void processRecord(final TypedRecord<JobRecord> command) {
    final long jobKey = command.getKey();
    jobUpdateBehaviour
        .getJob(jobKey, command)
        .ifRightOrLeft(
            job -> {
              final List<String> errors = new ArrayList<>();
              final Map<String, Number> changeset = command.getValue().getChangedAttributes();
              jobChange(
                  changeset,
                  JobRecord.RETRIES,
                  Number::intValue,
                  (retries) -> jobUpdateBehaviour.updateJobRetries(jobKey, retries, job, command),
                  errors);
              jobChange(
                  changeset,
                  JobRecord.TIMEOUT,
                  Number::longValue,
                  (timeout) -> jobUpdateBehaviour.updateJobTimeout(jobKey, timeout, job),
                  errors);
              if (errors.isEmpty()) {
                stateWriter.appendFollowUpEvent(jobKey, JobIntent.UPDATED, job);
                responseWriter.writeEventOnCommand(jobKey, JobIntent.UPDATED, job, command);
              } else {
                handleRejection(errors, command);
              }
            },
            errorMessage -> {
              responseWriter.writeRejectionOnCommand(
                  command, RejectionType.NOT_FOUND, errorMessage);
            });
  }

  private <T extends Number> void jobChange(
      final Map<String, Number> changeset,
      final String key,
      final Function<Number, T> converter,
      final Function<T, Optional<String>> updateFunction,
      final List<String> errors) {
    if (changeset.containsKey(key)) {
      final T value = converter.apply(changeset.get(key));
      updateFunction.apply(value).ifPresent(errors::add);
    }
  }

  // Helper method to handle rejections
  private void handleRejection(final List<String> errors, final TypedRecord<JobRecord> command) {
    final String errorMessage = String.join(", ", errors);
    rejectionWriter.appendRejection(command, RejectionType.INVALID_ARGUMENT, errorMessage);
    responseWriter.writeRejectionOnCommand(command, RejectionType.INVALID_ARGUMENT, errorMessage);
  }
}
