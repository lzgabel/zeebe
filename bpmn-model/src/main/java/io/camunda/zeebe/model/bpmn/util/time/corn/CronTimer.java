/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.camunda.zeebe.model.bpmn.util.time.corn;

import io.camunda.zeebe.model.bpmn.util.time.Interval;
import io.camunda.zeebe.model.bpmn.util.time.Timer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Optional;

public class CronTimer implements Timer {

  private final CronExpression expression;

  private Optional<ZonedDateTime> start;

  private int repetitions;

  public CronTimer(final CronExpression expression) {
    this(Optional.empty(), expression);
  }

  public CronTimer(final Optional<ZonedDateTime> start, final CronExpression expression) {
    this.expression = expression;
    this.start = start;
  }

  @Override
  public Interval getInterval() {
    return null;
  }

  @Override
  public int getRepetitions() {
    return repetitions;
  }

  public CronExpression getExpression() {
    return expression;
  }

  public Optional<ZonedDateTime> getStart() {
    return start;
  }

  public CronTimer withStart(final Instant start) {
    final Optional<ZonedDateTime> zoneAwareStart =
        Optional.of(ZonedDateTime.ofInstant(start, ZoneId.systemDefault()));
    return new CronTimer(zoneAwareStart, getExpression());
  }

  @Override
  public long getDueDate(final long fromEpochMilli) {
    final ZonedDateTime start =
        getStart()
            .orElseGet(
                () ->
                    ZonedDateTime.ofInstant(
                        Instant.ofEpochMilli(fromEpochMilli), ZoneId.systemDefault()));
    final ZonedDateTime next = expression.next(start);

    if (next == null) {
      repetitions = 0;
      return fromEpochMilli;
    }

    repetitions = -1;
    return next.toInstant().toEpochMilli();
  }

  public static CronTimer parse(final String text) {
    try {
      final CronExpression expression = CronExpression.parse(text);
      return new CronTimer(expression);
    } catch (IllegalArgumentException ex) {
      throw new DateTimeParseException(ex.getMessage(), Optional.ofNullable(text).orElse(""), 0);
    }
  }
}
