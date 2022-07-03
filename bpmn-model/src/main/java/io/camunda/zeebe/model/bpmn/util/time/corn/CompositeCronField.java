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

import java.time.temporal.Temporal;

/**
 * Extension of {@link CronField} that wraps an array of cron fields.
 *
 * @author Arjen Poutsma
 * @since 5.3.3
 */
final class CompositeCronField extends CronField {

  private final CronField[] fields;

  private final String value;

  private CompositeCronField(final Type type, final CronField[] fields, final String value) {
    super(type);
    this.fields = fields;
    this.value = value;
  }

  /** Composes the given fields into a {@link CronField}. */
  public static CronField compose(final CronField[] fields, final Type type, final String value) {
    if (fields == null || fields.length == 0) {
      throw new IllegalArgumentException("Fields must not be empty");
    }

    if (!StringUtil.hasLength(value)) {
      throw new IllegalArgumentException("Value must not be empty");
    }

    if (fields.length == 1) {
      return fields[0];
    } else {
      return new CompositeCronField(type, fields, value);
    }
  }

  @Override
  public <T extends Temporal & Comparable<? super T>> T nextOrSame(final T temporal) {
    T result = null;
    for (final CronField field : fields) {
      final T candidate = field.nextOrSame(temporal);
      if (result == null || candidate != null && candidate.compareTo(result) < 0) {
        result = candidate;
      }
    }
    return result;
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CompositeCronField)) {
      return false;
    }
    final CompositeCronField other = (CompositeCronField) o;
    return type() == other.type() && value.equals(other.value);
  }

  @Override
  public String toString() {
    return type() + " '" + value + "'";
  }
}
