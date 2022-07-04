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

import java.time.DateTimeException;
import java.time.temporal.Temporal;
import java.time.temporal.ValueRange;

/**
 * Efficient bitwise-operator extension of {@link CronField}. Created using the {@code parse*}
 * methods.
 *
 * @author Arjen Poutsma
 * @since 5.3
 */
final class BitsCronField extends CronField {

  private static final long MASK = 0xFFFFFFFFFFFFFFFFL;

  private static BitsCronField zeroNanos = null;

  // we store at most 60 bits, for seconds and minutes, so a 64-bit long suffices
  private long bits;

  private BitsCronField(final Type type) {
    super(type);
  }

  /** Return a {@code BitsCronField} enabled for 0 nano seconds. */
  public static BitsCronField zeroNanos() {
    if (zeroNanos == null) {
      final BitsCronField field = new BitsCronField(Type.NANO);
      field.setBit(0);
      zeroNanos = field;
    }
    return zeroNanos;
  }

  /**
   * Parse the given value into a seconds {@code BitsCronField}, the first entry of a cron
   * expression.
   */
  public static BitsCronField parseSeconds(final String value) {
    return parseField(value, Type.SECOND);
  }

  /**
   * Parse the given value into a minutes {@code BitsCronField}, the second entry of a cron
   * expression.
   */
  public static BitsCronField parseMinutes(final String value) {
    return BitsCronField.parseField(value, Type.MINUTE);
  }

  /**
   * Parse the given value into a hours {@code BitsCronField}, the third entry of a cron expression.
   */
  public static BitsCronField parseHours(final String value) {
    return BitsCronField.parseField(value, Type.HOUR);
  }

  /**
   * Parse the given value into a days of months {@code BitsCronField}, the fourth entry of a cron
   * expression.
   */
  public static BitsCronField parseDaysOfMonth(final String value) {
    return parseDate(value, Type.DAY_OF_MONTH);
  }

  /**
   * Parse the given value into a month {@code BitsCronField}, the fifth entry of a cron expression.
   */
  public static BitsCronField parseMonth(final String value) {
    return BitsCronField.parseField(value, Type.MONTH);
  }

  /**
   * Parse the given value into a days of week {@code BitsCronField}, the sixth entry of a cron
   * expression.
   */
  public static BitsCronField parseDaysOfWeek(final String value) {
    final BitsCronField result = parseDate(value, Type.DAY_OF_WEEK);
    if (result.getBit(0)) {
      // cron supports 0 for Sunday; we use 7 like java.time
      result.setBit(7);
      result.clearBit(0);
    }
    return result;
  }

  private static BitsCronField parseDate(final String value, final Type type) {
    String v = value;
    if ("?".equals(value)) {
      v = "*";
    }
    return BitsCronField.parseField(v, type);
  }

  private static BitsCronField parseField(final String value, final Type type) {
    if (!StringUtil.hasLength(value)) {
      throw new IllegalArgumentException("Value must not be empty");
    }

    if (type == null) {
      throw new IllegalArgumentException("Type must not be null");
    }

    try {
      final BitsCronField result = new BitsCronField(type);
      final String[] fields = value.split(",");
      for (final String field : fields) {
        final int slashPos = field.indexOf('/');
        if (slashPos == -1) {
          final ValueRange range = parseRange(field, type);
          result.setBits(range);
        } else {
          final String rangeStr = field.substring(0, slashPos);
          final String deltaStr = field.substring(slashPos + 1);
          ValueRange range = parseRange(rangeStr, type);
          if (rangeStr.indexOf('-') == -1) {
            range = ValueRange.of(range.getMinimum(), type.range().getMaximum());
          }
          final int delta = Integer.parseInt(deltaStr);
          if (delta <= 0) {
            throw new IllegalArgumentException("Incrementer delta must be 1 or higher");
          }
          result.setBits(range, delta);
        }
      }
      return result;
    } catch (final DateTimeException | IllegalArgumentException ex) {
      final String msg = ex.getMessage() + " '" + value + "'";
      throw new IllegalArgumentException(msg, ex);
    }
  }

  private static ValueRange parseRange(final String value, final Type type) {
    if ("*".equals(value)) {
      return type.range();
    } else {
      final int hyphenPos = value.indexOf('-');
      if (hyphenPos == -1) {
        try {
          final int result = type.checkValidValue(Integer.parseInt(value));
          return ValueRange.of(result, result);
        } catch (NumberFormatException ex) {
          throw new IllegalArgumentException(ex.getMessage());
        }
      } else {
        int min = IntergerUtil.parseInt(value, 0, hyphenPos, 10);
        int max = IntergerUtil.parseInt(value, hyphenPos + 1, value.length(), 10);
        min = type.checkValidValue(min);
        max = type.checkValidValue(max);
        if (type == Type.DAY_OF_WEEK && min == 7) {
          // If used as a minimum in a range, Sunday means 0 (not 7)
          min = 0;
        }
        return ValueRange.of(min, max);
      }
    }
  }

  @Override
  public <T extends Temporal & Comparable<? super T>> T nextOrSame(T temporal) {
    int current = type().get(temporal);
    int next = nextSetBit(current);
    if (next == -1) {
      temporal = type().rollForward(temporal);
      next = nextSetBit(0);
    }
    if (next == current) {
      return temporal;
    } else {
      int count = 0;
      current = type().get(temporal);
      while (current != next && count++ < CronExpression.MAX_ATTEMPTS) {
        temporal = type().elapseUntil(temporal, next);
        current = type().get(temporal);
        next = nextSetBit(current);
        if (next == -1) {
          temporal = type().rollForward(temporal);
          next = nextSetBit(0);
        }
      }
      if (count >= CronExpression.MAX_ATTEMPTS) {
        return null;
      }
      return type().reset(temporal);
    }
  }

  boolean getBit(final int index) {
    return (bits & (1L << index)) != 0;
  }

  private int nextSetBit(final int fromIndex) {
    final long result = bits & (MASK << fromIndex);
    if (result != 0) {
      return Long.numberOfTrailingZeros(result);
    } else {
      return -1;
    }
  }

  private void setBits(final ValueRange range) {
    if (range.getMinimum() == range.getMaximum()) {
      setBit((int) range.getMinimum());
    } else {
      final long minMask = MASK << range.getMinimum();
      final long maxMask = MASK >>> -(range.getMaximum() + 1);
      bits |= (minMask & maxMask);
    }
  }

  private void setBits(final ValueRange range, final int delta) {
    if (delta == 1) {
      setBits(range);
    } else {
      for (long i = range.getMinimum(); i <= range.getMaximum(); i += delta) {
        setBit((int) i);
      }
    }
  }

  private void setBit(final int index) {
    bits |= (1L << index);
  }

  private void clearBit(final int index) {
    bits &= ~(1L << index);
  }

  @Override
  public int hashCode() {
    return Long.hashCode(bits);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BitsCronField)) {
      return false;
    }
    final BitsCronField other = (BitsCronField) o;
    return type() == other.type() && bits == other.bits;
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder(type().toString());
    builder.append(" {");
    int i = nextSetBit(0);
    if (i != -1) {
      builder.append(i);
      i = nextSetBit(i + 1);
      while (i != -1) {
        builder.append(", ");
        builder.append(i);
        i = nextSetBit(i + 1);
      }
    }
    builder.append('}');
    return builder.toString();
  }
}
