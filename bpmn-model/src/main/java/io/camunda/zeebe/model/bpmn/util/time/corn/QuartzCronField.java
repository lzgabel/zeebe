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
import java.time.DayOfWeek;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAdjuster;
import java.time.temporal.TemporalAdjusters;

/**
 * Extension of {@link CronField} for <a href="https://www.quartz-scheduler.org>Quartz</a> -specific
 * fields. Created using the {@code parse*} methods, uses a {@link TemporalAdjuster} internally.
 *
 * @author Arjen Poutsma
 * @since 5.3
 */
final class QuartzCronField extends CronField {

  private final Type rollForwardType;

  private final TemporalAdjuster adjuster;

  private final String value;

  private QuartzCronField(final Type type, final TemporalAdjuster adjuster, final String value) {
    this(type, type, adjuster, value);
  }

  /**
   * Constructor for fields that need to roll forward over a different type than the type this field
   * represents. See {@link #parseDaysOfWeek(String)}.
   */
  private QuartzCronField(
      final Type type,
      final Type rollForwardType,
      final TemporalAdjuster adjuster,
      final String value) {
    super(type);
    this.adjuster = adjuster;
    this.value = value;
    this.rollForwardType = rollForwardType;
  }

  /** Returns whether the given value is a Quartz day-of-month field. */
  public static boolean isQuartzDaysOfMonthField(final String value) {
    return value.contains("L") || value.contains("W");
  }

  /**
   * Parse the given value into a days of months {@code QuartzCronField}, the fourth entry of a cron
   * expression. Expects a "L" or "W" in the given value.
   */
  public static QuartzCronField parseDaysOfMonth(final String value) {
    int idx = value.lastIndexOf('L');
    if (idx != -1) {
      final TemporalAdjuster adjuster;
      if (idx != 0) {
        throw new IllegalArgumentException("Unrecognized characters before 'L' in '" + value + "'");
      } else if (value.length() == 2 && value.charAt(1) == 'W') { // "LW"
        adjuster = lastWeekdayOfMonth();
      } else {
        if (value.length() == 1) { // "L"
          adjuster = lastDayOfMonth();
        } else { // "L-[0-9]+"
          final int offset = IntergerUtil.parseInt(value, idx + 1, value.length(), 10);
          if (offset >= 0) {
            throw new IllegalArgumentException(
                "Offset '" + offset + " should be < 0 '" + value + "'");
          }
          adjuster = lastDayWithOffset(offset);
        }
      }
      return new QuartzCronField(Type.DAY_OF_MONTH, adjuster, value);
    }
    idx = value.lastIndexOf('W');
    if (idx != -1) {
      if (idx == 0) {
        throw new IllegalArgumentException("No day-of-month before 'W' in '" + value + "'");
      } else if (idx != value.length() - 1) {
        throw new IllegalArgumentException("Unrecognized characters after 'W' in '" + value + "'");
      } else { // "[0-9]+W"
        int dayOfMonth = IntergerUtil.parseInt(value, 0, idx, 10);
        dayOfMonth = Type.DAY_OF_MONTH.checkValidValue(dayOfMonth);
        final TemporalAdjuster adjuster = weekdayNearestTo(dayOfMonth);
        return new QuartzCronField(Type.DAY_OF_MONTH, adjuster, value);
      }
    }
    throw new IllegalArgumentException("No 'L' or 'W' found in '" + value + "'");
  }

  /** Returns whether the given value is a Quartz day-of-week field. */
  public static boolean isQuartzDaysOfWeekField(final String value) {
    return value.contains("L") || value.contains("#");
  }

  /**
   * Parse the given value into a days of week {@code QuartzCronField}, the sixth entry of a cron
   * expression. Expects a "L" or "#" in the given value.
   */
  public static QuartzCronField parseDaysOfWeek(final String value) {
    int idx = value.lastIndexOf('L');
    if (idx != -1) {
      if (idx != value.length() - 1) {
        throw new IllegalArgumentException("Unrecognized characters after 'L' in '" + value + "'");
      } else {
        final TemporalAdjuster adjuster;
        if (idx == 0) {
          throw new IllegalArgumentException("No day-of-week before 'L' in '" + value + "'");
        } else { // "[0-7]L"
          final DayOfWeek dayOfWeek = parseDayOfWeek(value.substring(0, idx));
          adjuster = lastInMonth(dayOfWeek);
        }
        return new QuartzCronField(Type.DAY_OF_WEEK, Type.DAY_OF_MONTH, adjuster, value);
      }
    }
    idx = value.lastIndexOf('#');
    if (idx != -1) {
      if (idx == 0) {
        throw new IllegalArgumentException("No day-of-week before '#' in '" + value + "'");
      } else if (idx == value.length() - 1) {
        throw new IllegalArgumentException("No ordinal after '#' in '" + value + "'");
      }
      // "[0-7]#[0-9]+"
      final DayOfWeek dayOfWeek = parseDayOfWeek(value.substring(0, idx));
      final int ordinal = IntergerUtil.parseInt(value, idx + 1, value.length(), 10);
      if (ordinal <= 0) {
        throw new IllegalArgumentException(
            "Ordinal '" + ordinal + "' in '" + value + "' must be positive number ");
      }

      final TemporalAdjuster adjuster = dayOfWeekInMonth(ordinal, dayOfWeek);
      return new QuartzCronField(Type.DAY_OF_WEEK, Type.DAY_OF_MONTH, adjuster, value);
    }
    throw new IllegalArgumentException("No 'L' or '#' found in '" + value + "'");
  }

  private static DayOfWeek parseDayOfWeek(final String value) {
    int dayOfWeek = Integer.parseInt(value);
    if (dayOfWeek == 0) {
      dayOfWeek = 7; // cron is 0 based; java.time 1 based
    }
    try {
      return DayOfWeek.of(dayOfWeek);
    } catch (final DateTimeException ex) {
      final String msg = ex.getMessage() + " '" + value + "'";
      throw new IllegalArgumentException(msg, ex);
    }
  }

  /** Returns an adjuster that resets to midnight. */
  private static TemporalAdjuster atMidnight() {
    return temporal -> {
      if (temporal.isSupported(ChronoField.NANO_OF_DAY)) {
        return temporal.with(ChronoField.NANO_OF_DAY, 0);
      } else {
        return temporal;
      }
    };
  }

  /**
   * Returns an adjuster that returns a new temporal set to the last day of the current month at
   * midnight.
   */
  private static TemporalAdjuster lastDayOfMonth() {
    final TemporalAdjuster adjuster = TemporalAdjusters.lastDayOfMonth();
    return temporal -> {
      final Temporal result = adjuster.adjustInto(temporal);
      return rollbackToMidnight(temporal, result);
    };
  }

  /** Returns an adjuster that returns the last weekday of the month. */
  private static TemporalAdjuster lastWeekdayOfMonth() {
    final TemporalAdjuster adjuster = TemporalAdjusters.lastDayOfMonth();
    return temporal -> {
      final Temporal lastDom = adjuster.adjustInto(temporal);
      final Temporal result;
      final int dow = lastDom.get(ChronoField.DAY_OF_WEEK);
      if (dow == 6) { // Saturday
        result = lastDom.minus(1, ChronoUnit.DAYS);
      } else if (dow == 7) { // Sunday
        result = lastDom.minus(2, ChronoUnit.DAYS);
      } else {
        result = lastDom;
      }
      return rollbackToMidnight(temporal, result);
    };
  }

  /**
   * Return a temporal adjuster that finds the nth-to-last day of the month.
   *
   * @param offset the negative offset, i.e. -3 means third-to-last
   * @return a nth-to-last day-of-month adjuster
   */
  private static TemporalAdjuster lastDayWithOffset(final int offset) {
    if (offset >= 0) {
      throw new IllegalArgumentException("Offset should be < 0");
    }

    final TemporalAdjuster adjuster = TemporalAdjusters.lastDayOfMonth();
    return temporal -> {
      final Temporal result = adjuster.adjustInto(temporal).plus(offset, ChronoUnit.DAYS);
      return rollbackToMidnight(temporal, result);
    };
  }

  /**
   * Return a temporal adjuster that finds the weekday nearest to the given day-of-month. If {@code
   * dayOfMonth} falls on a Saturday, the date is moved back to Friday; if it falls on a Sunday (or
   * if {@code dayOfMonth} is 1 and it falls on a Saturday), it is moved forward to Monday.
   *
   * @param dayOfMonth the goal day-of-month
   * @return the weekday-nearest-to adjuster
   */
  private static TemporalAdjuster weekdayNearestTo(final int dayOfMonth) {
    return temporal -> {
      int current = Type.DAY_OF_MONTH.get(temporal);
      DayOfWeek dayOfWeek = DayOfWeek.from(temporal);

      if ((current == dayOfMonth && isWeekday(dayOfWeek))
          || // dayOfMonth is a weekday
          (dayOfWeek == DayOfWeek.FRIDAY && current == dayOfMonth - 1)
          || // dayOfMonth is a Saturday, so Friday before
          (dayOfWeek == DayOfWeek.MONDAY && current == dayOfMonth + 1)
          || // dayOfMonth is a Sunday, so Monday after
          (dayOfWeek == DayOfWeek.MONDAY
              && dayOfMonth == 1
              && current == 3)) { // dayOfMonth is Saturday 1st, so Monday 3rd
        return temporal;
      }
      int count = 0;
      while (count++ < CronExpression.MAX_ATTEMPTS) {
        if (current == dayOfMonth) {
          dayOfWeek = DayOfWeek.from(temporal);

          if (dayOfWeek == DayOfWeek.SATURDAY) {
            if (dayOfMonth != 1) {
              temporal = temporal.minus(1, ChronoUnit.DAYS);
            } else {
              // exception for "1W" fields: execute on next Monday
              temporal = temporal.plus(2, ChronoUnit.DAYS);
            }
          } else if (dayOfWeek == DayOfWeek.SUNDAY) {
            temporal = temporal.plus(1, ChronoUnit.DAYS);
          }
          return atMidnight().adjustInto(temporal);
        } else {
          temporal = Type.DAY_OF_MONTH.elapseUntil(cast(temporal), dayOfMonth);
          current = Type.DAY_OF_MONTH.get(temporal);
        }
      }
      return null;
    };
  }

  private static boolean isWeekday(final DayOfWeek dayOfWeek) {
    return dayOfWeek != DayOfWeek.SATURDAY && dayOfWeek != DayOfWeek.SUNDAY;
  }

  /** Return a temporal adjuster that finds the last of the given doy-of-week in a month. */
  private static TemporalAdjuster lastInMonth(final DayOfWeek dayOfWeek) {
    final TemporalAdjuster adjuster = TemporalAdjusters.lastInMonth(dayOfWeek);
    return temporal -> {
      final Temporal result = adjuster.adjustInto(temporal);
      return rollbackToMidnight(temporal, result);
    };
  }

  /**
   * Returns a temporal adjuster that finds {@code ordinal}-th occurrence of the given day-of-week
   * in a month.
   */
  private static TemporalAdjuster dayOfWeekInMonth(final int ordinal, final DayOfWeek dayOfWeek) {
    final TemporalAdjuster adjuster = TemporalAdjusters.dayOfWeekInMonth(ordinal, dayOfWeek);
    return temporal -> {
      final Temporal result = adjuster.adjustInto(temporal);
      return rollbackToMidnight(temporal, result);
    };
  }

  /**
   * Rolls back the given {@code result} to midnight. When {@code current} has the same day of month
   * as {@code result}, the former is returned, to make sure that we don't end up before where we
   * started.
   */
  private static Temporal rollbackToMidnight(final Temporal current, final Temporal result) {
    if (result.get(ChronoField.DAY_OF_MONTH) == current.get(ChronoField.DAY_OF_MONTH)) {
      return current;
    } else {
      return atMidnight().adjustInto(result);
    }
  }

  @Override
  public <T extends Temporal & Comparable<? super T>> T nextOrSame(T temporal) {
    T result = adjust(temporal);
    if (result != null) {
      if (result.compareTo(temporal) < 0) {
        // We ended up before the start, roll forward and try again
        temporal = rollForwardType.rollForward(temporal);
        result = adjust(temporal);
        if (result != null) {
          result = type().reset(result);
        }
      }
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private <T extends Temporal & Comparable<? super T>> T adjust(final T temporal) {
    return (T) adjuster.adjustInto(temporal);
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
    if (!(o instanceof QuartzCronField)) {
      return false;
    }
    final QuartzCronField other = (QuartzCronField) o;
    return type() == other.type() && value.equals(other.value);
  }

  @Override
  public String toString() {
    return type() + " '" + value + "'";
  }
}
