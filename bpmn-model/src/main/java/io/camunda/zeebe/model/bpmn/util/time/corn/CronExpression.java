/*
 * Copyright 2002-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Forked from: https://github.com/spring-projects/spring-framework
 */
package io.camunda.zeebe.model.bpmn.util.time.corn;

import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.Arrays;

/**
 * Representation of a <a href="https://www.manpagez.com/man/5/crontab/">crontab expression</a> that
 * can calculate the next time it matches.
 *
 * <p>{@code CronExpression} instances are created through {@link #parse(String)}; the next match is
 * determined with {@link #next(Temporal)}.
 *
 * @author Arjen Poutsma
 * @since 5.3
 */
public final class CronExpression {

  static final int MAX_ATTEMPTS = 366;

  private static final String[] MACROS =
      new String[] {
        "@yearly", "0 0 0 1 1 *",
        "@annually", "0 0 0 1 1 *",
        "@monthly", "0 0 0 1 * *",
        "@weekly", "0 0 0 * * 0",
        "@daily", "0 0 0 * * *",
        "@midnight", "0 0 0 * * *",
        "@hourly", "0 0 * * * *"
      };

  private final CronField[] fields;

  private final String expression;

  private CronExpression(
      final CronField seconds,
      final CronField minutes,
      final CronField hours,
      final CronField daysOfMonth,
      final CronField months,
      final CronField daysOfWeek,
      final String expression) {

    // reverse order, to make big changes first
    // to make sure we end up at 0 nanos, we add an extra field
    fields =
        new CronField[] {
          daysOfWeek, months, daysOfMonth, hours, minutes, seconds, CronField.zeroNanos()
        };
    this.expression = expression;
  }

  /**
   * Parse the given <a href="https://www.manpagez.com/man/5/crontab/">crontab expression</a> string
   * into a {@code CronExpression}. The string has six single space-separated time and date fields:
   *
   * <pre>
   * &#9484;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472; second (0-59)
   * &#9474; &#9484;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472; minute (0 - 59)
   * &#9474; &#9474; &#9484;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472; hour (0 - 23)
   * &#9474; &#9474; &#9474; &#9484;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472; day of the month (1 - 31)
   * &#9474; &#9474; &#9474; &#9474; &#9484;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472; month (1 - 12) (or JAN-DEC)
   * &#9474; &#9474; &#9474; &#9474; &#9474; &#9484;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472; day of the week (0 - 7)
   * &#9474; &#9474; &#9474; &#9474; &#9474; &#9474;          (0 or 7 is Sunday, or MON-SUN)
   * &#9474; &#9474; &#9474; &#9474; &#9474; &#9474;
   * &#42; &#42; &#42; &#42; &#42; &#42;
   * </pre>
   *
   * <p>The following rules apply:
   *
   * <ul>
   *   <li>A field may be an asterisk ({@code *}), which always stands for "first-last". For the
   *       "day of the month" or "day of the week" fields, a question mark ({@code ?}) may be used
   *       instead of an asterisk.
   *   <li>Ranges of numbers are expressed by two numbers separated with a hyphen ({@code -}). The
   *       specified range is inclusive.
   *   <li>Following a range (or {@code *}) with {@code /n} specifies the interval of the number's
   *       value through the range.
   *   <li>English names can also be used for the "month" and "day of week" fields. Use the first
   *       three letters of the particular day or month (case does not matter).
   *   <li>The "day of month" and "day of week" fields can contain a {@code L}-character, which
   *       stands for "last", and has a different meaning in each field:
   *       <ul>
   *         <li>In the "day of month" field, {@code L} stands for "the last day of the month". If
   *             followed by an negative offset (i.e. {@code L-n}), it means "{@code n}th-to-last
   *             day of the month". If followed by {@code W} (i.e. {@code LW}), it means "the last
   *             weekday of the month".
   *         <li>In the "day of week" field, {@code L} stands for "the last day of the week". If
   *             prefixed by a number or three-letter name (i.e. {@code dL} or {@code DDDL}), it
   *             means "the last day of week {@code d} (or {@code DDD}) in the month".
   *       </ul>
   *   <li>The "day of month" field can be {@code nW}, which stands for "the nearest weekday to day
   *       of the month {@code n}". If {@code n} falls on Saturday, this yields the Friday before
   *       it. If {@code n} falls on Sunday, this yields the Monday after, which also happens if
   *       {@code n} is {@code 1} and falls on a Saturday (i.e. {@code 1W} stands for "the first
   *       weekday of the month").
   *   <li>The "day of week" field can be {@code d#n} (or {@code DDD#n}), which stands for "the
   *       {@code n}-th day of week {@code d} (or {@code DDD}) in the month".
   * </ul>
   *
   * <p>Example expressions:
   *
   * <ul>
   *   <li>{@code "0 0 * * * *"} = the top of every hour of every day.
   *   <li><code>"*&#47;10 * * * * *"</code> = every ten seconds.
   *   <li>{@code "0 0 8-10 * * *"} = 8, 9 and 10 o'clock of every day.
   *   <li>{@code "0 0 6,19 * * *"} = 6:00 AM and 7:00 PM every day.
   *   <li>{@code "0 0/30 8-10 * * *"} = 8:00, 8:30, 9:00, 9:30, 10:00 and 10:30 every day.
   *   <li>{@code "0 0 9-17 * * MON-FRI"} = on the hour nine-to-five weekdays
   *   <li>{@code "0 0 0 25 12 ?"} = every Christmas Day at midnight
   *   <li>{@code "0 0 0 L * *"} = last day of the month at midnight
   *   <li>{@code "0 0 0 L-3 * *"} = third-to-last day of the month at midnight
   *   <li>{@code "0 0 0 1W * *"} = first weekday of the month at midnight
   *   <li>{@code "0 0 0 LW * *"} = last weekday of the month at midnight
   *   <li>{@code "0 0 0 * * 5L"} = last Friday of the month at midnight
   *   <li>{@code "0 0 0 * * THUL"} = last Thursday of the month at midnight
   *   <li>{@code "0 0 0 ? * 5#2"} = the second Friday in the month at midnight
   *   <li>{@code "0 0 0 ? * MON#1"} = the first Monday in the month at midnight
   * </ul>
   *
   * <p>The following macros are also supported:
   *
   * <ul>
   *   <li>{@code "@yearly"} (or {@code "@annually"}) to run un once a year, i.e. {@code "0 0 0 1 1
   *       *"},
   *   <li>{@code "@monthly"} to run once a month, i.e. {@code "0 0 0 1 * *"},
   *   <li>{@code "@weekly"} to run once a week, i.e. {@code "0 0 0 * * 0"},
   *   <li>{@code "@daily"} (or {@code "@midnight"}) to run once a day, i.e. {@code "0 0 0 * * *"},
   *   <li>{@code "@hourly"} to run once an hour, i.e. {@code "0 0 * * * *"}.
   * </ul>
   *
   * @param expression the expression string to parse
   * @return the parsed {@code CronExpression} object
   * @throws IllegalArgumentException in the expression does not conform to the cron format
   */
  public static CronExpression parse(final String expression) {
    if (!StringUtil.hasLength(expression)) {
      throw new IllegalArgumentException("Expression string must not be empty");
    }

    final String resolvedExpression = resolveMacros(expression);

    final String[] fields = resolvedExpression.split(" ");
    if (fields.length != 6) {
      throw new IllegalArgumentException(
          String.format(
              "Cron expression must consist of 6 fields (found %d in \"%s\")",
              fields.length, expression));
    }
    try {
      final CronField seconds = CronField.parseSeconds(fields[0]);
      final CronField minutes = CronField.parseMinutes(fields[1]);
      final CronField hours = CronField.parseHours(fields[2]);
      final CronField daysOfMonth = CronField.parseDaysOfMonth(fields[3]);
      final CronField months = CronField.parseMonth(fields[4]);
      final CronField daysOfWeek = CronField.parseDaysOfWeek(fields[5]);

      return new CronExpression(
          seconds, minutes, hours, daysOfMonth, months, daysOfWeek, expression);
    } catch (final IllegalArgumentException ex) {
      final String msg = ex.getMessage() + " in cron expression \"" + expression + "\"";
      throw new IllegalArgumentException(msg, ex);
    }
  }

  /**
   * Determine whether the given string represents a valid cron expression.
   *
   * @param expression the expression to evaluate
   * @return {@code true} if the given expression is a valid cron expression
   * @since 5.3.8
   */
  public static boolean isValidExpression(final String expression) {
    if (expression == null) {
      return false;
    }
    try {
      parse(expression);
      return true;
    } catch (final IllegalArgumentException ex) {
      return false;
    }
  }

  private static String resolveMacros(final String expression) {
    final String trimmedExpression = expression.trim();
    for (int i = 0; i < MACROS.length; i = i + 2) {
      if (MACROS[i].equalsIgnoreCase(trimmedExpression)) {
        return MACROS[i + 1];
      }
    }
    return trimmedExpression;
  }

  public <T extends Temporal & Comparable<? super T>> T next(final T temporal) {
    return nextOrSame(ChronoUnit.NANOS.addTo(temporal, 1));
  }

  private <T extends Temporal & Comparable<? super T>> T nextOrSame(T temporal) {
    for (int i = 0; i < MAX_ATTEMPTS; i++) {
      final T result = nextOrSameInternal(temporal);
      if (result == null || result.equals(temporal)) {
        return result;
      }
      temporal = result;
    }
    return null;
  }

  private <T extends Temporal & Comparable<? super T>> T nextOrSameInternal(T temporal) {
    for (final CronField field : fields) {
      temporal = field.nextOrSame(temporal);
      if (temporal == null) {
        return null;
      }
    }
    return temporal;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(fields);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o instanceof CronExpression) {
      return Arrays.equals(fields, ((CronExpression) o).fields);
    } else {
      return false;
    }
  }

  /**
   * Return the expression string used to create this {@code CronExpression}.
   *
   * @return the expression string
   */
  @Override
  public String toString() {
    return expression;
  }
}
