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

import java.util.Objects;

public class IntergerUtil {

  /**
   * Parses the {@link CharSequence} argument as a signed {@code int} in the specified {@code
   * radix}, beginning at the specified {@code beginIndex} and extending to {@code endIndex - 1}.
   *
   * <p>The method does not take steps to guard against the {@code CharSequence} being mutated while
   * parsing.
   *
   * @param s the {@code CharSequence} containing the {@code int} representation to be parsed
   * @param beginIndex the beginning index, inclusive.
   * @param endIndex the ending index, exclusive.
   * @param radix the radix to be used while parsing {@code s}.
   * @return the signed {@code int} represented by the subsequence in the specified radix.
   * @throws NullPointerException if {@code s} is null.
   * @throws IndexOutOfBoundsException if {@code beginIndex} is negative, or if {@code beginIndex}
   *     is greater than {@code endIndex} or if {@code endIndex} is greater than {@code s.length()}.
   * @throws NumberFormatException if the {@code CharSequence} does not contain a parsable {@code
   *     int} in the specified {@code radix}, or if {@code radix} is either smaller than {@link
   *     java.lang.Character#MIN_RADIX} or larger than {@link java.lang.Character#MAX_RADIX}.
   * @since 9
   */
  public static int parseInt(
      final CharSequence s, final int beginIndex, final int endIndex, final int radix)
      throws NumberFormatException {
    Objects.requireNonNull(s);

    if (beginIndex < 0 || beginIndex > endIndex || endIndex > s.length()) {
      throw new IndexOutOfBoundsException();
    }
    if (radix < Character.MIN_RADIX) {
      throw new NumberFormatException("radix " + radix + " less than Character.MIN_RADIX");
    }
    if (radix > Character.MAX_RADIX) {
      throw new NumberFormatException("radix " + radix + " greater than Character.MAX_RADIX");
    }

    boolean negative = false;
    int i = beginIndex;
    int limit = -Integer.MAX_VALUE;

    if (i < endIndex) {
      final char firstChar = s.charAt(i);
      if (firstChar < '0') { // Possible leading "+" or "-"
        if (firstChar == '-') {
          negative = true;
          limit = Integer.MIN_VALUE;
        } else if (firstChar != '+') {
          throw IntergerUtil.forCharSequence(s, beginIndex, endIndex, i);
        }
        i++;
        if (i == endIndex) { // Cannot have lone "+" or "-"
          throw IntergerUtil.forCharSequence(s, beginIndex, endIndex, i);
        }
      }
      final int multmin = limit / radix;
      int result = 0;
      while (i < endIndex) {
        // Accumulating negatively avoids surprises near MAX_VALUE
        final int digit = Character.digit(s.charAt(i), radix);
        if (digit < 0 || result < multmin) {
          throw IntergerUtil.forCharSequence(s, beginIndex, endIndex, i);
        }
        result *= radix;
        if (result < limit + digit) {
          throw IntergerUtil.forCharSequence(s, beginIndex, endIndex, i);
        }
        i++;
        result -= digit;
      }
      return negative ? result : -result;
    } else {
      throw IntergerUtil.forInputString("", radix);
    }
  }

  /**
   * Factory method for making a {@code NumberFormatException} given the specified input which
   * caused the error.
   *
   * @param s the input causing the error
   */
  static NumberFormatException forInputString(final String s, final int radix) {
    return new NumberFormatException(
        "For input string: \"" + s + "\"" + (radix == 10 ? "" : " under radix " + radix));
  }

  /**
   * Factory method for making a {@code NumberFormatException} given the specified input which
   * caused the error.
   *
   * @param s the input causing the error
   * @param beginIndex the beginning index, inclusive.
   * @param endIndex the ending index, exclusive.
   * @param errorIndex the index of the first error in s
   */
  static NumberFormatException forCharSequence(
      final CharSequence s, final int beginIndex, final int endIndex, final int errorIndex) {
    return new NumberFormatException(
        "Error at index "
            + (errorIndex - beginIndex)
            + " in: \""
            + s.subSequence(beginIndex, endIndex)
            + "\"");
  }
}
