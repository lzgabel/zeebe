package org.camunda.optimize.test.util;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class VariableTestUtil {

  private VariableTestUtil() {
  }

  public static Map<String, Object> createAllPrimitiveTypeVariables() {
    Map<String, Object> variables = new HashMap<>();
    Integer integer = 1;
    variables.put("stringVar", "aStringValue");
    variables.put("boolVar", true);
    variables.put("integerVar", integer);
    variables.put("shortVar", integer.shortValue());
    variables.put("longVar", 1L);
    variables.put("doubleVar", 1.1);
    variables.put("dateVar", new Date());
    return variables;
  }
}
