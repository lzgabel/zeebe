/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.optimize.service.db.reader;

import io.camunda.optimize.dto.optimize.persistence.BusinessKeyDto;
import java.util.List;
import java.util.Set;

public interface BusinessKeyReader {

  List<BusinessKeyDto> getBusinessKeysForProcessInstanceIds(Set<String> processInstanceIds);
}
