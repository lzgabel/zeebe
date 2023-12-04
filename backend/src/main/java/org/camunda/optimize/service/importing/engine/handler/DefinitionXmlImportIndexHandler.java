/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under one or more contributor license agreements.
 * Licensed under a proprietary license. See the License.txt file for more information.
 * You may not use this file except in compliance with the proprietary license.
 */
package org.camunda.optimize.service.importing.engine.handler;

import lombok.extern.slf4j.Slf4j;
import org.camunda.optimize.dto.optimize.index.AllEntitiesBasedImportIndexDto;
import org.camunda.optimize.service.db.DatabaseClient;
import org.camunda.optimize.service.db.reader.ImportIndexReader;
import org.camunda.optimize.service.db.es.OptimizeElasticsearchClient;
import org.camunda.optimize.service.importing.EngineImportIndexHandler;
import org.camunda.optimize.service.importing.page.IdSetBasedImportPage;
import org.camunda.optimize.service.util.DatabaseHelper;
import org.camunda.optimize.service.util.configuration.ConfigurationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;

import jakarta.annotation.PostConstruct;

import java.util.Optional;
import java.util.Set;

@Slf4j
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public abstract class DefinitionXmlImportIndexHandler
  implements EngineImportIndexHandler<IdSetBasedImportPage, AllEntitiesBasedImportIndexDto> {

  @Autowired
  protected DatabaseClient databaseClient;

  @Autowired
  protected ConfigurationService configurationService;

  @Autowired
  private ImportIndexReader importIndexReader;

  private Long importIndex = 0L;

  @Override
  public IdSetBasedImportPage getNextPage() {
    Set<String> ids = performSearchQuery();
    IdSetBasedImportPage page = new IdSetBasedImportPage();
    page.setIds(ids);
    updateIndex(ids.size());
    return page;
  }

  @Override
  public AllEntitiesBasedImportIndexDto getIndexStateDto() {
    AllEntitiesBasedImportIndexDto importIndexDto = new AllEntitiesBasedImportIndexDto();
    importIndexDto.setEsTypeIndexRefersTo(getDatabaseTypeForStoring());
    importIndexDto.setImportIndex(importIndex);
    importIndexDto.setEngine(getEngineAlias());
    return importIndexDto;
  }

  @Override
  public void resetImportIndex() {
    log.debug("Resetting import index");
    importIndex = 0L;
  }

  public void updateIndex(int pageSize) {
    importIndex += pageSize;
  }

  @PostConstruct
  protected void init() {
    readIndexFromDatabase();
  }

  protected abstract Set<String> performSearchQuery();

  protected abstract String getDatabaseTypeForStoring();

  private String getDatabaseId() {
    return DatabaseHelper.constructKey(getDatabaseTypeForStoring(), getEngineAlias());
  }

  private void readIndexFromDatabase() {
    Optional<AllEntitiesBasedImportIndexDto> storedIndex =
      importIndexReader.getImportIndex(getDatabaseId());
    storedIndex.ifPresent(
      allEntitiesBasedImportIndexDto -> importIndex = allEntitiesBasedImportIndexDto.getImportIndex()
    );
  }

}
