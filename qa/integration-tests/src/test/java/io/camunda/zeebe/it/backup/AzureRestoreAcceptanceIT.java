/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.it.backup;

import com.azure.storage.blob.BlobServiceClient;
import io.camunda.zeebe.backup.azure.AzureBackupConfig;
import io.camunda.zeebe.backup.azure.AzureBackupStore;
import io.camunda.zeebe.broker.system.configuration.BrokerCfg;
import io.camunda.zeebe.broker.system.configuration.backup.AzureBackupStoreConfig;
import io.camunda.zeebe.broker.system.configuration.backup.BackupStoreCfg.BackupStoreType;
import io.camunda.zeebe.qa.util.testcontainers.AzuriteContainer;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
final class AzureRestoreAcceptanceIT implements RestoreAcceptance {
  @Container private static final AzuriteContainer AZURITE_CONTAINER = new AzuriteContainer();
  private static final String CONTAINER_NAME = RandomStringUtils.randomAlphabetic(10).toLowerCase();

  @BeforeAll
  static void setupBucket() {
    final AzureBackupConfig config =
        new AzureBackupConfig.Builder()
            .withConnectionString(AZURITE_CONTAINER.getConnectString())
            .withContainerName(CONTAINER_NAME)
            .build();

    final BlobServiceClient blobServiceClient = AzureBackupStore.buildClient(config);
    blobServiceClient.createBlobContainerIfNotExists(CONTAINER_NAME);
  }

  @Override
  public void configureBackupStore(final BrokerCfg cfg) {
    final var backup = cfg.getData().getBackup();
    backup.setStore(BackupStoreType.AZURE);
    final AzureBackupStoreConfig azureBackupStoreConfig = new AzureBackupStoreConfig();
    azureBackupStoreConfig.setConnectionString(AZURITE_CONTAINER.getConnectString());
    azureBackupStoreConfig.setBasePath(CONTAINER_NAME);
    backup.setAzure(azureBackupStoreConfig);
  }
}
