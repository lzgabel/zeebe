/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.topology;

import io.camunda.zeebe.topology.serializer.ClusterTopologySerializer;
import io.camunda.zeebe.topology.state.ClusterTopology;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Manages reading and updating ClusterTopology in a local persisted file * */
final class PersistedClusterTopology {
  private final Path topologyFile;
  private final ClusterTopologySerializer serializer;
  private ClusterTopology clusterTopology = ClusterTopology.uninitialized();

  private Listener topologyUpdateListener;

  PersistedClusterTopology(final Path topologyFile, final ClusterTopologySerializer serializer) {
    this.topologyFile = topologyFile;
    this.serializer = serializer;
  }

  void tryInitialize() throws IOException {
    if (Files.exists(topologyFile)) {
      final var serializedTopology = Files.readAllBytes(topologyFile);
      if (serializedTopology.length > 0) {
        clusterTopology = serializer.decodeClusterTopology(serializedTopology);
      }
    }
  }

  ClusterTopology getTopology() {
    return clusterTopology;
  }

  void update(final ClusterTopology clusterTopology) throws IOException {
    if (this.clusterTopology.equals(clusterTopology)) {
      return;
    }

    final var serializedTopology = serializer.encode(clusterTopology);
    Files.write(
        topologyFile,
        serializedTopology,
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE,
        StandardOpenOption.DSYNC);

    this.clusterTopology = clusterTopology;
    if (topologyUpdateListener != null) {
      topologyUpdateListener.onTopologyUpdated(clusterTopology);
    }
  }

  public boolean isUninitialized() {
    return clusterTopology.isUninitialized();
  }

  public void addUpdateListener(final Listener updateListener) {
    topologyUpdateListener = updateListener;
  }

  public void removeUpdateListener(final Listener updateListener) {
    if (topologyUpdateListener == updateListener) {
      topologyUpdateListener = null;
    }
  }

  @FunctionalInterface
  interface Listener {
    void onTopologyUpdated(ClusterTopology clusterTopology);
  }
}
