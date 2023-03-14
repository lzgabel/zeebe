/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.transport.streamapi;

import io.atomix.cluster.MemberId;
import io.camunda.zeebe.broker.jobstream.StreamRegistry;
import io.camunda.zeebe.protocol.impl.stream.AddStreamRequest;
import io.camunda.zeebe.protocol.impl.stream.RemoveStreamRequest;
import io.camunda.zeebe.protocol.record.UUIDEncoder;
import io.camunda.zeebe.stream.api.GatewayStreamer.Metadata;
import io.camunda.zeebe.util.CloseableSilently;
import java.util.UUID;
import java.util.function.Supplier;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages mutating the stream registry via specific requests.
 *
 * @param <M> the metadata type of the registered streams
 */
public final class StreamApiHandler<M extends Metadata> implements CloseableSilently {
  private static final Logger LOG = LoggerFactory.getLogger(StreamApiHandler.class);

  // differs from the UUID RFC, which describes the nil-UUID as (0L, 0L), but there is no real way
  // to configure SBE null values. at the same time, the chances of generating the same ID as this
  // are infinitesimally low. this also produces a UUID with version 0, which is not generated by
  // the standard library
  private static final UUID NULL_ID =
      new UUID(UUIDEncoder.highNullValue(), UUIDEncoder.lowNullValue());

  private final StreamRegistry<M> registry;
  private final Supplier<M> metadataFactory;

  public StreamApiHandler(final StreamRegistry<M> registry, final Supplier<M> metadataFactory) {
    this.registry = registry;
    this.metadataFactory = metadataFactory;
  }

  @Override
  public void close() {
    registry.clear();
  }

  public void add(final MemberId sender, final AddStreamRequest request) {
    final M properties = metadataFactory.get();
    properties.wrap(request.metadata(), 0, request.metadata().capacity());

    if (request.streamType().capacity() <= 0) {
      final String errorMessage =
          "Expected a stream type of length > 0, but it has %d"
              .formatted(request.streamType().capacity());
      LOG.warn("Failed to open stream for '{}': [{}]", sender, errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }

    if (request.streamId() == null || request.streamId().equals(NULL_ID)) {
      final String errorMessage =
          "Expected a stream ID, but received a nil UUID ([%s])".formatted(request.streamId());
      LOG.warn("Failed to open stream for '{}': [{}]", sender, errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }

    registry.add(new UnsafeBuffer(request.streamType()), request.streamId(), sender, properties);
    LOG.debug("Opened stream {} from {}", request.streamId(), sender);
  }

  public void remove(final MemberId sender, final RemoveStreamRequest request) {
    registry.remove(request.streamId(), sender);
    LOG.debug("Removed stream {} from {}", request.streamId(), sender);
  }

  public void removeAll(final MemberId sender) {
    registry.removeAll(sender);
    LOG.debug("Removed all streams from {}", sender);
  }
}
