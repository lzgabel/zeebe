/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine.state.appliers;

import io.camunda.zeebe.engine.state.TypedEventApplier;
import io.camunda.zeebe.engine.state.mutable.MutableConditionalSubscriptionState;
import io.camunda.zeebe.protocol.impl.record.value.condition.ConditionalSubscriptionRecord;
import io.camunda.zeebe.protocol.record.intent.ConditionalSubscriptionIntent;

public final class ConditionalSubscriptionDeletedApplier
    implements TypedEventApplier<ConditionalSubscriptionIntent, ConditionalSubscriptionRecord> {

  private final MutableConditionalSubscriptionState subscriptionState;

  public ConditionalSubscriptionDeletedApplier(
      final MutableConditionalSubscriptionState subscriptionState) {
    this.subscriptionState = subscriptionState;
  }

  @Override
  public void applyState(final long key, final ConditionalSubscriptionRecord value) {
    final var subscriptionKey = value.getSubscriptionKey();
    subscriptionState.remove(subscriptionKey, value.getConditionBuffer(), value.getTenantId());
  }
}
