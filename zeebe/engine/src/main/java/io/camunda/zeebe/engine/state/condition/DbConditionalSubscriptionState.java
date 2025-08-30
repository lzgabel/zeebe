/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine.state.condition;

import io.camunda.zeebe.db.ColumnFamily;
import io.camunda.zeebe.db.TransactionContext;
import io.camunda.zeebe.db.ZeebeDb;
import io.camunda.zeebe.db.impl.DbCompositeKey;
import io.camunda.zeebe.db.impl.DbLong;
import io.camunda.zeebe.db.impl.DbString;
import io.camunda.zeebe.db.impl.DbTenantAwareKey;
import io.camunda.zeebe.db.impl.DbTenantAwareKey.PlacementType;
import io.camunda.zeebe.engine.state.immutable.ConditionalSubscriptionState.ConditionalSubscriptionVisitor;
import io.camunda.zeebe.engine.state.mutable.MutableConditionalSubscriptionState;
import io.camunda.zeebe.protocol.ZbColumnFamilies;
import io.camunda.zeebe.protocol.impl.record.value.condition.ConditionalSubscriptionRecord;
import org.agrona.DirectBuffer;

public final class DbConditionalSubscriptionState implements MutableConditionalSubscriptionState {

  // processDefinitionKey, processInstanceKey or elementInstanceKey
  private final DbLong subscriptionKey;
  private final DbString condition;
  private final ConditionalSubscription conditionalSubscription = new ConditionalSubscription();

  private final DbString tenantIdKey;
  private final DbTenantAwareKey<DbString> tenantAwareCondition;

  // [subscriptionKey, [tenantId, condition]] => ConditionalSubscription
  private final DbCompositeKey<DbLong, DbTenantAwareKey<DbString>>
      subscriptionKeyAndTenantAwareCondition;
  private final ColumnFamily<
          DbCompositeKey<DbLong, DbTenantAwareKey<DbString>>, ConditionalSubscription>
      subscriptionKeyAndConditionColumnFamily;

  public DbConditionalSubscriptionState(
      final ZeebeDb<ZbColumnFamilies> zeebeDb, final TransactionContext transactionContext) {
    condition = new DbString();
    subscriptionKey = new DbLong();
    tenantIdKey = new DbString();
    tenantAwareCondition = new DbTenantAwareKey<>(tenantIdKey, condition, PlacementType.PREFIX);

    subscriptionKeyAndTenantAwareCondition =
        new DbCompositeKey<>(subscriptionKey, tenantAwareCondition);
    subscriptionKeyAndConditionColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.CONDITIONAL_SUBSCRIPTION_BY_KEY_AND_CONDITION,
            transactionContext,
            subscriptionKeyAndTenantAwareCondition,
            conditionalSubscription);
  }

  @Override
  public void visitBySubscriptionKey(
      final long subscriptionKey, final ConditionalSubscriptionVisitor visitor) {
    this.subscriptionKey.wrapLong(subscriptionKey);
    visitSubscriptions(visitor);
  }

  private void visitSubscriptions(final ConditionalSubscriptionVisitor visitor) {
    subscriptionKeyAndConditionColumnFamily.whileEqualPrefix(
        subscriptionKey,
        (key, subscription) -> {
          visitor.visit(subscription);
        });
  }

  @Override
  public void put(final long key, final ConditionalSubscriptionRecord record) {
    conditionalSubscription.setKey(key).setRecord(record);
    wrapSubscriptionKeys(record);

    subscriptionKeyAndConditionColumnFamily.upsert(
        subscriptionKeyAndTenantAwareCondition, conditionalSubscription);
  }

  @Override
  public void remove(
      final long subscriptionKey, final DirectBuffer signalName, final String tenantId) {
    wrapSubscriptionKeys(subscriptionKey, signalName, tenantId);

    subscriptionKeyAndConditionColumnFamily.deleteExisting(subscriptionKeyAndTenantAwareCondition);
  }

  private void wrapSubscriptionKeys(final ConditionalSubscriptionRecord subscription) {
    final var key = subscription.getSubscriptionKey();
    wrapSubscriptionKeys(key, subscription.getConditionBuffer(), subscription.getTenantId());
  }

  private void wrapSubscriptionKeys(
      final long key, final DirectBuffer condition, final String tenantId) {
    subscriptionKey.wrapLong(key);
    this.condition.wrapBuffer(condition);
    tenantIdKey.wrapString(tenantId);
  }
}
