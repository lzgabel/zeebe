/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.state.signal;

import io.camunda.zeebe.db.ColumnFamily;
import io.camunda.zeebe.db.TransactionContext;
import io.camunda.zeebe.db.ZeebeDb;
import io.camunda.zeebe.db.impl.DbCompositeKey;
import io.camunda.zeebe.db.impl.DbLong;
import io.camunda.zeebe.db.impl.DbNil;
import io.camunda.zeebe.db.impl.DbString;
import io.camunda.zeebe.engine.state.mutable.MutableSignalSubscriptionState;
import io.camunda.zeebe.protocol.ZbColumnFamilies;
import io.camunda.zeebe.protocol.impl.record.value.signal.SignalSubscriptionRecord;
import org.agrona.DirectBuffer;

public final class DbSignalSubscriptionState implements MutableSignalSubscriptionState {

  private final DbString signalName;
  private final DbLong subscriptionKey;

  // (signalName, subscriptionKey => SignalSubscription)
  private final DbCompositeKey<DbString, DbLong> signalNameAndSubscriptionKey;
  private final ColumnFamily<DbCompositeKey<DbString, DbLong>, SignalSubscription>
      signalNameAndSubscriptionKeyColumnFamily;
  private final SignalSubscription signalSubscription = new SignalSubscription();

  // (subscriptionKey, signalName) => \0  : to find existing subscriptions of a process or element
  private final DbCompositeKey<DbLong, DbString> subscriptionKeyAndSignalName;
  private final ColumnFamily<DbCompositeKey<DbLong, DbString>, DbNil>
      subscriptionKeyAndSignalNameColumnFamily;

  public DbSignalSubscriptionState(
      final ZeebeDb<ZbColumnFamilies> zeebeDb, final TransactionContext transactionContext) {
    signalName = new DbString();
    subscriptionKey = new DbLong();
    signalNameAndSubscriptionKey = new DbCompositeKey<>(signalName, subscriptionKey);
    signalNameAndSubscriptionKeyColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.SIGNAL_SUBSCRIPTION_BY_NAME_AND_KEY,
            transactionContext,
            signalNameAndSubscriptionKey,
            signalSubscription);

    subscriptionKeyAndSignalName = new DbCompositeKey<>(subscriptionKey, signalName);
    subscriptionKeyAndSignalNameColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.SIGNAL_SUBSCRIPTION_BY_KEY_AND_NAME,
            transactionContext,
            subscriptionKeyAndSignalName,
            DbNil.INSTANCE);
  }

  @Override
  public void put(final long key, final SignalSubscriptionRecord subscription) {
    signalSubscription.setKey(key).setRecord(subscription);

    signalName.wrapBuffer(subscription.getSignalNameBuffer());
    final var subKey =
        subscription.getCatchEventInstanceKey() > -1
            ? subscription.getCatchEventInstanceKey()
            : subscription.getProcessDefinitionKey();
    subscriptionKey.wrapLong(subKey);
    signalNameAndSubscriptionKeyColumnFamily.upsert(
        signalNameAndSubscriptionKey, signalSubscription);
    subscriptionKeyAndSignalNameColumnFamily.upsert(subscriptionKeyAndSignalName, DbNil.INSTANCE);
  }

  @Override
  public void remove(final long subscriptionKey, final DirectBuffer signalName) {
    this.subscriptionKey.wrapLong(subscriptionKey);
    this.signalName.wrapBuffer(signalName);

    signalNameAndSubscriptionKeyColumnFamily.deleteExisting(signalNameAndSubscriptionKey);
    subscriptionKeyAndSignalNameColumnFamily.deleteExisting(subscriptionKeyAndSignalName);
  }

  @Override
  public boolean exists(final SignalSubscriptionRecord subscription) {
    signalName.wrapBuffer(subscription.getSignalNameBuffer());
    final var key =
        subscription.getCatchEventInstanceKey() > -1
            ? subscription.getCatchEventInstanceKey()
            : subscription.getProcessDefinitionKey();
    subscriptionKey.wrapLong(key);

    return signalNameAndSubscriptionKeyColumnFamily.exists(signalNameAndSubscriptionKey);
  }

  @Override
  public void visitBySignalName(
      final DirectBuffer signalName, final SignalSubscriptionVisitor visitor) {
    this.signalName.wrapBuffer(signalName);
    signalNameAndSubscriptionKeyColumnFamily.whileEqualPrefix(
        this.signalName,
        (key, value) -> {
          visitor.visit(value);
        });
  }

  @Override
  public void visitStartEventSubscriptionsByProcessDefinitionKey(
      final long processDefinitionKey, final SignalSubscriptionVisitor visitor) {
    this.subscriptionKey.wrapLong(processDefinitionKey);

    subscriptionKeyAndSignalNameColumnFamily.whileEqualPrefix(
        this.subscriptionKey,
        (key, value) -> {
          final var subscription =
              signalNameAndSubscriptionKeyColumnFamily.get(signalNameAndSubscriptionKey);

          if (subscription != null) {
            visitor.visit(subscription);
          }
        });
  }
}
