/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.camunda.zeebe.broker.SpringBrokerBridge;
import io.camunda.zeebe.broker.exporter.repo.ExporterRepository;
import io.camunda.zeebe.broker.system.configuration.BrokerCfg;
import io.camunda.zeebe.broker.system.monitoring.BrokerHealthCheckService;
import io.camunda.zeebe.protocol.impl.encoding.BrokerInfo;
import io.camunda.zeebe.test.util.socket.SocketUtil;
import io.camunda.zeebe.util.sched.ActorScheduler;
import io.camunda.zeebe.util.sched.TestConcurrencyControl;
import io.camunda.zeebe.util.sched.future.ActorFuture;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ApiMessagingServiceStepTest {
  private static final TestConcurrencyControl CONCURRENCY_CONTROL = new TestConcurrencyControl();
  private static final BrokerCfg TEST_BROKER_CONFIG = new BrokerCfg();
  private static final BrokerInfo TEST_BROKER_INFO = new BrokerInfo(0, "localhost");
  private static final Duration TIME_OUT = Duration.ofSeconds(10);

  static {
    final var commandApiCfg = TEST_BROKER_CONFIG.getNetwork().getCommandApi();
    commandApiCfg.setHost("localhost");
    commandApiCfg.setAdvertisedHost("localhost");
  }

  private final ActorScheduler mockActorSchedulingService = mock(ActorScheduler.class);
  private BrokerStartupContextImpl testBrokerStartupContext;
  private final ApiMessagingServiceStep sut = new ApiMessagingServiceStep();

  @BeforeEach
  void setUp() {
    when(mockActorSchedulingService.submitActor(any()))
        .thenReturn(CONCURRENCY_CONTROL.completedFuture(null));

    testBrokerStartupContext =
        new BrokerStartupContextImpl(
            TEST_BROKER_INFO,
            TEST_BROKER_CONFIG,
            mock(SpringBrokerBridge.class),
            mockActorSchedulingService,
            mock(BrokerHealthCheckService.class),
            mock(ExporterRepository.class),
            Collections.emptyList());
    testBrokerStartupContext.setConcurrencyControl(CONCURRENCY_CONTROL);
  }

  @Test
  void shouldHaveDescriptiveName() {
    // when
    final var actual = sut.getName();

    // then
    assertThat(actual).isSameAs("API Messaging Service");
  }

  @Nested
  class StartupBehavior {
    private ActorFuture<BrokerStartupContext> startupFuture;

    @BeforeEach
    void setUp() {
      startupFuture = CONCURRENCY_CONTROL.createFuture();

      final var port = SocketUtil.getNextAddress().getPort();
      final var commandApiCfg = TEST_BROKER_CONFIG.getNetwork().getCommandApi();
      commandApiCfg.setPort(port);
      commandApiCfg.setAdvertisedPort(port);
    }

    @AfterEach
    void tearDown() {
      final var messagingService = testBrokerStartupContext.getApiMessagingService();
      if (messagingService != null) {
        messagingService.stop().join();
      }
    }

    @Test
    void shouldCompleteFuture() {
      // when
      sut.startupInternal(testBrokerStartupContext, CONCURRENCY_CONTROL, startupFuture);

      // then
      assertThat(startupFuture).succeedsWithin(TIME_OUT);
      assertThat(startupFuture.join()).isNotNull();
    }

    @Test
    void shouldStartAndInstallMessagingService() {
      // when
      sut.startupInternal(testBrokerStartupContext, CONCURRENCY_CONTROL, startupFuture);
      await().until(startupFuture::isDone);

      // then
      final var messagingService = testBrokerStartupContext.getApiMessagingService();
      assertThat(messagingService).isNotNull();
      assertThat(messagingService.isRunning()).isTrue();
    }
  }

  @Nested
  class ShutdownBehavior {
    private ManagedMessagingService mockManagedMessagingService;
    private ActorFuture<BrokerStartupContext> shutdownFuture;

    @BeforeEach
    void setUp() {
      mockManagedMessagingService = mock(ManagedMessagingService.class);
      when(mockManagedMessagingService.stop()).thenReturn(CompletableFuture.completedFuture(null));
      shutdownFuture = CONCURRENCY_CONTROL.createFuture();
      testBrokerStartupContext.setApiMessagingService(mockManagedMessagingService);
    }

    @Test
    void shouldStopAndUninstallMessagingService() {
      // when
      sut.shutdownInternal(testBrokerStartupContext, CONCURRENCY_CONTROL, shutdownFuture);
      await().until(shutdownFuture::isDone);

      // then
      verify(mockManagedMessagingService).stop();
      final var messagingService = testBrokerStartupContext.getApiMessagingService();
      assertThat(messagingService).isNull();
    }

    @Test
    void shouldCompleteFutureExceptionally() {
      // given
      final var failingMessagingService = mock(ManagedMessagingService.class);
      final var exception = new Exception();
      when(failingMessagingService.stop()).thenReturn(CompletableFuture.failedFuture(exception));
      testBrokerStartupContext.setApiMessagingService(failingMessagingService);

      // when
      sut.shutdownInternal(testBrokerStartupContext, CONCURRENCY_CONTROL, shutdownFuture);

      // then
      await().until(shutdownFuture::isCompletedExceptionally);
      assertThat(shutdownFuture.getException()).isEqualTo(exception);
      assertThat(testBrokerStartupContext.getApiMessagingService()).isNotNull();
    }

    @Test
    void shouldCompleteFuture() {
      // when
      sut.shutdownInternal(testBrokerStartupContext, CONCURRENCY_CONTROL, shutdownFuture);

      // then
      assertThat(shutdownFuture).succeedsWithin(TIME_OUT);
      assertThat(shutdownFuture.join()).isNotNull();
    }
  }
}
