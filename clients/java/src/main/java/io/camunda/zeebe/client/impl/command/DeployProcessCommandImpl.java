/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.camunda.zeebe.client.impl.command;

import com.google.protobuf.ByteString;
import io.camunda.zeebe.client.api.ZeebeFuture;
import io.camunda.zeebe.client.api.command.ClientException;
import io.camunda.zeebe.client.api.command.DeployProcessCommandStep1;
import io.camunda.zeebe.client.api.command.DeployProcessCommandStep1.DeployProcessCommandBuilderStep2;
import io.camunda.zeebe.client.api.command.FinalCommandStep;
import io.camunda.zeebe.client.api.response.DeploymentEvent;
import io.camunda.zeebe.client.impl.RetriableClientFutureImpl;
import io.camunda.zeebe.client.impl.response.DeploymentEventImpl;
import io.camunda.zeebe.gateway.protocol.GatewayGrpc.GatewayStub;
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass;
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass.DeployProcessRequest;
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass.ProcessRequestObject;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.instance.Process;
import io.camunda.zeebe.model.bpmn.instance.*;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeCalledElement;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeTaskDefinition;
import io.grpc.stub.StreamObserver;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static io.camunda.zeebe.client.impl.command.ArgumentUtil.ensureNotNull;
import static io.camunda.zeebe.client.impl.command.StreamUtil.readInputStream;

public final class DeployProcessCommandImpl
    implements DeployProcessCommandStep1, DeployProcessCommandBuilderStep2 {

  private final DeployProcessRequest.Builder requestBuilder = DeployProcessRequest.newBuilder();
  private final GatewayStub asyncStub;
  private final String namespace;
  private final Predicate<Throwable> retryPredicate;
  private Duration requestTimeout;

  public DeployProcessCommandImpl(
      final GatewayStub asyncStub,
      final Duration requestTimeout,
      final String namespace,
      final Predicate<Throwable> retryPredicate) {
    this.asyncStub = asyncStub;
    this.namespace = namespace;
    this.requestTimeout = requestTimeout;
    this.retryPredicate = retryPredicate;
  }

  @Override
  public DeployProcessCommandBuilderStep2 addResourceBytes(
      final byte[] resource, final String resourceName) {
    ByteString definition = ByteString.copyFrom(resource);
    if (namespace != null && namespace.length() > 0) {
      final BpmnModelInstance instance = Bpmn.readModelFromStream(new ByteArrayInputStream(resource));
      final Collection<Process> processes = instance.getModelElementsByType(Process.class);
      final Process process = processes.iterator().next();

      // ServiceTask
      resolveNamespace(instance, namespace, ServiceTask.class);

      // BusinessRuleTask
      resolveNamespace(instance, namespace, BusinessRuleTask.class);

      // ScriptTask
      resolveNamespace(instance, namespace, ScriptTask.class);

      // SendTask
      resolveNamespace(instance, namespace, SendTask.class);

      // ManualTask
      resolveNamespace(instance, namespace, ManualTask.class);

      // CallActivity
      resolveNamespace(instance, namespace, CallActivity.class);

      process.setId(namespace + "." + process.getId());
      process.setName(namespace + "." + process.getName());
      definition = ByteString.copyFrom(Bpmn.convertToString(instance).getBytes());
    }
    requestBuilder.addProcesses(
        ProcessRequestObject.newBuilder()
            .setName(resourceName)
            .setDefinition(definition));

    return this;
  }

  private <T extends Activity> void resolveNamespace(
      final BpmnModelInstance instance,
      final String namespace,
      final Class<T> type) {
    final Collection<T> tasks = instance.getModelElementsByType(type);
    if (type == CallActivity.class) {
      tasks.stream().forEach(task -> {
        ExtensionElements extensionElements = task.getExtensionElements();
        Collection<ZeebeCalledElement> zeebeCalledElements = extensionElements
            .getChildElementsByType(ZeebeCalledElement.class);
        ZeebeCalledElement zeebeCalledElement = zeebeCalledElements.iterator().next();
        // If it is not a dynamic variable, add a namespace for business isolation.
        if (Objects.nonNull(zeebeCalledElement.getProcessId()) && !zeebeCalledElement.getProcessId()
            .startsWith("=")) {
          zeebeCalledElement.setProcessId(namespace + "." + zeebeCalledElement.getProcessId());
        }
      });
    } else {
      tasks.stream().forEach(task -> {
        ExtensionElements extensionElements = task.getExtensionElements();
        Collection<ZeebeTaskDefinition> taskDefinitions = extensionElements
            .getChildElementsByType(ZeebeTaskDefinition.class);
        ZeebeTaskDefinition taskDefinition = taskDefinitions.iterator().next();
        // If it is not a dynamic variable, add a namespace for business isolation.
        if (Objects.nonNull(taskDefinition.getType()) && !taskDefinition.getType()
            .startsWith("=")) {
          taskDefinition.setType(namespace + "." + taskDefinition.getType());
        }
      });
    }
  }

  @Override
  public DeployProcessCommandBuilderStep2 addResourceString(
      final String resource, final Charset charset, final String resourceName) {
    return addResourceBytes(resource.getBytes(charset), resourceName);
  }

  @Override
  public DeployProcessCommandBuilderStep2 addResourceStringUtf8(
      final String resourceString, final String resourceName) {
    return addResourceString(resourceString, StandardCharsets.UTF_8, resourceName);
  }

  @Override
  public DeployProcessCommandBuilderStep2 addResourceStream(
      final InputStream resourceStream, final String resourceName) {
    ensureNotNull("resource stream", resourceStream);

    try {
      final byte[] bytes = readInputStream(resourceStream);

      return addResourceBytes(bytes, resourceName);
    } catch (final IOException e) {
      final String exceptionMsg =
          String.format("Cannot deploy bpmn resource from stream. %s", e.getMessage());
      throw new ClientException(exceptionMsg, e);
    }
  }

  @Override
  public DeployProcessCommandBuilderStep2 addResourceFromClasspath(final String classpathResource) {
    ensureNotNull("classpath resource", classpathResource);

    try (final InputStream resourceStream =
        getClass().getClassLoader().getResourceAsStream(classpathResource)) {
      if (resourceStream != null) {
        return addResourceStream(resourceStream, classpathResource);
      } else {
        throw new FileNotFoundException(classpathResource);
      }

    } catch (final IOException e) {
      final String exceptionMsg =
          String.format("Cannot deploy resource from classpath. %s", e.getMessage());
      throw new RuntimeException(exceptionMsg, e);
    }
  }

  @Override
  public DeployProcessCommandBuilderStep2 addResourceFile(final String filename) {
    ensureNotNull("filename", filename);

    try (final InputStream resourceStream = new FileInputStream(filename)) {
      return addResourceStream(resourceStream, filename);
    } catch (final IOException e) {
      final String exceptionMsg =
          String.format("Cannot deploy resource from file. %s", e.getMessage());
      throw new RuntimeException(exceptionMsg, e);
    }
  }

  @Override
  public DeployProcessCommandBuilderStep2 addProcessModel(
      final BpmnModelInstance processDefinition, final String resourceName) {
    ensureNotNull("process model", processDefinition);

    final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    Bpmn.writeModelToStream(outStream, processDefinition);
    return addResourceBytes(outStream.toByteArray(), resourceName);
  }

  @Override
  public FinalCommandStep<DeploymentEvent> requestTimeout(final Duration requestTimeout) {
    this.requestTimeout = requestTimeout;
    return this;
  }

  @Override
  public ZeebeFuture<DeploymentEvent> send() {
    final DeployProcessRequest request = requestBuilder.build();

    final RetriableClientFutureImpl<DeploymentEvent, GatewayOuterClass.DeployProcessResponse>
        future =
        new RetriableClientFutureImpl<>(
            DeploymentEventImpl::new,
            retryPredicate,
            streamObserver -> send(request, streamObserver));

    send(request, future);

    return future;
  }

  private void send(final DeployProcessRequest request, final StreamObserver streamObserver) {
    asyncStub
        .withDeadlineAfter(requestTimeout.toMillis(), TimeUnit.MILLISECONDS)
        .deployProcess(request, streamObserver);
  }
}
