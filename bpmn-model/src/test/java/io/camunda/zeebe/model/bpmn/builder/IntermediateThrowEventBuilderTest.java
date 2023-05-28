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
package io.camunda.zeebe.model.bpmn.builder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.instance.ExtensionElements;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeMessageDefinition;
import org.camunda.bpm.model.xml.instance.ModelElementInstance;
import org.junit.jupiter.api.Test;

public class IntermediateThrowEventBuilderTest {

  @Test
  void shouldSetMessageName() {
    // when
    final BpmnModelInstance instance =
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .intermediateThrowEvent(
                "throw", t -> t.message("message").zeebeMessageName("message-name-1"))
            .done();

    // then
    final ModelElementInstance messageThrowEvent = instance.getModelElementById("throw");
    final ExtensionElements extensionElements =
        (ExtensionElements) messageThrowEvent.getUniqueChildElementByType(ExtensionElements.class);
    assertThat(extensionElements.getChildElementsByType(ZeebeMessageDefinition.class))
        .hasSize(1)
        .extracting(ZeebeMessageDefinition::getMessageName)
        .containsExactly("message-name-1");
  }

  @Test
  void shouldSetMessageNameExpression() {
    // when
    final BpmnModelInstance instance =
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .intermediateThrowEvent(
                "throw", t -> t.message("message").zeebeMessageNameExpression("messageNameExpr"))
            .done();

    // then
    final ModelElementInstance messageThrowEvent = instance.getModelElementById("throw");
    final ExtensionElements extensionElements =
        (ExtensionElements) messageThrowEvent.getUniqueChildElementByType(ExtensionElements.class);
    assertThat(extensionElements.getChildElementsByType(ZeebeMessageDefinition.class))
        .hasSize(1)
        .extracting(ZeebeMessageDefinition::getMessageName)
        .containsExactly("=messageNameExpr");
  }

  @Test
  void shouldSetMessageId() {
    // when
    final BpmnModelInstance instance =
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .intermediateThrowEvent(
                "throw", t -> t.message("message").zeebeMessageId("message-id-1"))
            .done();

    // then
    final ModelElementInstance messageThrowEvent = instance.getModelElementById("throw");
    final ExtensionElements extensionElements =
        (ExtensionElements) messageThrowEvent.getUniqueChildElementByType(ExtensionElements.class);
    assertThat(extensionElements.getChildElementsByType(ZeebeMessageDefinition.class))
        .hasSize(1)
        .extracting(ZeebeMessageDefinition::getMessageId)
        .containsExactly("message-id-1");
  }

  @Test
  void shouldSetMessageIdExpression() {
    // when
    final BpmnModelInstance instance =
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .intermediateThrowEvent(
                "throw", t -> t.message("message").zeebeMessageIdExpression("messageIdExpr"))
            .done();

    // then
    final ModelElementInstance messageThrowEvent = instance.getModelElementById("throw");
    final ExtensionElements extensionElements =
        (ExtensionElements) messageThrowEvent.getUniqueChildElementByType(ExtensionElements.class);
    assertThat(extensionElements.getChildElementsByType(ZeebeMessageDefinition.class))
        .hasSize(1)
        .extracting(ZeebeMessageDefinition::getMessageId)
        .containsExactly("=messageIdExpr");
  }

  @Test
  void shouldSetCorrelationKey() {
    // when
    final BpmnModelInstance instance =
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .intermediateThrowEvent(
                "throw", t -> t.message("message").zeebeCorrelationKey("correlation-key-1"))
            .done();

    // then
    final ModelElementInstance messageThrowEvent = instance.getModelElementById("throw");
    final ExtensionElements extensionElements =
        (ExtensionElements) messageThrowEvent.getUniqueChildElementByType(ExtensionElements.class);
    assertThat(extensionElements.getChildElementsByType(ZeebeMessageDefinition.class))
        .hasSize(1)
        .extracting(ZeebeMessageDefinition::getCorrelationKey)
        .containsExactly("correlation-key-1");
  }

  @Test
  void shouldSetCorrelationKeyExpression() {
    // when
    final BpmnModelInstance instance =
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .intermediateThrowEvent(
                "throw",
                t -> t.message("message").zeebeCorrelationKeyExpression("correlationKeyExpr"))
            .done();

    // then
    final ModelElementInstance messageThrowEvent = instance.getModelElementById("throw");
    final ExtensionElements extensionElements =
        (ExtensionElements) messageThrowEvent.getUniqueChildElementByType(ExtensionElements.class);
    assertThat(extensionElements.getChildElementsByType(ZeebeMessageDefinition.class))
        .hasSize(1)
        .extracting(ZeebeMessageDefinition::getCorrelationKey)
        .containsExactly("=correlationKeyExpr");
  }

  @Test
  void shouldSetTimeToLive() {
    // when
    final BpmnModelInstance instance =
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .intermediateThrowEvent("throw", t -> t.message("message").zeebeTimeToLive("PT10S"))
            .done();

    // then
    final ModelElementInstance messageThrowEvent = instance.getModelElementById("throw");
    final ExtensionElements extensionElements =
        (ExtensionElements) messageThrowEvent.getUniqueChildElementByType(ExtensionElements.class);
    assertThat(extensionElements.getChildElementsByType(ZeebeMessageDefinition.class))
        .hasSize(1)
        .extracting(ZeebeMessageDefinition::getTimeToLive)
        .containsExactly("PT10S");
  }

  @Test
  void shouldSetTimeToLiveExpression() {
    // when
    final BpmnModelInstance instance =
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .intermediateThrowEvent(
                "throw", t -> t.message("message").zeebeTimeToLiveExpression("timeToLiveExpr"))
            .done();

    // then
    final ModelElementInstance messageThrowEvent = instance.getModelElementById("throw");
    final ExtensionElements extensionElements =
        (ExtensionElements) messageThrowEvent.getUniqueChildElementByType(ExtensionElements.class);
    assertThat(extensionElements.getChildElementsByType(ZeebeMessageDefinition.class))
        .hasSize(1)
        .extracting(ZeebeMessageDefinition::getTimeToLive)
        .containsExactly("=timeToLiveExpr");
  }

  @Test
  void shouldSetMessageNameAndMessageIdAndCorrelationKeyAndTimeToLive() {
    // when
    final BpmnModelInstance instance =
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .intermediateThrowEvent(
                "throw",
                t ->
                    t.message("message")
                        .zeebeMessageName("message-name")
                        .zeebeMessageId("message-id")
                        .zeebeCorrelationKey("correlation-key")
                        .zeebeTimeToLive("PT10S"))
            .done();

    // then
    final ModelElementInstance messageThrowEvent = instance.getModelElementById("throw");
    final ExtensionElements extensionElements =
        (ExtensionElements) messageThrowEvent.getUniqueChildElementByType(ExtensionElements.class);
    assertThat(extensionElements.getChildElementsByType(ZeebeMessageDefinition.class))
        .hasSize(1)
        .extracting(
            ZeebeMessageDefinition::getMessageName,
            ZeebeMessageDefinition::getMessageId,
            ZeebeMessageDefinition::getCorrelationKey,
            ZeebeMessageDefinition::getTimeToLive)
        .containsExactly(tuple("message-name", "message-id", "correlation-key", "PT10S"));
  }
}
