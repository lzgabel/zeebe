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

import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.instance.IntermediateThrowEvent;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeMessageDefinition;

/**
 * @author Sebastian Menski
 */
public abstract class AbstractIntermediateThrowEventBuilder<
        B extends AbstractIntermediateThrowEventBuilder<B>>
    extends AbstractThrowEventBuilder<B, IntermediateThrowEvent> {

  protected AbstractIntermediateThrowEventBuilder(
      final BpmnModelInstance modelInstance,
      final IntermediateThrowEvent element,
      final Class<?> selfType) {
    super(modelInstance, element, selfType);
  }

  /**
   * Sets a static name of the message.
   *
   * @param messageName the name of the message
   * @return the builder object
   */
  public B zeebeMessageName(final String messageName) {
    final ZeebeMessageDefinition messageDefinition =
        getCreateSingleExtensionElement(ZeebeMessageDefinition.class);
    messageDefinition.setMessageName(messageName);
    return myself;
  }

  /**
   * Sets a dynamic name of the message. The name is retrieved from the given expression.
   *
   * @param messageNameExpression the expression for the name of the message
   * @return the builder object
   */
  public B zeebeMessageNameExpression(final String messageNameExpression) {
    return zeebeMessageName(asZeebeExpression(messageNameExpression));
  }

  /**
   * Sets a static id of the message.
   *
   * @param messageId the id of the message
   * @return the builder object
   */
  public B zeebeMessageId(final String messageId) {
    final ZeebeMessageDefinition messageDefinition =
        getCreateSingleExtensionElement(ZeebeMessageDefinition.class);
    messageDefinition.setMessageId(messageId);
    return myself;
  }

  /**
   * Sets a dynamic id of the message. The id is retrieved from the given expression.
   *
   * @param messageIdExpression the expression for the id of the message
   * @return the builder object
   */
  public B zeebeMessageIdExpression(final String messageIdExpression) {
    return zeebeMessageId(asZeebeExpression(messageIdExpression));
  }

  /**
   * Sets a static correlation key of the message.
   *
   * @param correlationKey the correlation key of the message
   * @return the builder object
   */
  public B zeebeCorrelationKey(String correlationKey) {
    final ZeebeMessageDefinition messageDefinition =
        getCreateSingleExtensionElement(ZeebeMessageDefinition.class);
    messageDefinition.setCorrelationKey(correlationKey);
    return myself;
  }

  /**
   * Sets a dynamic correlation key of the message. The correlation key is retrieved from the given
   * expression.
   *
   * @param correlationKeyExpression the expression for the correlation key of the message
   * @return the builder object
   */
  public B zeebeCorrelationKeyExpression(final String correlationKeyExpression) {
    return zeebeCorrelationKey(asZeebeExpression(correlationKeyExpression));
  }

  /**
   * Sets a static time to live of the message.
   *
   * @param timeToLive the correlation key of the message
   * @return the builder object
   */
  public B zeebeTimeToLive(String timeToLive) {
    final ZeebeMessageDefinition messageDefinition =
        getCreateSingleExtensionElement(ZeebeMessageDefinition.class);
    messageDefinition.setTimeToLive(timeToLive);
    return myself;
  }

  /**
   * Sets a dynamic time to live of the message. The time to live is retrieved from the given
   * expression.
   *
   * @param timeToLiveExpression the expression for the time to live of the message
   * @return the builder object
   */
  public B zeebeTimeToLiveExpression(final String timeToLiveExpression) {
    return zeebeTimeToLive(asZeebeExpression(timeToLiveExpression));
  }
}
