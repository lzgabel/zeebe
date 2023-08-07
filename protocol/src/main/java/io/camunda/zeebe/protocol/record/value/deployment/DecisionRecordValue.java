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
package io.camunda.zeebe.protocol.record.value.deployment;

import io.camunda.zeebe.protocol.record.ImmutableProtocol;
import io.camunda.zeebe.protocol.record.RecordValue;
import org.immutables.value.Value;

/**
 * Represents a deployed decision. A decision belongs to a decision requirements graph (DRG/DRD)
 * that represents the DMN resource. The DMN resource itself is stored as part of the DRG (see
 * {@link DecisionRequirementsRecordValue}).
 */
@Value.Immutable
@ImmutableProtocol(builder = ImmutableDecisionRecordValue.Builder.class)
public interface DecisionRecordValue extends RecordValue, DecisionMetadataValue {}
