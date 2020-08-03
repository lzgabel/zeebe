/*
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
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
package io.atomix.utils.serializer;

import static io.atomix.utils.serializer.NamespaceImpl.MAGIC_BYTE;
import static io.atomix.utils.serializer.NamespaceImpl.VERSION_BYTE;
import static io.atomix.utils.serializer.NamespaceImpl.VERSION_HEADER;
import static org.slf4j.LoggerFactory.getLogger;

import com.google.common.collect.ImmutableList;
import io.atomix.utils.serializer.NamespaceImpl.Builder;
import io.atomix.utils.serializer.NamespaceImpl.RegistrationBlock;
import java.nio.ByteBuffer;
import org.slf4j.Logger;

public class FallbackNamespace implements Namespace {

  private static final Logger LOG = getLogger(FallbackNamespace.class);
  private static final String DESERIALIZE_ERROR =
      "Deserialization failed with both the versioned and fallback serializers. The fallback serializer failed with:\n %s";
  private final NamespaceImpl fallback;
  private final NamespaceImpl namespace;

  FallbackNamespace(final NamespaceImpl fallback, final NamespaceImpl namespace) {
    this.fallback = fallback;
    this.namespace = namespace;
  }

  public FallbackNamespace(final NamespaceImpl.Builder builder) {
    final Builder copy = builder.copy();
    fallback = builder.build();
    namespace = copy.name(copy.getName() + "-compatible").setCompatible(true).build();
  }

  /**
   * Serializes given object to byte array using Kryo instance in pool.
   *
   * <p>Note: Serialized bytes must be smaller than {@link NamespaceImpl#MAX_BUFFER_SIZE}.
   *
   * @param obj Object to serialize
   * @return serialized bytes
   */
  @Override
  public byte[] serialize(final Object obj) {
    return namespace.serialize(obj);
  }

  /**
   * Serializes given object to byte buffer using Kryo instance in pool.
   *
   * @param obj Object to serialize
   * @param buffer to write to
   */
  @Override
  public void serialize(final Object obj, final ByteBuffer buffer) {
    namespace.serialize(obj, buffer);
  }

  /**
   * Deserializes given byte array to Object using Kryo instance in pool.
   *
   * @param bytes serialized bytes
   * @param <T> deserialized Object type
   * @return deserialized Object
   */
  @Override
  public <T> T deserialize(final byte[] bytes) {
    if (bytes[1] != MAGIC_BYTE) {
      return fallback.deserialize(bytes);
    }

    try {
      if (bytes[0] == VERSION_BYTE) {
        return namespace.deserialize(bytes, VERSION_HEADER.length);
      } else {
        LOG.warn(
            "Magic byte was encountered but version {} is unrecognized. Using FieldSerializer as fallback",
            (int) bytes[0]);
        return fallback.deserialize(bytes);
      }
    } catch (final Exception compatEx) {
      try {
        return fallback.deserialize(bytes);
      } catch (final Exception legacyEx) {
        // rethrow most relevant exception and log the second one
        LOG.warn(String.format(DESERIALIZE_ERROR, legacyEx));
        throw compatEx;
      }
    }
  }

  /**
   * Deserializes given byte buffer to Object using Kryo instance in pool.
   *
   * @param buffer input with serialized bytes
   * @param <T> deserialized Object type
   * @return deserialized Object
   */
  @Override
  public <T> T deserialize(final ByteBuffer buffer) {
    final int position = buffer.position();

    final byte version = buffer.get(buffer.position());
    final byte magicByte = buffer.get(buffer.position() + 1);

    if (magicByte != MAGIC_BYTE) {
      return fallback.deserialize(buffer);
    }

    try {
      if (version == VERSION_BYTE) {
        buffer.position(position + 2);
        return namespace.deserialize(buffer);
      } else {
        LOG.warn(
            "Magic byte was encountered but version {} is unrecognized. Using FieldSerializer as fallback",
            (int) version);
        return fallback.deserialize(buffer);
      }
    } catch (final Exception compatEx) {
      try {
        buffer.position(position);
        return fallback.deserialize(buffer);
      } catch (final Exception legacyEx) {
        // rethrow most relevant exception and log the second one
        LOG.warn(String.format(DESERIALIZE_ERROR, legacyEx));
        throw compatEx;
      }
    }
  }

  @Override
  public ImmutableList<RegistrationBlock> getRegisteredBlocks() {
    return namespace.getRegisteredBlocks();
  }
}
