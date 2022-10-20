/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.extensions.mongo.eventhandling.deadletter;

import org.axonframework.eventhandling.EventMessage;
import org.axonframework.serialization.Serializer;

/**
 * Converter that can convert an {@link EventMessage} to a {@link DeadLetterEventEntry} and vice versa.
 *
 * @param <M> The type of the event message this converter will convert.
 * @author Gerard Klijs
 * @since 4.7.0
 */
public interface DeadLetterMongoConverter<M extends EventMessage<?>> {

    /**
     * Converts an {@link EventMessage} implementation to a {@link DeadLetterEventEntry}.
     *
     * @param message    The message to convert.
     * @param serializer The {@link Serializer} to use for serialization of payload and metadata.
     * @return The created {@link DeadLetterEventEntry}
     */
    DeadLetterEventEntry convert(M message, Serializer serializer);

    /**
     * Converts a {@link DeadLetterEventEntry} to a {@link EventMessage} implementation.
     *
     * @param entry      The database entry to convert to a {@link EventMessage}
     * @param serializer The {@link Serializer} to use for deserialization of payload and metadata.
     * @return The created {@link DeadLetterEventEntry}
     */
    M convert(DeadLetterEventEntry entry, Serializer serializer);

    /**
     * Check whether this converter supports the given {@link DeadLetterEventEntry}.
     *
     * @param message The message to check support for.
     * @return Whether the provided message is supported by this converter.
     */
    boolean canConvert(DeadLetterEventEntry message);

    /**
     * Check whether this converter supports the given {@link EventMessage}.
     *
     * @param message The message to check support for.
     * @return Whether the provided message is supported by this converter.
     */
    boolean canConvert(M message);
}
