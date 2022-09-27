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

import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericDomainEventMessage;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.extensions.mongo.eventsourcing.eventstore.documentperevent.EventEntry;
import org.axonframework.extensions.mongo.eventsourcing.eventstore.documentperevent.EventEntryConfiguration;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.SimpleSerializedObject;
import org.bson.Document;
import org.bson.types.Binary;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;

import static java.util.Objects.isNull;

/**
 * Represents an {@link org.axonframework.eventhandling.EventMessage} when stored into the database. It contains all
 * properties known in the framework implementations. Based on which properties are present, the original message type
 * can be determined. For example, if an aggregate identifier is present, it was a {@link DomainEventMessage}.
 *
 * @author Gerard Klijs
 * @since 4.7.0
 */
public class DeadLetterEventEntry extends EventEntry {

    private static final String MESSAGE_TYPE_KEY = "messageType";
    private static final String TOKEN_TYPE_KEY = "tokenType";
    private static final String TOKEN_KEY = "token";

    private String messageType;
    private String tokenType;
    private byte[] token;

    /**
     * Create a new {@code DeadLetterEventEntry} based on data provided by Mongo.
     *
     * @param dbObject      Mongo object that contains data to represent a {@code DeadLetterEventEntry}.
     * @param configuration Configuration containing the property names.
     */
    public DeadLetterEventEntry(@Nonnull Document dbObject, @Nonnull EventEntryConfiguration configuration) {
        super(dbObject, configuration);
        this.messageType = dbObject.getString(MESSAGE_TYPE_KEY);
        Optional.ofNullable(dbObject.getString(TOKEN_TYPE_KEY)).ifPresent(type -> this.tokenType = type);
        Optional.ofNullable(dbObject.get(TOKEN_KEY)).ifPresent(t -> this.token = ((Binary) t).getData());
    }

    /**
     * Constructor used to create a new dead letter event entry to store in Mongo. Should not be used directly, use
     * {@link #fromEventMessage(EventMessage, Serializer)} instead.
     *
     * @param event      The actual Event to store.
     * @param serializer Serializer to use for the event to store.
     */
    private DeadLetterEventEntry(@Nonnull DomainEventMessage<?> event, @Nonnull Serializer serializer) {
        super(event, serializer);
    }

    /**
     * Static method to create a new dead letter event entry to store in Mongo from any event.
     *
     * @param message    The actual message to store.
     * @param serializer Serializer to use for the event to store and optionally for the tracking token.
     * @return a new {@link DeadLetterEventEntry} instance.
     */
    @SuppressWarnings("squid:S1874")
    public static DeadLetterEventEntry fromEventMessage(
            @Nonnull EventMessage<?> message,
            @Nonnull Serializer serializer
    ) {
        DeadLetterEventEntry entry = new DeadLetterEventEntry(asDomainEventMessage(message), serializer);
        entry.messageType = message.getClass().getName();
        Optional.of(message)
                .filter(TrackedEventMessage.class::isInstance)
                .map(TrackedEventMessage.class::cast)
                .map(m -> serializer.serialize(m.trackingToken(), byte[].class))
                .ifPresent(t -> {
                    entry.tokenType = t.getType().getName();
                    entry.token = t.getData();
                });
        return entry;
    }

    private static <T> DomainEventMessage<T> asDomainEventMessage(EventMessage<T> eventMessage) {
        if (eventMessage instanceof DomainEventMessage<?>) {
            return (DomainEventMessage<T>) eventMessage;
        }
        return new GenericDomainEventMessage<>(null, null, 0L, eventMessage,
                                               eventMessage::getTimestamp);
    }

    /**
     * Returns the event represented by this entry as a Mongo Document.
     *
     * @return Document representing the entry
     */
    @Override
    public Document asDocument(@Nonnull EventEntryConfiguration configuration) {
        Document document = super.asDocument(configuration);
        document.append(MESSAGE_TYPE_KEY, messageType);
        if (!isNull(tokenType) && !isNull(token)) {
            document.append(TOKEN_TYPE_KEY, tokenType).append(TOKEN_KEY, token);
        }
        return document;
    }

    /**
     * Returns the message type, which is defined by the {@link DeadLetterMongoConverter} that mapped this entry. Used
     * for later matching whether a converter can convert it back to an
     * {@link org.axonframework.eventhandling.EventMessage}.
     *
     * @return The message type.
     */
    public String getMessageType() {
        return messageType;
    }

    /**
     * Returns the original {@link TrackedEventMessage#trackingToken()} as a
     * {@link org.axonframework.serialization.SerializedObject}, if the original message was a
     * {@code TrackedEventMessage}.
     *
     * @return The original tracking token.
     */
    public SimpleSerializedObject<byte[]> getTrackingToken() {
        if (token == null) {
            return null;
        }
        return new SimpleSerializedObject<>(
                token,
                byte[].class,
                tokenType,
                null);
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        DeadLetterEventEntry that = (DeadLetterEventEntry) o;
        if (!Objects.equals(messageType, that.messageType)) {
            return false;
        }
        if (!Objects.equals(tokenType, that.tokenType)) {
            return false;
        }
        return Arrays.equals(token, that.token);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            messageType,
                            tokenType,
                            Arrays.hashCode(token));
    }

    @Override
    public String toString() {
        return "DeadLetterEventEntry{" +
                "messageType='" + messageType + '\'' +
                ", eventIdentifier='" + getEventIdentifier() + '\'' +
                ", timeStamp='" + getTimestamp() + '\'' +
                ", payload='" + getPayload() + '\'' +
                ", metaData=" + getMetaData() +
                ", type='" + getType() + '\'' +
                ", aggregateIdentifier='" + getAggregateIdentifier() + '\'' +
                ", sequenceNumber=" + getSequenceNumber() +
                ", tokenType='" + tokenType + '\'' +
                ", token=" + Arrays.toString(token) +
                '}';
    }
}
