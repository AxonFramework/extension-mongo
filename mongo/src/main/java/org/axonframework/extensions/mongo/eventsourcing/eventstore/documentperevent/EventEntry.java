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

package org.axonframework.extensions.mongo.eventsourcing.eventstore.documentperevent;

import com.mongodb.DBObject;
import org.axonframework.eventhandling.DomainEventData;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.serialization.SerializedMetaData;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.SimpleSerializedObject;
import org.bson.Document;

import java.time.Instant;
import java.util.Date;
import java.util.Objects;

import static org.axonframework.common.DateTimeUtils.parseInstant;

/**
 * Implementation of a serialized event message that can be used to create a Mongo document.
 *
 * @author Rene de Waele
 * @since 3.0
 */
public class EventEntry implements DomainEventData<Object> {

    private final String aggregateIdentifier;
    private final String aggregateType;
    private final long sequenceNumber;
    private final Instant timestamp;
    private final Object serializedPayload;
    private final String payloadType;
    private final String payloadRevision;
    private final Object serializedMetaData;
    private final String eventIdentifier;

    /**
     * Constructor used to create a new event entry to store in Mongo.
     *
     * @param event      The actual DomainEvent to store
     * @param serializer Serializer to use for the event to store
     */
    public EventEntry(DomainEventMessage<?> event, Serializer serializer) {
        aggregateIdentifier = event.getAggregateIdentifier();
        aggregateType = event.getType();
        sequenceNumber = event.getSequenceNumber();
        eventIdentifier = event.getIdentifier();
        Class<?> serializationTarget = String.class;
        if (serializer.canSerializeTo(DBObject.class)) {
            serializationTarget = DBObject.class;
        }
        SerializedObject<?> serializedPayloadObject = event.serializePayload(serializer, serializationTarget);
        SerializedObject<?> serializedMetaDataObject = event.serializeMetaData(serializer, serializationTarget);
        serializedPayload = serializedPayloadObject.getData();
        payloadType = serializedPayloadObject.getType().getName();
        payloadRevision = serializedPayloadObject.getType().getRevision();
        serializedMetaData = serializedMetaDataObject.getData();
        timestamp = event.getTimestamp();
    }

    /**
     * Creates a new EventEntry based on data provided by Mongo.
     *
     * @param dbObject      Mongo object that contains data to represent an EventEntry
     * @param configuration Configuration containing the property names
     */
    public EventEntry(Document dbObject, EventEntryConfiguration configuration) {
        String aggregateId = (String) dbObject.get(configuration.aggregateIdentifierProperty());
        String eventId = (String) dbObject.get(configuration.eventIdentifierProperty());

        aggregateIdentifier = eventId.equals(aggregateId) ? null : aggregateId;
        aggregateType = (String) dbObject.get(configuration.typeProperty());
        sequenceNumber = ((Number) dbObject.get(configuration.sequenceNumberProperty())).longValue();
        serializedPayload = dbObject.get(configuration.payloadProperty());
        Object timestampField = dbObject.get(configuration.timestampProperty());
        if(timestampField instanceof Date) {
            timestamp = ((Date) timestampField).toInstant();
        }
        else if(timestampField instanceof Instant) {
            timestamp = (Instant) timestampField;
        }
        else timestamp = parseInstant((String)timestampField);//to ensure backward compatibility
        // when timestamps were written as strings

        payloadType = (String) dbObject.get(configuration.payloadTypeProperty());
        payloadRevision = (String) dbObject.get(configuration.payloadRevisionProperty());
        serializedMetaData = dbObject.get(configuration.metaDataProperty());
        eventIdentifier = eventId;
    }

    /**
     * Returns the current entry as a mongo Document.
     *
     * @param configuration The configuration describing property names
     * @return Document representing the entry
     */
    public Document asDocument(EventEntryConfiguration configuration) {
        return new Document(configuration.aggregateIdentifierProperty(), aggregateIdentifier)
                .append(configuration.typeProperty(), aggregateType)
                .append(configuration.sequenceNumberProperty(), sequenceNumber)
                .append(configuration.payloadProperty(), serializedPayload)
                .append(configuration.timestampProperty(), timestamp)
                .append(configuration.payloadTypeProperty(), payloadType)
                .append(configuration.payloadRevisionProperty(), payloadRevision)
                .append(configuration.metaDataProperty(), serializedMetaData)
                .append(configuration.eventIdentifierProperty(), eventIdentifier);
    }

    @Override
    public String getType() {
        return aggregateType;
    }

    @Override
    public String getAggregateIdentifier() {
        return aggregateIdentifier;
    }

    @Override
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public String getEventIdentifier() {
        return eventIdentifier;
    }

    @Override
    public Instant getTimestamp() {
        return timestamp;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public SerializedObject<Object> getMetaData() {
        return new SerializedMetaData(serializedMetaData, getRepresentationType());
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public SerializedObject<Object> getPayload() {
        return new SimpleSerializedObject(serializedPayload, getRepresentationType(), payloadType, payloadRevision);
    }

    private Class<?> getRepresentationType() {
        Class<?> representationType = String.class;
        if (serializedPayload instanceof DBObject) {
            representationType = DBObject.class;
        } else if (serializedPayload instanceof Document) {
            representationType = Document.class;
        }
        return representationType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EventEntry that = (EventEntry) o;

        if (!Objects.equals(getEventIdentifier(), that.getEventIdentifier())) {
            return false;
        }
        if (!Objects.equals(getTimestamp(), that.getTimestamp())) {
            return false;
        }
        if (!Objects.equals(getPayload(), that.getPayload())) {
            return false;
        }
        if (!Objects.equals(getMetaData(), that.getMetaData())) {
            return false;
        }
        if (!Objects.equals(getType(), that.getType())) {
            return false;
        }
        if (!Objects.equals(getAggregateIdentifier(), that.getAggregateIdentifier())) {
            return false;
        }
        return Objects.equals(getSequenceNumber(), that.getSequenceNumber());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getEventIdentifier(),
                            getTimestamp(),
                            getPayload(),
                            getMetaData(),
                            getType(),
                            getAggregateIdentifier(),
                            getSequenceNumber());
    }

    @Override
    public String toString() {
        return "EventEntry{" +
                ", eventIdentifier='" + getEventIdentifier() + '\'' +
                ", timeStamp='" + getTimestamp() + '\'' +
                ", payload='" + getPayload() + '\'' +
                ", metaData=" + getMetaData() +
                ", type='" + getType() + '\'' +
                ", aggregateIdentifier='" + getAggregateIdentifier() + '\'' +
                ", sequenceNumber=" + getSequenceNumber() +
                '}';
    }
}
