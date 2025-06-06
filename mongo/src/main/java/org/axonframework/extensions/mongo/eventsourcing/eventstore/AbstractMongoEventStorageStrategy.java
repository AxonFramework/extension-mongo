/*
 * Copyright (c) 2010-2025. Axon Framework
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

package org.axonframework.extensions.mongo.eventsourcing.eventstore;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Sorts;
import org.axonframework.common.Assert;
import org.axonframework.eventhandling.DomainEventData;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.TrackedEventData;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.extensions.mongo.eventsourcing.eventstore.documentperevent.EventEntryConfiguration;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Sorts.orderBy;
import static java.util.stream.StreamSupport.stream;
import static org.axonframework.common.DateTimeUtils.formatInstant;
import static org.axonframework.common.DateTimeUtils.parseInstant;
import static org.axonframework.common.ObjectUtils.getOrDefault;

/**
 * Abstract implementation of a Mongo {@link StorageStrategy}. Implementations only need to provide methods to convert
 * events and snapshots into Documents and vice versa.
 *
 * @author Rene de Waele
 * @since 3.0
 */
public abstract class AbstractMongoEventStorageStrategy implements StorageStrategy {

    /**
     * The value to pass to Mongo to fetch documents in ascending order.
     */
    protected static final int ORDER_ASC = 1;

    /**
     * The value to pass to Mongo to fetch documents in descending order.
     */
    protected static final int ORDER_DESC = -1;

    private final EventEntryConfiguration eventConfiguration;
    private final Duration lookBackTime;

    /**
     * Initializes a new StorageStrategy for a EventStorageEngine that uses Mongo.
     *
     * @param eventConfiguration configuration of the event entry 'schema'. If {@code null} the schema with default
     *                           values is used.
     * @param lookBackTime       the maximum time to look back when fetching new events while tracking. If {@code null}
     *                           a 10 second interval is used.
     */
    public AbstractMongoEventStorageStrategy(EventEntryConfiguration eventConfiguration, Duration lookBackTime) {
        this.eventConfiguration = getOrDefault(eventConfiguration, EventEntryConfiguration.getDefault());
        this.lookBackTime = getOrDefault(lookBackTime, Duration.ofMillis(1000L));
    }

    @Override
    public void appendEvents(MongoCollection<Document> eventCollection, List<? extends EventMessage<?>> events,
                             Serializer serializer) {
        eventCollection.insertMany(createEventDocuments(events, serializer).collect(Collectors.toList()));
    }

    /**
     * Returns a stream of Mongo documents that represent the given batch of events. The given list of {@code events}
     * represents events produced in the context of a single unit of work. Uses the given {@code serializer} to
     * serialize event payload and metadata.
     *
     * @param events     the events to convert to Mongo documents
     * @param serializer the serializer to convert the events' payload and metadata
     * @return stream of Mongo documents from the given event batch
     */
    protected abstract Stream<Document> createEventDocuments(List<? extends EventMessage<?>> events,
                                                             Serializer serializer);

    @Override
    public void appendSnapshot(MongoCollection<Document> snapshotCollection, DomainEventMessage<?> snapshot,
                               Serializer serializer) {
        snapshotCollection.findOneAndReplace(
                new BsonDocument(
                        eventConfiguration.aggregateIdentifierProperty(),
                        new BsonString(snapshot.getAggregateIdentifier())
                ),
                createSnapshotDocument(snapshot, serializer),
                new FindOneAndReplaceOptions().upsert(true)
        );
    }

    /**
     * Returns a Mongo document for given snapshot event. Uses the given {@code serializer} to serialize event payload
     * and metadata.
     *
     * @param snapshot   the snapshot to convert
     * @param serializer the to convert the snapshot's payload and metadata
     * @return a Mongo documents from given snapshot
     */
    protected abstract Document createSnapshotDocument(DomainEventMessage<?> snapshot, Serializer serializer);

    @Override
    public void deleteSnapshots(MongoCollection<Document> snapshotCollection, String aggregateIdentifier,
                                long sequenceNumber) {
        snapshotCollection.deleteMany(and(eq(eventConfiguration.aggregateIdentifierProperty(), aggregateIdentifier),
                                          lt(eventConfiguration.sequenceNumberProperty(), sequenceNumber)));
    }

    @Override
    public List<? extends DomainEventData<?>> findDomainEvents(MongoCollection<Document> collection,
                                                               String aggregateIdentifier, long firstSequenceNumber,
                                                               int batchSize) {
        FindIterable<Document> cursor = collection
                .find(and(eq(eventConfiguration.aggregateIdentifierProperty(), aggregateIdentifier),
                          gte(eventConfiguration.sequenceNumberProperty(), firstSequenceNumber)))
                .sort(new BasicDBObject(eventConfiguration().sequenceNumberProperty(), ORDER_ASC));
        cursor = cursor.batchSize(batchSize);
        return stream(cursor.spliterator(), false).flatMap(this::extractEvents)
                                                  .filter(event -> event.getSequenceNumber() >= firstSequenceNumber)
                                                  .collect(Collectors.toList());
    }

    /**
     * Retrieves event data from the given Mongo {@code object}.
     *
     * @param object the object to convert to event data
     * @return stream of events from given document
     */
    protected abstract Stream<? extends DomainEventData<?>> extractEvents(Document object);

    @Override
    public List<? extends TrackedEventData<?>> findTrackedEvents(MongoCollection<Document> eventCollection,
                                                                 TrackingToken lastToken, int batchSize) {
        FindIterable<Document> cursor;
        if (lastToken == null) {
            cursor = eventCollection.find();
        } else {
            Assert.isTrue(lastToken instanceof MongoTrackingToken,
                          () -> String.format("Token %s is of the wrong type", lastToken));
            //noinspection ConstantConditions
            MongoTrackingToken trackingToken = (MongoTrackingToken) lastToken;
            cursor = eventCollection.find(and(gte(eventConfiguration.timestampProperty(),
                                                  formatInstant(trackingToken.getTimestamp().minus(lookBackTime))),
                                              nin(eventConfiguration.eventIdentifierProperty(),
                                                  trackingToken.getKnownEventIds())));
        }
        cursor = cursor.sort(new BasicDBObject(eventConfiguration().timestampProperty(), ORDER_ASC)
                                     .append(eventConfiguration().sequenceNumberProperty(), ORDER_ASC));
        cursor = cursor.batchSize(batchSize);
        //noinspection ConstantConditions
        AtomicReference<MongoTrackingToken> previousToken = new AtomicReference<>((MongoTrackingToken) lastToken);
        List<TrackedEventData<?>> results = new ArrayList<>();
        try (MongoCursor<Document> iterator = cursor.iterator()) {
            while (results.size() < batchSize && iterator.hasNext()) {
                Document document = iterator.next();
                extractEvents(document)
                        .filter(ed -> previousToken.get() == null
                                || !previousToken.get().getKnownEventIds().contains(ed.getEventIdentifier()))
                        .map(event -> new TrackedMongoEventEntry<>(event, previousToken.updateAndGet(
                                token -> token == null
                                        ? MongoTrackingToken.of(event.getTimestamp(), event.getEventIdentifier())
                                        : token.advanceTo(event.getTimestamp(), event.getEventIdentifier(), lookBackTime))))
                        .forEach(results::add);
            }
        }
        return results;
    }

    @Override
    public Stream<? extends DomainEventData<?>> findSnapshots(MongoCollection<Document> snapshotCollection,
                                                              String aggregateIdentifier) {
        FindIterable<Document> cursor =
                snapshotCollection.find(eq(eventConfiguration.aggregateIdentifierProperty(), aggregateIdentifier))
                                  .sort(orderBy(descending(eventConfiguration.sequenceNumberProperty())));
        return stream(cursor.spliterator(), false).map(this::extractSnapshot);
    }

    @Override
    public Optional<Long> lastSequenceNumberFor(MongoCollection<Document> eventsCollection,
                                                String aggregateIdentifier) {
        Document lastDocument =
                eventsCollection.find(eq(eventConfiguration.aggregateIdentifierProperty(), aggregateIdentifier))
                                .sort(descending(eventConfiguration.sequenceNumberProperty()))
                                .first();
        return Optional.ofNullable(lastDocument).map(this::extractHighestSequenceNumber);
    }

    @Override
    public TrackingToken createTailToken(MongoCollection<Document> eventsCollection) {
        Document first = eventsCollection.find()
                                         .sort(Sorts.ascending(eventConfiguration.timestampProperty()))
                                         .first();
        return Optional.ofNullable(first)
                       .map(d -> d.get(eventConfiguration.timestampProperty()))
                       .map(t -> parseInstant((String) t))
                       .map(t -> MongoTrackingToken.of(t, Collections.emptyMap()))
                       .orElse(null);
    }

    /**
     * Retrieves snapshot event data from the given Mongo {@code object}.
     *
     * @param object the object to convert to snapshot data
     * @return snapshot data contained in given document
     */
    protected abstract DomainEventData<?> extractSnapshot(Document object);

    /**
     * Extract the highest sequence number known from the entry represented by the given {@code document}.
     * <p>
     * This implementation takes the {@code sequenceNumberProperty} defined in the {@code eventConfiguration}.
     * Implementations that allow storage of multiple events in a single document should override this method.
     *
     * @param document The document representing the entry stored in Mongo
     * @return a Long representing the highest sequence number found
     */
    protected Long extractHighestSequenceNumber(Document document) {
        return (Long) document.get(eventConfiguration.sequenceNumberProperty());
    }

    @Override
    public void ensureIndexes(MongoCollection<Document> eventsCollection,
                              MongoCollection<Document> snapshotsCollection) {
        eventsCollection.createIndex(new BasicDBObject(eventConfiguration.aggregateIdentifierProperty(), ORDER_ASC)
                                             .append(eventConfiguration.sequenceNumberProperty(), ORDER_ASC),
                                     new IndexOptions().unique(true).name("uniqueAggregateIndex"));

        eventsCollection.createIndex(new BasicDBObject(eventConfiguration.timestampProperty(), ORDER_ASC)
                                             .append(eventConfiguration.sequenceNumberProperty(), ORDER_ASC),
                                     new IndexOptions().unique(false).name("orderedEventStreamIndex"));
        snapshotsCollection.createIndex(new BasicDBObject(eventConfiguration.aggregateIdentifierProperty(), ORDER_ASC)
                                                .append(eventConfiguration.sequenceNumberProperty(), ORDER_ASC),
                                        new IndexOptions().unique(true).name("uniqueAggregateIndex"));
    }

    /**
     * Returns the {@link EventEntryConfiguration} that configures how event entries are to be stored.
     *
     * @return the event entry configuration
     */
    protected EventEntryConfiguration eventConfiguration() {
        return eventConfiguration;
    }

    private static class TrackedMongoEventEntry<T> implements DomainEventData<T>, TrackedEventData<T> {

        private final DomainEventData<T> delegate;
        private final TrackingToken trackingToken;

        public TrackedMongoEventEntry(DomainEventData<T> delegate, TrackingToken trackingToken) {
            this.delegate = delegate;
            this.trackingToken = trackingToken;
        }

        @Override
        public String getType() {
            return delegate.getType();
        }

        @Override
        public String getAggregateIdentifier() {
            return delegate.getAggregateIdentifier();
        }

        @Override
        public long getSequenceNumber() {
            return delegate.getSequenceNumber();
        }

        @Override
        public TrackingToken trackingToken() {
            return trackingToken;
        }

        @Override
        public String getEventIdentifier() {
            return delegate.getEventIdentifier();
        }

        @Override
        public Instant getTimestamp() {
            return delegate.getTimestamp();
        }

        @Override
        public SerializedObject<T> getMetaData() {
            return delegate.getMetaData();
        }

        @Override
        public SerializedObject<T> getPayload() {
            return delegate.getPayload();
        }
    }
}
