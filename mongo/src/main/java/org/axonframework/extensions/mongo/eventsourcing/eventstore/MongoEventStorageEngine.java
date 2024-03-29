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

package org.axonframework.extensions.mongo.eventsourcing.eventstore;

import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoBulkWriteException;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.jdbc.PersistenceExceptionResolver;
import org.axonframework.common.transaction.NoTransactionManager;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.eventhandling.DomainEventData;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.TrackedEventData;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventsourcing.eventstore.BatchingEventStorageEngine;
import org.axonframework.eventsourcing.snapshotting.SnapshotFilter;
import org.axonframework.extensions.mongo.MongoTemplate;
import org.axonframework.extensions.mongo.eventsourcing.eventstore.documentperevent.DocumentPerEventStorageStrategy;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.upcasting.event.EventUpcaster;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * EventStorageEngine implementation that uses Mongo to store and fetch events.
 *
 * @author Rene de Waele
 * @since 3.0
 */
public class MongoEventStorageEngine extends BatchingEventStorageEngine {

    private final MongoTemplate template;
    private final StorageStrategy storageStrategy;
    private final TransactionManager transactionManager;

    /**
     * Instantiate a {@link MongoEventStorageEngine} based on the fields contained in the {@link Builder}.
     * <p>
     * Will assert that the {@link MongoTemplate} is not {@code null}, and will throw an
     * {@link AxonConfigurationException} if it is {@code null}.
     *
     * @param builder the {@link Builder} used to instantiate a {@link MongoEventStorageEngine} instance
     */
    protected MongoEventStorageEngine(Builder builder) {
        super(builder);
        this.template = builder.template;
        this.storageStrategy = builder.storageStrategy;
        this.transactionManager = builder.transactionManager;
        ensureIndexes();
    }

    /**
     * Instantiate a Builder to be able to create a {@link MongoEventStorageEngine}.
     * <p>
     * The following configurable fields have defaults:
     * <ul>
     * <li>The snapshot {@link Serializer} defaults to {@link org.axonframework.serialization.xml.XStreamSerializer}.</li>
     * <li>The {@link EventUpcaster} defaults to an {@link org.axonframework.serialization.upcasting.event.NoOpEventUpcaster}.</li>
     * <li>The {@link PersistenceExceptionResolver} is defaulted to {@link MongoEventStorageEngine#isDuplicateKeyException(Exception)}</li>
     * <li>The event Serializer defaults to a {@link org.axonframework.serialization.xml.XStreamSerializer}.</li>
     * <li>The {@code snapshotFilter} defaults to a {@link Predicate} which returns {@code true} regardless.</li>
     * <li>The {@code batchSize} defaults to an integer of size {@code 100}.</li>
     * <li>The {@link StorageStrategy} defaults to a {@link DocumentPerEventStorageStrategy}.</li>
     * <li>The {@link TransactionManager} defaults to a {@link NoTransactionManager}.</li>
     * </ul>
     * <p>
     * The {@link MongoTemplate} is a <b>hard requirement</b> and as such should be provided.
     *
     * @return a Builder to be able to create a {@link MongoEventStorageEngine}
     */
    public static Builder builder() {
        return new Builder();
    }

    private static boolean isDuplicateKeyException(Exception exception) {
        return exception instanceof DuplicateKeyException || (exception instanceof MongoBulkWriteException &&
                ((MongoBulkWriteException) exception).getWriteErrors().stream().anyMatch(e -> e.getCode() == 11000));
    }

    /**
     * Make sure an index is created on the collection that stores domain events.
     *
     * @deprecated This method is now called by the constructor instead of the dependency injection framework running
     * the @PostConstruct. i.e. You no longer have to call it manually if you don't use a dependency injection
     * framework.
     */
    @Deprecated
    public void ensureIndexes() {
        transactionManager.executeInTransaction(
                () -> storageStrategy.ensureIndexes(template.eventCollection(), template.snapshotCollection())
        );
    }

    @Override
    protected void appendEvents(List<? extends EventMessage<?>> events, Serializer serializer) {
        if (!events.isEmpty()) {
            try {
                transactionManager.executeInTransaction(
                        () -> storageStrategy.appendEvents(template.eventCollection(), events, serializer)
                );
            } catch (Exception e) {
                handlePersistenceException(e, events.get(0));
            }
        }
    }

    @Override
    protected void storeSnapshot(DomainEventMessage<?> snapshot, Serializer serializer) {
        try {
            transactionManager.executeInTransaction(
                    () -> {
                        storageStrategy.appendSnapshot(
                                template.snapshotCollection(),
                                snapshot,
                                serializer);
                        storageStrategy.deleteSnapshots(
                                template.snapshotCollection(),
                                snapshot.getAggregateIdentifier(),
                                snapshot.getSequenceNumber()
                        );
                    }
            );
        } catch (Exception e) {
            handlePersistenceException(e, snapshot);
        }
    }

    @Override
    protected Stream<? extends DomainEventData<?>> readSnapshotData(String aggregateIdentifier) {
        return transactionManager.fetchInTransaction(
                () -> storageStrategy.findSnapshots(template.snapshotCollection(), aggregateIdentifier)
        );
    }

    @Override
    protected List<? extends DomainEventData<?>> fetchDomainEvents(String aggregateIdentifier, long firstSequenceNumber,
                                                                   int batchSize) {
        return transactionManager.fetchInTransaction(
                () -> storageStrategy
                        .findDomainEvents(template.eventCollection(),
                                          aggregateIdentifier,
                                          firstSequenceNumber,
                                          batchSize)
        );
    }

    @Override
    protected List<? extends TrackedEventData<?>> fetchTrackedEvents(TrackingToken lastToken, int batchSize) {
        return transactionManager.fetchInTransaction(
                () -> storageStrategy.findTrackedEvents(template.eventCollection(), lastToken, batchSize)
        );
    }

    @Override
    public Optional<Long> lastSequenceNumberFor(@Nonnull String aggregateIdentifier) {
        return transactionManager.fetchInTransaction(
                () -> storageStrategy.lastSequenceNumberFor(template.eventCollection(), aggregateIdentifier)
        );
    }

    @Override
    public TrackingToken createTailToken() {
        return transactionManager.fetchInTransaction(
                () -> storageStrategy.createTailToken(template.eventCollection())
        );
    }

    @Override
    public TrackingToken createHeadToken() {
        return createTokenAt(Instant.now());
    }

    @Override
    public TrackingToken createTokenAt(@Nonnull Instant dateTime) {
        return MongoTrackingToken.of(dateTime, Collections.emptyMap());
    }

    /**
     * Builder class to instantiate a {@link MongoEventStorageEngine}.
     * <p>
     * The following configurable fields have defaults:
     * <ul>
     * <li>The snapshot {@link Serializer} defaults to {@link org.axonframework.serialization.xml.XStreamSerializer}.</li>
     * <li>The {@link EventUpcaster} defaults to an {@link org.axonframework.serialization.upcasting.event.NoOpEventUpcaster}.</li>
     * <li>The {@link PersistenceExceptionResolver} is defaulted to {@link MongoEventStorageEngine#isDuplicateKeyException(Exception)}</li>
     * <li>The event Serializer defaults to a {@link org.axonframework.serialization.xml.XStreamSerializer}.</li>
     * <li>The {@code snapshotFilter} defaults to a {@link Predicate} which returns {@code true} regardless.</li>
     * <li>The {@code batchSize} defaults to an integer of size {@code 100}.</li>
     * <li>The {@link StorageStrategy} defaults to a {@link DocumentPerEventStorageStrategy}.</li>
     * <li>The {@link TransactionManager} defaults to a {@link NoTransactionManager}.</li>
     * </ul>
     * <p>
     * The {@link MongoTemplate} is a <b>hard requirement</b> and as such should be provided.
     */
    public static class Builder extends BatchingEventStorageEngine.Builder {

        private MongoTemplate template;
        private StorageStrategy storageStrategy = new DocumentPerEventStorageStrategy();
        private TransactionManager transactionManager = NoTransactionManager.instance();

        private Builder() {
            persistenceExceptionResolver(MongoEventStorageEngine::isDuplicateKeyException);
        }

        @Override
        public Builder snapshotSerializer(Serializer snapshotSerializer) {
            super.snapshotSerializer(snapshotSerializer);
            return this;
        }

        @Override
        public Builder upcasterChain(EventUpcaster upcasterChain) {
            super.upcasterChain(upcasterChain);
            return this;
        }

        @Override
        public Builder persistenceExceptionResolver(PersistenceExceptionResolver persistenceExceptionResolver) {
            super.persistenceExceptionResolver(persistenceExceptionResolver);
            return this;
        }

        @Override
        public Builder eventSerializer(Serializer eventSerializer) {
            super.eventSerializer(eventSerializer);
            return this;
        }

        /**
         * {@inheritDoc}
         *
         * @deprecated in favor of {@link #snapshotFilter(SnapshotFilter)}
         */
        @Override
        @Deprecated
        public Builder snapshotFilter(Predicate<? super DomainEventData<?>> snapshotFilter) {
            super.snapshotFilter(snapshotFilter);
            return this;
        }

        @Override
        public Builder snapshotFilter(SnapshotFilter snapshotFilter) {
            super.snapshotFilter(snapshotFilter);
            return this;
        }

        @Override
        public Builder batchSize(int batchSize) {
            super.batchSize(batchSize);
            return this;
        }

        /**
         * Sets the {@link MongoTemplate} used to obtain the database and the collections.
         *
         * @param template the {@link MongoTemplate} used to obtain the database and the collections
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder mongoTemplate(MongoTemplate template) {
            assertNonNull(template, "MongoTemplate may not be null");
            this.template = template;
            return this;
        }

        /**
         * Sets the {@link StorageStrategy} specifying how to store and retrieve events and snapshots from the
         * collections. Defaults to a {@link DocumentPerEventStorageStrategy}, causing every event and snapshot to be
         * stored in a separate Mongo Document.
         *
         * @param storageStrategy the {@link StorageStrategy} specifying how to store and retrieve events and snapshots
         *                        from the collections
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder storageStrategy(StorageStrategy storageStrategy) {
            assertNonNull(storageStrategy, "StorageStrategy may not be null");
            this.storageStrategy = storageStrategy;
            return this;
        }

        /**
         * Sets the {@link TransactionManager} used to manage transaction around fetching tokens. Will default to
         * {@link NoTransactionManager}, which effectively will not use transactions.
         *
         * @param transactionManager a {@link TransactionManager} used to manage transaction around fetching event data
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder transactionManager(TransactionManager transactionManager) {
            assertNonNull(transactionManager, "TransactionManager may not be null");
            this.transactionManager = transactionManager;
            return this;
        }

        /**
         * Initializes a {@link MongoEventStorageEngine} as specified through this Builder.
         *
         * @return a {@link MongoEventStorageEngine} as specified through this Builder
         */
        public MongoEventStorageEngine build() {
            return new MongoEventStorageEngine(this);
        }

        /**
         * Validates whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException if one field is asserted to be incorrect according to the Builder's
         *                                    specifications
         */
        @Override
        protected void validate() throws AxonConfigurationException {
            super.validate();
            assertNonNull(template, "The MongoTemplate is a hard requirement and should be provided");
        }
    }
}
