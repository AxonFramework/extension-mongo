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

import com.mongodb.client.MongoCursor;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.transaction.NoTransactionManager;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.extensions.mongo.MongoTemplate;
import org.axonframework.extensions.mongo.eventsourcing.eventstore.documentperevent.EventEntryConfiguration;
import org.axonframework.messaging.Message;
import org.axonframework.messaging.deadletter.Cause;
import org.axonframework.messaging.deadletter.DeadLetter;
import org.axonframework.messaging.deadletter.DeadLetterQueueOverflowException;
import org.axonframework.messaging.deadletter.EnqueueDecision;
import org.axonframework.messaging.deadletter.GenericDeadLetter;
import org.axonframework.messaging.deadletter.NoSuchDeadLetterException;
import org.axonframework.messaging.deadletter.SequencedDeadLetterQueue;
import org.axonframework.messaging.deadletter.WrongDeadLetterTypeException;
import org.axonframework.serialization.Serializer;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import javax.annotation.Nonnull;

import static java.util.Objects.isNull;
import static org.axonframework.common.BuilderUtils.*;
import static org.axonframework.extensions.mongo.eventhandling.deadletter.DeadLetterEntry.*;

/**
 * Mongo backed implementation of the {@link SequencedDeadLetterQueue}, used for storing dead letters containing
 * {@link EventMessage Eventmessages} durably as a {@link DeadLetterEntry}.
 * <p>
 * Keeps the insertion order intact by saving an incremented index within each unique sequence, backed by the
 * {@link DeadLetterEntry#getSequenceIdentifier()} property. Each sequence is uniquely identified by the sequence
 * identifier, stored in the {@link DeadLetterEntry#getSequenceIdentifier()} field.
 * <p>
 * When processing an item, single execution across all applications is guaranteed by setting the
 * {@link DeadLetterEntry#getProcessingStarted()} property, locking other processes out of the sequence for the
 * configured {@code claimDuration} (30 seconds by default).
 * <p>
 * The stored {@link DeadLetterEntry entries} are converted to a {@link MongoDeadLetter} when they need to be processed
 * or filtered. In order to restore the original {@link EventMessage} a matching {@link DeadLetterMongoConverter} is
 * used. The default supports all {@code EventMessage} implementations provided by the framework. If you have a custom
 * variant, you have to build your own.
 * <p>
 * {@link org.axonframework.serialization.upcasting.Upcaster upcasters} are not supported by this implementation, so
 * breaking changes for events messages stored in the queue should be avoided.
 *
 * @param <M> An implementation of {@link Message} contained in the {@link DeadLetter dead-letters} within this queue.
 * @author Gerard Klijs
 * @since 4.7.0
 */
public class MongoSequencedDeadLetterQueue<M extends EventMessage<?>> implements SequencedDeadLetterQueue<M> {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final String processingGroup;
    private final List<DeadLetterMongoConverter<EventMessage<?>>> converters;
    private final EventEntryConfiguration eventConfiguration;
    private final int maxSequences;
    private final int maxSequenceSize;
    private final MongoTemplate mongoTemplate;
    private final Serializer serializer;
    private final Duration claimDuration;
    private final TransactionManager transactionManager;

    /**
     * Instantiate a Mongo {@link SequencedDeadLetterQueue} based on the given
     * {@link MongoSequencedDeadLetterQueue.Builder builder}.
     *
     * @param builder The {@link MongoSequencedDeadLetterQueue.Builder} used to instantiate a
     *                {@link MongoSequencedDeadLetterQueue} instance.
     */
    protected <T extends EventMessage<?>> MongoSequencedDeadLetterQueue(Builder<T> builder) {
        builder.validate();
        this.processingGroup = builder.processingGroup;
        this.converters = builder.converters;
        this.eventConfiguration = builder.eventConfiguration;
        this.maxSequences = builder.maxSequences;
        this.maxSequenceSize = builder.maxSequenceSize;
        this.mongoTemplate = builder.mongoTemplate;
        this.serializer = builder.serializer;
        this.claimDuration = builder.claimDuration;
        this.transactionManager = builder.transactionManager;
        ensureDeadLetterIndexes(mongoTemplate.deadLetterCollection());
    }

    /**
     * Creates a new builder, capable of building a {@link MongoSequencedDeadLetterQueue} according to the provided
     * configuration. Note that the {@link MongoSequencedDeadLetterQueue.Builder#processingGroup(String)},
     * {@link MongoSequencedDeadLetterQueue.Builder#mongoTemplate} and
     * {@link MongoSequencedDeadLetterQueue.Builder#serializer(Serializer)} are mandatory for the queue to be
     * constructed.
     *
     * @param <M> An implementation of {@link Message} contained in the {@link DeadLetter dead-letters} within this
     *            queue.
     * @return The builder
     */
    public static <M extends EventMessage<?>> MongoSequencedDeadLetterQueue.Builder<M> builder() {
        return new MongoSequencedDeadLetterQueue.Builder<>();
    }

    @Override
    public void enqueue(@Nonnull Object sequenceIdentifier, @Nonnull DeadLetter<? extends M> letter)
            throws DeadLetterQueueOverflowException {
        String stringSequenceIdentifier = toStringSequenceIdentifier(sequenceIdentifier);
        if (isFull(stringSequenceIdentifier)) {
            throw new DeadLetterQueueOverflowException(
                    "No room left to enqueue [" + letter.message() + "] for identifier ["
                            + stringSequenceIdentifier + "] since the queue is full."
            );
        }

        Optional<Cause> optionalCause = letter.cause();
        if (optionalCause.isPresent()) {
            logger.info("Adding dead letter [{}] because [{}].", letter.message(), optionalCause.get());
        } else {
            logger.info("Adding dead letter [{}] because the sequence identifier [{}] is already present.",
                        letter.message(), stringSequenceIdentifier);
        }

        DeadLetterEventEntry entry = converters
                .stream()
                .filter(c -> c.canConvert(letter.message()))
                .findFirst()
                .map(c -> c.convert(letter.message(), serializer))
                .orElseThrow(() -> new NoMongoConverterFoundException(
                        String.format("No converter found for message of type: [%s]",
                                      letter.message().getClass().getName()))
                );
        Document message = entry.asDocument(eventConfiguration);
        Long sequenceIndex = getNextIndexForSequence(stringSequenceIdentifier);
        logger.info("Storing DeadLetter (id: [{}]) for sequence [{}] with index [{}] in processing group [{}].",
                    entry.getEventIdentifier(),
                    stringSequenceIdentifier,
                    sequenceIndex,
                    processingGroup);
        DeadLetterEntry deadLetter = new DeadLetterEntry(
                processingGroup,
                stringSequenceIdentifier,
                sequenceIndex,
                message,
                letter.enqueuedAt(),
                letter.lastTouched(),
                letter.cause().orElse(null),
                letter.diagnostics(),
                serializer);
        Document document = deadLetter.asDocument();
        transactionManager.executeInTransaction(
                () -> mongoTemplate.deadLetterCollection().insertOne(document)
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    public void evict(DeadLetter<? extends M> letter) {
        if (!(letter instanceof MongoDeadLetter)) {
            throw new WrongDeadLetterTypeException(
                    String.format("Evict should be called with a MongoDeadLetter instance. Instead got: [%s]",
                                  letter.getClass().getName()));
        }
        MongoDeadLetter<M> deadLetter = (MongoDeadLetter<M>) letter;
        if (logger.isInfoEnabled()) {
            logger.info("Trying to evict MongoDeadLetter with processing group {} sequence {} and index {}",
                        processingGroup,
                        deadLetter.sequenceIdentifier(),
                        deadLetter.index());
        }
        DeleteResult result = transactionManager.fetchInTransaction(
                () -> mongoTemplate.deadLetterCollection().deleteOne(findOneFilter(
                        processingGroup,
                        deadLetter.sequenceIdentifier(),
                        deadLetter.index()))
        );
        if (logger.isInfoEnabled()) {
            if (result.getDeletedCount() == 1) {
                logger.info("Successfully evict MongoDeadLetter with processing group {} sequence {} and index {}",
                            processingGroup,
                            deadLetter.sequenceIdentifier(),
                            deadLetter.index());
            } else {
                logger.info(
                        "Failed to evict MongoDeadLetter with processing group {} sequence {} and index {} with result {}",
                        processingGroup,
                        deadLetter.sequenceIdentifier(),
                        deadLetter.index(),
                        result);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void requeue(@Nonnull DeadLetter<? extends M> letter,
                        @Nonnull UnaryOperator<DeadLetter<? extends M>> letterUpdater)
            throws NoSuchDeadLetterException {
        if (!(letter instanceof MongoDeadLetter)) {
            throw new WrongDeadLetterTypeException(String.format(
                    "Requeue should be called with a MongoDeadLetter instance. Instead got: [%s]",
                    letter.getClass().getName()));
        }
        DeadLetter<? extends M> updatedLetter = letterUpdater.apply(letter).markTouched();
        MongoDeadLetter<? extends M> mongoDeadLetter = (MongoDeadLetter<? extends M>) letter;
        Document letterEntityDocument = transactionManager.fetchInTransaction(
                () -> mongoTemplate
                        .deadLetterCollection()
                        .find(findOneFilter(
                                processingGroup,
                                mongoDeadLetter.sequenceIdentifier(),
                                mongoDeadLetter.index()))
                        .first()
        );
        if (letterEntityDocument == null) {
            throw new NoSuchDeadLetterException(
                    String.format(
                            "Can not find dead letter with processing group [%s], sequence identifier [%s] and index [%s] to requeue.",
                            processingGroup,
                            mongoDeadLetter.sequenceIdentifier(),
                            mongoDeadLetter.index()));
        }
        DeadLetterEntry letterEntity = new DeadLetterEntry(letterEntityDocument);
        letterEntity.setDiagnostics(updatedLetter.diagnostics(), serializer);
        letterEntity.setLastTouched(updatedLetter.lastTouched());
        updatedLetter.cause().ifPresent(letterEntity::setCause);
        letterEntity.clearProcessingStarted();
        if (logger.isInfoEnabled()) {
            logger.info(
                    "Requeueing dead letter with processing group [{}], sequence identifier [{}] and index [{}] with cause [{}]",
                    processingGroup,
                    mongoDeadLetter.sequenceIdentifier(),
                    mongoDeadLetter.index(),
                    updatedLetter.cause());
        }
        transactionManager.executeInTransaction(
                () -> mongoTemplate.deadLetterCollection().findOneAndReplace(
                        findOneFilter(
                                processingGroup,
                                mongoDeadLetter.sequenceIdentifier(),
                                mongoDeadLetter.index()),
                        letterEntity.asDocument()
                )
        );
    }

    @Override
    public boolean contains(@Nonnull Object sequenceIdentifier) {
        String stringSequenceIdentifier = toStringSequenceIdentifier(sequenceIdentifier);
        return sequenceSize(stringSequenceIdentifier) > 0;
    }

    @Override
    public Iterable<DeadLetter<? extends M>> deadLetterSequence(@Nonnull Object sequenceIdentifier) {
        String stringSequenceIdentifier = toStringSequenceIdentifier(sequenceIdentifier);
        return transactionManager.fetchInTransaction(
                () -> mongoTemplate.deadLetterCollection()
                                   .find(processingGroupAndSequenceIdentifierFilter(processingGroup,
                                                                                    stringSequenceIdentifier))
                                   .sort(indexSortAscending())
                                   .map(DeadLetterEntry::new)
                                   .map(this::toLetter)
        );
    }

    @Override
    public Iterable<Iterable<DeadLetter<? extends M>>> deadLetters() {
        return transactionManager.fetchInTransaction(
                () -> sequenceIdentifierIterator(mongoTemplate.deadLetterCollection(), processingGroup)
                        .map(sequenceIdentifier -> {
                            assertNonNull(sequenceIdentifier, "SequenceIdentifier can not be null.");
                            return deadLetterSequence(sequenceIdentifier);
                        })
        );
    }

    /**
     * Converts a {@link DeadLetterEntry} from the database into a {@link MongoDeadLetter}, using the configured
     * {@link DeadLetterMongoConverter DeadLetterMongoConverters} to restore the original message from it.
     *
     * @param entry The entry to convert.
     * @return The {@link DeadLetter} result.
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    private MongoDeadLetter<M> toLetter(DeadLetterEntry entry) {
        DeadLetterEventEntry deadLetterEventEntry = entry.getMessage(eventConfiguration);
        DeadLetterMongoConverter<M> converter = (DeadLetterMongoConverter<M>) converters
                .stream()
                .filter(c -> c.canConvert(deadLetterEventEntry))
                .findFirst()
                .orElseThrow(() -> new NoMongoConverterFoundException(String.format(
                        "No converter found to convert message of class [%s].",
                        deadLetterEventEntry.getMessageType())));
        return new MongoDeadLetter<>(entry,
                                     serializer.deserialize(entry.getDiagnostics()),
                                     converter.convert(deadLetterEventEntry, serializer));
    }

    @Override
    public boolean isFull(@Nonnull Object sequenceIdentifier) {
        String stringSequenceIdentifier = toStringSequenceIdentifier(sequenceIdentifier);
        long numberInSequence = sequenceSize(stringSequenceIdentifier);
        if (numberInSequence > 0) {
            // Is already in queue, cannot cause overflow any longer.
            return numberInSequence >= maxSequenceSize;
        }
        return amountOfSequences() >= maxSequences;
    }

    @Override
    public long size() {
        return transactionManager.fetchInTransaction(
                () -> mongoTemplate.deadLetterCollection()
                                   .countDocuments(processingGroupFilter(processingGroup))
        );
    }

    @Override
    public long sequenceSize(@Nonnull Object sequenceIdentifier) {
        String identifier = toStringSequenceIdentifier(sequenceIdentifier);
        return transactionManager.fetchInTransaction(
                () -> mongoTemplate.deadLetterCollection()
                                   .countDocuments(processingGroupAndSequenceIdentifierFilter(processingGroup,
                                                                                              identifier))
        );
    }

    @Override
    public long amountOfSequences() {
        AtomicLong counter = new AtomicLong(0L);
        transactionManager.executeInTransaction(
                () -> sequenceIdentifierIterator(mongoTemplate.deadLetterCollection(), processingGroup)
                        .forEach(i -> counter.incrementAndGet())
        );
        return counter.get();
    }

    @Override
    public boolean process(@Nonnull Predicate<DeadLetter<? extends M>> sequenceFilter,
                           @Nonnull Function<DeadLetter<? extends M>, EnqueueDecision<M>> processingTask) {
        final AtomicReference<MongoDeadLetter<M>> claimedLetter = new AtomicReference<>();
        transactionManager.executeInTransaction(
                () -> {
                    try (MongoCursor<Document> iterator = mongoTemplate
                            .deadLetterCollection()
                            .aggregate(firstNotLockedFilter(processingGroup, getProcessingStartedLimit()))
                            .iterator()) {
                        while (iterator.hasNext() && claimedLetter.get() == null) {
                            Document id = iterator.next();
                            Document document = mongoTemplate.deadLetterCollection().find(id).first();
                            if (isNull(document) || isLocked(getProcessingStartedLimit(), document)) {
                                continue;
                            }
                            MongoDeadLetter<M> current = toLetter(new DeadLetterEntry(document));
                            if (sequenceFilter.test(current) && claimDeadLetter(current)) {
                                claimedLetter.set(current);
                            }
                        }
                    }
                });
        if (claimedLetter.get() != null) {
            return processLetterAndFollowing(claimedLetter.get(), processingTask);
        }
        logger.info("No claimable and/or matching dead letters found to process.");
        return false;
    }

    /**
     * Processes the given {@code firstDeadLetter} using the provided {@code processingTask}. When successful (the
     * message is evicted) it will automatically process all messages in the same
     * {@link MongoDeadLetter#sequenceIdentifier()}, evicting messages that succeed and stopping when the first one
     * fails (and is requeued).
     * <p>
     * Will claim the next letter in the same sequence before removing the old one to prevent concurrency issues.
     *
     * @param firstDeadLetter The dead letter to start processing.
     * @param processingTask  The task to use to process the dead letter, providing a decision afterwards.
     * @return Whether processing all letters in this sequence was successful.
     */
    private boolean processLetterAndFollowing(MongoDeadLetter<M> firstDeadLetter,
                                              Function<DeadLetter<? extends M>, EnqueueDecision<M>> processingTask) {
        MongoDeadLetter<M> deadLetter = firstDeadLetter;
        while (deadLetter != null) {
            if (logger.isInfoEnabled()) {
                logger.info("Processing dead letter with sequence identifier [{}] and index [{}]",
                            deadLetter.sequenceIdentifier(),
                            deadLetter.index());
            }
            EnqueueDecision<M> decision = processingTask.apply(deadLetter);
            if (!decision.shouldEnqueue()) {
                MongoDeadLetter<M> oldLetter = deadLetter;
                Document deadLetterDocument = findNextDeadLetter(oldLetter);
                if (deadLetterDocument != null) {
                    deadLetter = toLetter(new DeadLetterEntry(deadLetterDocument));
                    claimDeadLetter(deadLetter);
                } else {
                    deadLetter = null;
                }
                evict(oldLetter);
            } else {
                requeue(deadLetter,
                        l -> decision.withDiagnostics(l)
                                     .withCause(decision.enqueueCause().orElse(null))
                );
                return false;
            }
        }
        return true;
    }

    /**
     * Finds the next dead letter after the provided {@code oldLetter} in the database. This is the message in the same
     * {@code processingGroup} and {@code sequence}, but with the next index.
     *
     * @param oldLetter The base letter to search from.
     * @return The next letter to process.
     */
    private Document findNextDeadLetter(MongoDeadLetter<M> oldLetter) {
        return transactionManager.fetchInTransaction(
                () -> mongoTemplate
                        .deadLetterCollection()
                        .find(nextItemInSequenceFilter(processingGroup,
                                                       oldLetter.sequenceIdentifier(),
                                                       oldLetter.index()))
                        .sort(indexSortAscending())
                        .limit(1)
                        .first()
        );
    }

    /**
     * Claims the provided {@link DeadLetter} in the database by setting the {@code processingStarted} property. Will
     * check whether it was claimed successfully and return an appropriate boolean result.
     *
     * @return Whether the letter was successfully claimed or not.
     */
    private boolean claimDeadLetter(MongoDeadLetter<M> deadLetter) {
        Instant processingStartedLimit = getProcessingStartedLimit();
        UpdateResult result = transactionManager.fetchInTransaction(
                () -> mongoTemplate.deadLetterCollection().updateOne(
                        uniqueNotLockedFilter(
                                processingGroup,
                                deadLetter.sequenceIdentifier(),
                                deadLetter.index(),
                                processingStartedLimit),
                        updateProcessingStarted(GenericDeadLetter.clock.instant()))
        );

        if (result.getMatchedCount() > 0) {
            logger.info("Claimed dead letter with id [{}] to process.", result.getUpsertedId());
            return true;
        }
        if (logger.isInfoEnabled()) {
            logger.info("Failed to claim dead letter with sequence identifier [{}] and index [{}].",
                        deadLetter.sequenceIdentifier(),
                        deadLetter.index());
        }
        return false;
    }

    /**
     * Determines the time the {@link DeadLetterEntry#getProcessingStarted()} needs to at least have to stay claimed.
     * This is based on the configured {@link #claimDuration}.
     */
    private Instant getProcessingStartedLimit() {
        return GenericDeadLetter.clock.instant().minus(claimDuration);
    }

    @Override
    public void clear() {
        transactionManager.executeInTransaction(
                () -> mongoTemplate.deadLetterCollection().deleteMany(processingGroupFilter(processingGroup))
        );
    }

    /**
     * Fetches the next maximum index for a sequence that should be used when inserting a new item into the database for
     * this {@code sequence}.
     *
     * @param sequenceIdentifier The identifier of the sequence to fetch the next index for.
     * @return The next sequence index.
     */
    private Long getNextIndexForSequence(String sequenceIdentifier) {
        Long maxIndex = getMaxIndexForSequence(sequenceIdentifier);
        if (maxIndex == null) {
            return 0L;
        }
        return maxIndex + 1;
    }

    /**
     * Fetches the current maximum index for a queue identifier. Messages which are enqueued next should have an index
     * higher than the one returned. If the query returns null it indicates that the queue is empty.
     *
     * @param sequenceIdentifier The identifier of the sequence to check the index for.
     * @return The current maximum index, or null if not present.
     */
    private Long getMaxIndexForSequence(String sequenceIdentifier) {
        Document deadLetterEntry = transactionManager.fetchInTransaction(
                () -> mongoTemplate
                        .deadLetterCollection()
                        .find(processingGroupAndSequenceIdentifierFilter(processingGroup, sequenceIdentifier))
                        .sort(indexSortDescending())
                        .limit(1)
                        .first()
        );
        return index(deadLetterEntry);
    }

    /**
     * Converts the given sequence identifier to a String.
     */
    private String toStringSequenceIdentifier(Object sequenceIdentifier) {
        if (sequenceIdentifier instanceof String) {
            return (String) sequenceIdentifier;
        }
        return Integer.toString(sequenceIdentifier.hashCode());
    }

    /**
     * Builder class to instantiate an {@link MongoSequencedDeadLetterQueue}.
     * <p>
     * The maximum number of unique sequences defaults to {@code 1024}, the maximum amount of dead letters inside a
     * unique sequence to {@code 1024}, the claim duration defaults to {@code 30} seconds, the query page size defaults
     * to {@code 100}, and the converters default to containing a single {@link EventMessageDeadLetterMongoConverter}.
     * <p>
     * If you have custom {@link EventMessage} to use with this queue, replace the current (or add a second) converter.
     * <p>
     * The {@link TransactionManager} is defaulted to a {@link NoTransactionManager}.
     * <p>
     * The {@code processingGroup}, {@link MongoTemplate}, and {@link Serializer} have to be configured for the
     * {@link MongoSequencedDeadLetterQueue} to be constructed.
     *
     * @param <T> The type of {@link Message} maintained in this {@link MongoSequencedDeadLetterQueue}.
     */
    public static class Builder<T extends EventMessage<?>> {

        private final List<DeadLetterMongoConverter<EventMessage<?>>> converters = new LinkedList<>();
        private EventEntryConfiguration eventConfiguration = EventEntryConfiguration.getDefault();
        private String processingGroup = null;
        private int maxSequences = 1024;
        private int maxSequenceSize = 1024;
        private MongoTemplate mongoTemplate;
        private Serializer serializer;
        private Duration claimDuration = Duration.ofSeconds(30);
        private TransactionManager transactionManager = NoTransactionManager.instance();

        public Builder() {
            converters.add(new EventMessageDeadLetterMongoConverter());
        }

        /**
         * Sets the processing group, which is used for storing and querying which event processor the deadlettered item
         * belonged to.
         *
         * @param processingGroup The processing group of this {@link SequencedDeadLetterQueue}.
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<T> processingGroup(String processingGroup) {
            assertNonEmpty(processingGroup, "Can not set processingGroup to an empty String.");
            this.processingGroup = processingGroup;
            return this;
        }

        /**
         * Sets the event configuration, which is used for storing the events
         *
         * @param eventConfiguration The event configuration of this {@link SequencedDeadLetterQueue}.
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<T> eventConfiguration(EventEntryConfiguration eventConfiguration) {
            assertNonNull(eventConfiguration, "EventEntryConfiguration may not be null");
            this.eventConfiguration = eventConfiguration;
            return this;
        }

        /**
         * Sets the maximum number of unique sequences this {@link SequencedDeadLetterQueue} may contain.
         * <p>
         * The given {@code maxSequences} is required to be a positive number, higher or equal to {@code 128}. It
         * defaults to {@code 1024}.
         *
         * @param maxSequences The maximum amount of unique sequences for the queue under construction.
         * @return The current Builder, for fluent interfacing.
         */
        public Builder<T> maxSequences(int maxSequences) {
            assertStrictPositive(maxSequences,
                                 "The maximum number of sequences should be larger or equal to 0");
            this.maxSequences = maxSequences;
            return this;
        }

        /**
         * Sets the maximum amount of {@link DeadLetter letters} per unique sequences this
         * {@link SequencedDeadLetterQueue} can store.
         * <p>
         * The given {@code maxSequenceSize} is required to be a positive number, higher or equal to {@code 128}. It
         * defaults to {@code 1024}.
         *
         * @param maxSequenceSize The maximum amount of {@link DeadLetter letters} per unique  sequence.
         * @return The current Builder, for fluent interfacing.
         */
        public Builder<T> maxSequenceSize(int maxSequenceSize) {
            assertStrictPositive(maxSequenceSize,
                                 "The maximum number of entries in a sequence should be larger or equal to 128");
            this.maxSequenceSize = maxSequenceSize;
            return this;
        }

        /**
         * Sets the {@link MongoTemplate} providing access to the collections.
         *
         * @param mongoTemplate the {@link MongoTemplate} providing access to the collections
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<T> mongoTemplate(MongoTemplate mongoTemplate) {
            assertNonNull(mongoTemplate, "MongoTemplate may not be null");
            this.mongoTemplate = mongoTemplate;
            return this;
        }

        /**
         * Sets the {@link Serializer} to deserialize the events, metadata and diagnostics of the {@link DeadLetter}
         * when storing it to a database.
         *
         * @param serializer The serializer to use
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<T> serializer(Serializer serializer) {
            assertNonNull(serializer, "The serializer may not be null");
            this.serializer = serializer;
            return this;
        }

        /**
         * Removes all current converters currently configured, including the default
         * {@link EventMessageDeadLetterMongoConverter}.
         *
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<T> clearConverters() {
            this.converters.clear();
            return this;
        }

        /**
         * Adds a {@link DeadLetterMongoConverter} to the configuration, which is used to deserialize dead-letter
         * entries from the database.
         *
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<T> addConverter(
                DeadLetterMongoConverter<EventMessage<?>> converter) {
            assertNonNull(converter, "Can not add a null DeadLetterMongoConverter.");
            this.converters.add(converter);
            return this;
        }

        /**
         * Sets the claim duration, which is the time a message gets locked when processing and waiting for it to
         * complete. Other invocations of the {@link #process(Predicate, Function)} method will be unable to process a
         * sequence while the claim is active. Its default is 30 seconds.
         * <p>
         * Claims are automatically released once the item is requeued, the claim time is a backup policy in case of
         * unforeseen trouble such as down database connections.
         *
         * @param claimDuration The longest claim duration allowed.
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<T> claimDuration(Duration claimDuration) {
            assertNonNull(claimDuration, "Claim duration can not be set to null.");
            this.claimDuration = claimDuration;
            return this;
        }

        /**
         * Sets the {@link TransactionManager} used to manage transaction around fetching tokens. Will default to
         * {@link NoTransactionManager}, which effectively will not use transactions.
         *
         * @param transactionManager a {@link TransactionManager} used to manage transaction around fetching event data
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<T> transactionManager(TransactionManager transactionManager) {
            assertNonNull(transactionManager, "TransactionManager may not be null");
            this.transactionManager = transactionManager;
            return this;
        }

        /**
         * Initializes a {@link MongoSequencedDeadLetterQueue} as specified through this Builder.
         *
         * @return A {@link MongoSequencedDeadLetterQueue} as specified through this Builder.
         */
        public MongoSequencedDeadLetterQueue<T> build() {
            return new MongoSequencedDeadLetterQueue<>(this);
        }

        /**
         * Validate whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException When one field asserts to be incorrect according to the Builder's
         *                                    specifications.
         */
        protected void validate() {
            assertNonEmpty(processingGroup,
                           "Must supply processingGroup when constructing a MongoSequencedDeadLetterQueue");
            assertNonNull(mongoTemplate,
                          "Must supply a Mongo template when constructing a MongoSequencedDeadLetterQueue");
            assertNonNull(serializer,
                          "Must supply a Serializer when constructing a MongoSequencedDeadLetterQueue");
        }
    }
}
