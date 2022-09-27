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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Projections;
import org.axonframework.extensions.mongo.eventsourcing.eventstore.documentperevent.EventEntryConfiguration;
import org.axonframework.messaging.MetaData;
import org.axonframework.messaging.deadletter.Cause;
import org.axonframework.serialization.SerializedMetaData;
import org.axonframework.serialization.Serializer;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Updates.set;
import static org.axonframework.extensions.mongo.eventhandling.deadletter.InstantEntry.NANOSECONDS_KEY;
import static org.axonframework.extensions.mongo.eventhandling.deadletter.InstantEntry.SECONDS_KEY;

/**
 * Default DeadLetter Mongo entity implementation of dead letters. Used by the {@link MongoSequencedDeadLetterQueue} to
 * store these into the database to be retried later.
 * <p>
 * The original message is embedded as a {@link DeadLetterEventEntry}. This is mapped, upon both storage and retrieval,
 * by one of the configured {@link DeadLetterMongoConverter converters}.
 *
 * @author Gerard Klijs
 * @since 4.7.0
 */
public class DeadLetterEntry {

    private static final int ORDER_ASC = 1;

    private static final String PROCESSING_GROUP_KEY = "processingGroup";
    private static final String SEQUENCE_IDENTIFIER_KEY = "sequenceIdentifier";
    private static final String INDEX_KEY = "index";
    private static final String MESSAGE_KEY = "message";
    private static final String ENQUEUED_AT_KEY = "enqueuedAt";
    private static final String LAST_TOUCHED_KEY = "lastTouched";
    private static final String LAST_TOUCHED_SECONDS_KEY = LAST_TOUCHED_KEY + "." + SECONDS_KEY;
    private static final String LAST_TOUCHED_NANOSECONDS_KEY = LAST_TOUCHED_KEY + "." + NANOSECONDS_KEY;
    private static final String PROCESSING_STARTED_KEY = "processingStarted";
    private static final String PROCESSING_STARTED_SECONDS_KEY = PROCESSING_STARTED_KEY + "." + SECONDS_KEY;
    private static final String PROCESSING_STARTED_NANOSECONDS_KEY = PROCESSING_STARTED_KEY + "." + NANOSECONDS_KEY;
    private static final String CAUSE_TYPE_KEY = "causeType";
    private static final String CAUSE_MESSAGE_KEY = "causeMessage";
    private static final String DIAGNOSTICS_KEY = "diagnostics";

    private final String processingGroup;
    private final String sequenceIdentifier;
    private final long index;
    private final Document message;
    private final Document enqueuedAt;
    private Document lastTouched;
    private Document processingStarted;
    private String causeType;
    private String causeMessage;
    private Object serializedDiagnostics;

    /**
     * Initializes a {@link DeadLetterEntry} entry using a DBObject containing the Mongo Document.
     *
     * @param dbDeadLetterEntry The mongo Document containing the serialized dead letter.
     */
    public DeadLetterEntry(Document dbDeadLetterEntry) {
        this.processingGroup = dbDeadLetterEntry.getString(PROCESSING_GROUP_KEY);
        this.sequenceIdentifier = dbDeadLetterEntry.getString(SEQUENCE_IDENTIFIER_KEY);
        this.index = dbDeadLetterEntry.getLong(INDEX_KEY);
        this.message = (Document) dbDeadLetterEntry.get(MESSAGE_KEY);
        this.enqueuedAt = (Document) dbDeadLetterEntry.get(ENQUEUED_AT_KEY);
        this.lastTouched = (Document) dbDeadLetterEntry.get(LAST_TOUCHED_KEY);
        this.processingStarted = (Document) dbDeadLetterEntry.get(PROCESSING_STARTED_KEY);
        this.causeType = dbDeadLetterEntry.getString(CAUSE_TYPE_KEY);
        this.causeMessage = dbDeadLetterEntry.getString(CAUSE_MESSAGE_KEY);
        this.serializedDiagnostics = dbDeadLetterEntry.get(DIAGNOSTICS_KEY);
    }

    /**
     * Creates a new {@link DeadLetterEntry} consisting of the given parameters.
     *
     * @param processingGroup    The processing group this message belongs to.
     * @param sequenceIdentifier The sequence identifier this message belongs to.
     * @param index              The index of this message within the sequence.
     * @param message            The message stored as {@link Document}.
     * @param enqueuedAt         The time the message was enqueued.
     * @param lastTouched        The time the message has been last processed.
     * @param cause              The reason the message was enqueued.
     * @param diagnostics        The diagnostics, a map of metadata.
     * @param serializer         Serializer to use for the diagnostics
     */
    @SuppressWarnings("squid:S107")
    public DeadLetterEntry(String processingGroup,
                           String sequenceIdentifier,
                           long index,
                           Document message,
                           Instant enqueuedAt,
                           Instant lastTouched,
                           Cause cause,
                           MetaData diagnostics,
                           Serializer serializer) {
        this.processingGroup = processingGroup;
        this.sequenceIdentifier = sequenceIdentifier;
        this.index = index;
        this.message = message;
        this.enqueuedAt = new InstantEntry(enqueuedAt).asDocument();
        this.lastTouched = new InstantEntry(lastTouched).asDocument();
        Optional.ofNullable(cause).ifPresent(c -> {
            this.causeType = c.type();
            this.causeMessage = c.message();
        });
        Class<?> serializationTarget = String.class;
        if (serializer.canSerializeTo(DBObject.class)) {
            serializationTarget = DBObject.class;
        }
        this.serializedDiagnostics = serializer.serialize(diagnostics, serializationTarget).getData();
    }

    /**
     * Returns the current dead letter as a mongo Document.
     *
     * @return Document representing the entry.
     */
    public Document asDocument() {
        return new Document()
                .append(PROCESSING_GROUP_KEY, processingGroup)
                .append(SEQUENCE_IDENTIFIER_KEY, sequenceIdentifier)
                .append(INDEX_KEY, index)
                .append(MESSAGE_KEY, message)
                .append(ENQUEUED_AT_KEY, enqueuedAt)
                .append(LAST_TOUCHED_KEY, lastTouched)
                .append(PROCESSING_STARTED_KEY, processingStarted)
                .append(CAUSE_TYPE_KEY, causeType)
                .append(CAUSE_MESSAGE_KEY, causeMessage)
                .append(DIAGNOSTICS_KEY, serializedDiagnostics);
    }

    /**
     * The processing group this dead letter belongs to.
     *
     * @return The processing group.
     */
    public String getProcessingGroup() {
        return processingGroup;
    }

    /**
     * The sequence identifier of this dead letter. If two have the same, they must be handled sequentially.
     *
     * @return The sequence identifier.
     */
    public String getSequenceIdentifier() {
        return sequenceIdentifier;
    }

    /**
     * The index of this message within the {@link #getSequenceIdentifier()}, used for keeping the messages in the same
     * order within the same sequence.
     *
     * @return The index.
     */
    public long getIndex() {
        return index;
    }

    /**
     * The embedded {@link DeadLetterEventEntry} representing the original message.
     *
     * @return The embedded original message.
     */
    public DeadLetterEventEntry getMessage(@Nonnull EventEntryConfiguration configuration) {
        return new DeadLetterEventEntry(message, configuration);
    }

    /**
     * The time the message was enqueued.
     *
     * @return The time the message was enqueued.
     */
    public Instant getEnqueuedAt() {
        return new InstantEntry(enqueuedAt).getInstant();
    }

    /**
     * The time the messages was last touched, meaning having been queued or having been tried to process.
     *
     * @return The time the messages was last touched.
     */
    public Instant getLastTouched() {
        return new InstantEntry(lastTouched).getInstant();
    }

    /**
     * Sets the time the message was last touched. Should be updated every time the message has been attempted to
     * process.
     *
     * @param lastTouched The new time to set to.
     */
    public void setLastTouched(@Nonnull Instant lastTouched) {
        this.lastTouched = new InstantEntry(lastTouched).asDocument();
    }

    /**
     * Timestamp indicating when the processing of this dead letter has started. Used for claiming messages and
     * preventing multiple processes or thread from handling items concurrently within the same sequence.
     *
     * @return Timestamp of start processing.
     */
    public Instant getProcessingStarted() {
        return Optional.ofNullable(processingStarted)
                       .map(InstantEntry::new)
                       .map(InstantEntry::getInstant)
                       .orElse(null);
    }


    /**
     * Sets the cause of the error when the message was originally processed, or processed later and the cause was
     * updated.
     *
     * @param cause The new cause to set to.
     */
    public void setCause(@Nonnull Cause cause) {
        this.causeType = cause.type();
        this.causeMessage = cause.message();
    }

    /**
     * Gets the class of the original exception.
     *
     * @return The type of the cause.
     */
    public String getCauseType() {
        return causeType;
    }

    /**
     * Gets the message of the original exception.
     *
     * @return The message of the cause.
     */
    public String getCauseMessage() {
        return causeMessage;
    }

    /**
     * Returns the serialized diagnostics.
     *
     * @return The serialized diagnostics.
     */
    @SuppressWarnings({"unchecked", "rawtypes", "squid:S1452"})
    public SerializedMetaData<?> getDiagnostics() {
        return new SerializedMetaData(serializedDiagnostics, getRepresentationType());
    }

    /**
     * Sets the diagnostics.
     *
     * @param diagnostics The new diagnostics.
     */
    public void setDiagnostics(@Nonnull MetaData diagnostics, @Nonnull Serializer serializer) {
        Class<?> serializationTarget = String.class;
        if (serializer.canSerializeTo(DBObject.class)) {
            serializationTarget = DBObject.class;
        }
        this.serializedDiagnostics = serializer.serialize(diagnostics, serializationTarget).getData();
    }

    /**
     * Releases the message for processing by another thread or process.
     */
    public void clearProcessingStarted() {
        this.processingStarted = null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DeadLetterEntry that = (DeadLetterEntry) o;

        return Objects.equals(processingGroup, that.processingGroup) &&
                Objects.equals(sequenceIdentifier, that.sequenceIdentifier) &&
                Objects.equals(index, that.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processingGroup, sequenceIdentifier, index);
    }

    @Override
    public String toString() {
        return "DeadLetterEntry{" +
                "processingGroup='" + processingGroup + '\'' +
                ", sequenceIdentifier='" + sequenceIdentifier + '\'' +
                ", index=" + index +
                ", message=" + message +
                ", enqueuedAt=" + enqueuedAt +
                ", lastTouched=" + lastTouched +
                ", processingStarted=" + processingStarted +
                ", causeType='" + causeType + '\'' +
                ", causeMessage='" + causeMessage + '\'' +
                ", diagnostics=" + serializedDiagnostics +
                '}';
    }

    /**
     * Sets some indexes needed for the dead letter collection.
     *
     * @param deadLetterCollection the {@link MongoCollection} to create the indexes on.
     */
    public static void ensureDeadLetterIndexes(@Nonnull MongoCollection<Document> deadLetterCollection) {
        deadLetterCollection.createIndex(
                new BasicDBObject(PROCESSING_GROUP_KEY, ORDER_ASC),
                new IndexOptions().unique(false).name("processingGroupIdentifierIndex"));
        deadLetterCollection.createIndex(
                new BasicDBObject(PROCESSING_GROUP_KEY, ORDER_ASC)
                        .append(SEQUENCE_IDENTIFIER_KEY, ORDER_ASC),
                new IndexOptions().unique(false).name("processingGroupAndSequenceIdentifierIndex"));
        deadLetterCollection.createIndex(
                new BasicDBObject(PROCESSING_GROUP_KEY, ORDER_ASC)
                        .append(SEQUENCE_IDENTIFIER_KEY, ORDER_ASC)
                        .append(INDEX_KEY, ORDER_ASC),
                new IndexOptions().unique(true).name("uniqueDeadLetterEntryIndex"));
    }

    /**
     * Filter only on processingGroup
     *
     * @param processingGroup the processing group
     * @return a {@link Bson} filter.
     */
    public static Bson processingGroupFilter(@Nonnull String processingGroup) {
        return eq(PROCESSING_GROUP_KEY, processingGroup);
    }

    /**
     * Filter on processingGroup and sequenceIdentifier.
     *
     * @param processingGroup    the processing group.
     * @param sequenceIdentifier the sequence identifier.
     * @return a {@link Bson} filter.
     */
    public static Bson processingGroupAndSequenceIdentifierFilter(
            @Nonnull String processingGroup,
            @Nonnull String sequenceIdentifier
    ) {
        return and(eq(PROCESSING_GROUP_KEY, processingGroup),
                   eq(SEQUENCE_IDENTIFIER_KEY, sequenceIdentifier));
    }

    /**
     * Filter on processingGroup, sequenceIdentifier and index.
     *
     * @param processingGroup    the processing group.
     * @param sequenceIdentifier the sequence identifier.
     * @param index              the index.
     * @return a {@link Bson} filter.
     */
    public static Bson findOneFilter(
            @Nonnull String processingGroup,
            @Nonnull String sequenceIdentifier,
            long index
    ) {
        return and(eq(PROCESSING_GROUP_KEY, processingGroup),
                   eq(SEQUENCE_IDENTIFIER_KEY, sequenceIdentifier),
                   eq(INDEX_KEY, index));
    }

    /**
     * Filter on processingGroup, sequenceIdentifier where the index needs to be higher than the provided
     * {@code index}.
     *
     * @param processingGroup    the processing group.
     * @param sequenceIdentifier the sequence identifier.
     * @param index              the index.
     * @return a {@link Bson} filter.
     */
    public static Bson nextItemInSequenceFilter(
            @Nonnull String processingGroup,
            @Nonnull String sequenceIdentifier,
            long index
    ) {
        return and(eq(PROCESSING_GROUP_KEY, processingGroup),
                   eq(SEQUENCE_IDENTIFIER_KEY, sequenceIdentifier),
                   gt(INDEX_KEY, index));
    }

    /**
     * Filter on processingGroup, sequenceIdentifier, index and processingStarted. The processingGroup,
     * sequenceIdentifier and index should match. The processingStarted is either absent or earlier in time than the
     * given {@code processingStartedLimit}.
     *
     * @param processingGroup        the processing group.
     * @param sequenceIdentifier     the sequence identifier.
     * @param index                  the index.
     * @param processingStartedLimit the processing started limit.
     * @return a {@link Bson} filter.
     */
    public static Bson uniqueNotLockedFilter(
            @Nonnull String processingGroup,
            @Nonnull String sequenceIdentifier,
            long index,
            @Nonnull Instant processingStartedLimit
    ) {
        return and(eq(PROCESSING_GROUP_KEY, processingGroup),
                   eq(SEQUENCE_IDENTIFIER_KEY, sequenceIdentifier),
                   eq(INDEX_KEY, index),
                   or(exists(PROCESSING_STARTED_SECONDS_KEY, false),
                      lt(PROCESSING_STARTED_SECONDS_KEY, processingStartedLimit.getEpochSecond()),
                      and(
                              eq(PROCESSING_STARTED_SECONDS_KEY, processingStartedLimit.getEpochSecond()),
                              lt(PROCESSING_STARTED_NANOSECONDS_KEY, processingStartedLimit.getNano())
                      )));
    }

    /**
     * Provides a pipeline which can be used in an aggregate to get the id's of dead letters where, only the processing
     * group is included, for each queue only the first, with the lowest index is considered, it should be claimable,
     * and the result is sorted by last touched. The pipeline can be used in a {@link MongoCollection#aggregate(List)}.
     *
     * @param processingGroup        the processing group.
     * @param processingStartedLimit the processing started limit.
     * @return a {@link Bson} pipeline – the aggregation pipeline.
     */
    @SuppressWarnings("squid:S1452")
    public static List<? extends Bson> firstNotLockedFilter(
            @Nonnull String processingGroup,
            @Nonnull Instant processingStartedLimit
    ) {
        Document result = new Document()
                .append(LAST_TOUCHED_KEY, "$" + LAST_TOUCHED_KEY)
                .append(PROCESSING_STARTED_KEY, "$" + PROCESSING_STARTED_KEY)
                .append("_id", "$_id");

        return Arrays.asList(
                Aggregates.match(eq(PROCESSING_GROUP_KEY, processingGroup)),
                Aggregates.group("$" + SEQUENCE_IDENTIFIER_KEY,
                                 Accumulators.first(INDEX_KEY, result)),
                Aggregates.replaceRoot("$" + INDEX_KEY),
                Aggregates.match(or(exists(PROCESSING_STARTED_SECONDS_KEY, false),
                                    lt(PROCESSING_STARTED_SECONDS_KEY,
                                       processingStartedLimit.getEpochSecond()),
                                    and(eq(PROCESSING_STARTED_SECONDS_KEY,
                                           processingStartedLimit.getEpochSecond()),
                                        lt(PROCESSING_STARTED_NANOSECONDS_KEY,
                                           processingStartedLimit.getNano())
                                    ))),
                Aggregates.sort(ascending(LAST_TOUCHED_SECONDS_KEY, LAST_TOUCHED_NANOSECONDS_KEY)),
                Aggregates.project(Projections.fields(Projections.include("_id")))
        );
    }

    /**
     * {@link Bson} update the processing started value to the given {@code now} value.
     *
     * @param now the current time as instant
     * @return a {@link Bson} filter
     */
    public static Bson updateProcessingStarted(
            @Nonnull Instant now
    ) {
        return set(PROCESSING_STARTED_KEY, new InstantEntry(now).asDocument());
    }

    /**
     * Get an iterator for all the sequence identifiers of a certain processing group. These are no guarantees on the
     * ordering.
     *
     * @param collection      the {@link MongoCollection} to distinct on.
     * @param processingGroup the processing group.
     * @return a {@link DistinctIterable} with sequence identifiers.
     */
    public static DistinctIterable<String> sequenceIdentifierIterator(
            @Nonnull MongoCollection<Document> collection,
            @Nonnull String processingGroup
    ) {
        return collection.distinct(
                SEQUENCE_IDENTIFIER_KEY,
                processingGroupFilter(processingGroup),
                String.class
        );
    }

    /**
     * Sorts by index descending, so the highest first.
     *
     * @return a {@link Bson} sort.
     */
    public static Bson indexSortDescending() {
        return descending(INDEX_KEY);
    }

    /**
     * Sorts by index ascending, so the lowest first.
     *
     * @return a {@link Bson} sort.
     */
    public static Bson indexSortAscending() {
        return ascending(INDEX_KEY);
    }

    /**
     * Returns the index if present on the document, returns {@code null} when not present or document is null.
     *
     * @param document a {@link Document} which is expected to be a {@link DeadLetterEntry} representation.
     * @return the index retrieved, or null.
     */
    public static Long index(@Nullable Document document) {
        return Optional.ofNullable(document)
                       .map(d -> d.getLong(INDEX_KEY))
                       .orElse(null);
    }

    /**
     * Returns whether this dead letter is locked, as check before trying to claim it.
     *
     * @param processingStartedLimit the processing started limit.
     * @param dbDeadLetterEntry      the {@link DeadLetterEntry} as raw mongo {@link Document}.
     * @return if the document is currently locked.
     */
    public static boolean isLocked(@Nonnull Instant processingStartedLimit, @Nonnull Document dbDeadLetterEntry) {
        return Optional.ofNullable((Document) dbDeadLetterEntry.get(PROCESSING_STARTED_KEY))
                       .map(InstantEntry::new)
                       .map(InstantEntry::getInstant)
                       .map(ps -> ps.isAfter(processingStartedLimit))
                       .orElse(false);
    }

    private Class<?> getRepresentationType() {
        Class<?> representationType = String.class;
        if (serializedDiagnostics instanceof DBObject) {
            representationType = DBObject.class;
        } else if (serializedDiagnostics instanceof Document) {
            representationType = Document.class;
        }
        return representationType;
    }
}
