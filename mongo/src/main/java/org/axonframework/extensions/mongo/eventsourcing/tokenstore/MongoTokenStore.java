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

package org.axonframework.extensions.mongo.eventsourcing.tokenstore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.BuilderUtils;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventhandling.tokenstore.AbstractTokenEntry;
import org.axonframework.eventhandling.tokenstore.ConfigToken;
import org.axonframework.eventhandling.tokenstore.GenericTokenEntry;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.UnableToClaimTokenException;
import org.axonframework.eventhandling.tokenstore.UnableToInitializeTokenException;
import org.axonframework.eventhandling.tokenstore.UnableToRetrieveIdentifierException;
import org.axonframework.extensions.mongo.MongoTemplate;
import org.axonframework.serialization.Serializer;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import static java.lang.String.format;
import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * An implementation of TokenStore that allows you store and retrieve tracking tokens with MongoDB.
 *
 * @author Joris van der Kallen
 * @since 3.1
 */
public class MongoTokenStore implements TokenStore {

    private static final Logger logger = LoggerFactory.getLogger(MongoTokenStore.class);
    private static final Clock clock = Clock.systemUTC();

    private static final String CONFIG_TOKEN_ID = "__config";
    private static final int CONFIG_SEGMENT = 0;

    private static final String OWNER_PROPERTY_NAME = "owner";
    private static final String TIMESTAMP_PROPERTY_NAME = "timestamp";
    private static final String PROCESSOR_NAME_PROPERTY_NAME = "processorName";
    private static final String SEGMENT_PROPERTY_NAME = "segment";
    private static final String TOKEN_PROPERTY_NAME = "token";
    private static final String TOKEN_TYPE_PROPERTY_NAME = "tokenType";

    private final MongoTemplate mongoTemplate;
    private final Serializer serializer;
    private final TemporalAmount claimTimeout;
    private final String nodeId;
    private final Class<?> contentType;

    /**
     * Instantiate a {@link MongoTokenStore} based on the fields contained in the {@link Builder}.
     * <p>
     * Will assert that the {@link MongoTemplate} and {@link Serializer} are not {@code null}, and will throw an {@link
     * AxonConfigurationException} if any of them is {@code null}.
     *
     * @param builder the {@link Builder} used to instantiate a {@link MongoTokenStore} instance
     */
    protected MongoTokenStore(Builder builder) {
        builder.validate();
        this.mongoTemplate = builder.mongoTemplate;
        this.serializer = builder.serializer;
        this.claimTimeout = builder.claimTimeout;
        this.nodeId = builder.nodeId;
        this.contentType = builder.contentType;
        if (builder.ensureIndexes) {
            ensureIndexes();
        }
    }

    /**
     * Instantiate a Builder to be able to create a {@link MongoTokenStore}.
     * <p>
     * The {@code claimTimeout} is defaulted to a 10 seconds duration (by using {@link Duration#ofSeconds(long)}, {@code
     * nodeId} is defaulted to the {@code ManagementFactory#getRuntimeMXBean#getName} output and the {@code contentType}
     * to a {@code byte[]} {@link Class}. The {@link MongoTemplate} and {@link Serializer} are
     * <b>hard requirements</b> and as such should be provided.
     *
     * @return a Builder to be able to create a {@link MongoTokenStore}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeToken(TrackingToken token, String processorName, int segment) throws UnableToClaimTokenException {
        updateToken(token, processorName, segment);
    }

    private void updateToken(TrackingToken token, String processorName, int segment) {
        AbstractTokenEntry<?> tokenEntry =
                new GenericTokenEntry<>(token, serializer, contentType, processorName, segment);
        tokenEntry.claim(nodeId, claimTimeout);

        Bson update = combine(set(OWNER_PROPERTY_NAME, nodeId),
                              set(TIMESTAMP_PROPERTY_NAME, tokenEntry.timestamp().toEpochMilli()),
                              set(TOKEN_PROPERTY_NAME, tokenEntry.getSerializedToken().getData()),
                              set(TOKEN_TYPE_PROPERTY_NAME, tokenEntry.getSerializedToken().getType().getName()));

        UpdateResult updateResult = mongoTemplate.trackingTokensCollection()
                                                 .updateOne(claimableTokenEntryFilter(processorName, segment), update);

        if (updateResult.getModifiedCount() == 0) {
            throw new UnableToClaimTokenException(format(
                    "Unable to claim token '%s[%s]'. It is either already claimed or it does not exist",
                    processorName, segment
            ));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initializeTokenSegments(String processorName, int segmentCount) throws UnableToClaimTokenException {
        initializeTokenSegments(processorName, segmentCount, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initializeTokenSegments(String processorName,
                                        int segmentCount,
                                        TrackingToken initialToken) throws UnableToClaimTokenException {
        if (fetchSegments(processorName).length > 0) {
            throw new UnableToClaimTokenException(
                    "Unable to initialize segments. Some tokens were already present for the given processor."
            );
        }

        List<Document> entries = IntStream.range(0, segmentCount)
                                          .mapToObj(segment -> new GenericTokenEntry<>(initialToken, serializer,
                                                                                       contentType, processorName,
                                                                                       segment))
                                          .map(this::tokenEntryToDocument)
                                          .collect(Collectors.toList());
        mongoTemplate.trackingTokensCollection()
                     .insertMany(entries, new InsertManyOptions().ordered(false));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TrackingToken fetchToken(String processorName, int segment) throws UnableToClaimTokenException {
        return loadToken(processorName, segment).getToken(serializer);
    }

    private AbstractTokenEntry<?> loadToken(String processorName, int segment) {
        Document document = mongoTemplate.trackingTokensCollection()
                                         .findOneAndUpdate(
                                                 claimableTokenEntryFilter(processorName, segment),
                                                 combine(set(OWNER_PROPERTY_NAME, nodeId),
                                                         set(TIMESTAMP_PROPERTY_NAME, clock.millis())),
                                                 new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)
                                         );

        if (document == null) {
            throw new UnableToClaimTokenException(
                    format("Unable to claim token '%s[%s]'. It has not been initialized yet", processorName, segment)
            );
        }

        AbstractTokenEntry<?> tokenEntry = documentToTokenEntry(document);
        if (!tokenEntry.claim(nodeId, claimTimeout)) {
            throw new UnableToClaimTokenException(format(
                    "Unable to claim token '%s[%s]'. It is owned by '%s'", processorName, segment, tokenEntry.getOwner()
            ));
        }

        return tokenEntry;
    }

    @Override
    public void extendClaim(String processorName, int segment) throws UnableToClaimTokenException {
        UpdateResult updateResult =
                mongoTemplate.trackingTokensCollection()
                             .updateOne(and(eq(PROCESSOR_NAME_PROPERTY_NAME, processorName),
                                            eq(SEGMENT_PROPERTY_NAME, segment),
                                            eq(OWNER_PROPERTY_NAME, nodeId)),
                                        set(TIMESTAMP_PROPERTY_NAME, clock.instant().toEpochMilli()));
        if (updateResult.getMatchedCount() == 0) {
            throw new UnableToClaimTokenException(format(
                    "Unable to extend claim on token token '%s[%s]'. It is owned by another segment.",
                    processorName, segment
            ));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void releaseClaim(String processorName, int segment) {
        UpdateResult updateResult = mongoTemplate.trackingTokensCollection()
                                                 .updateOne(and(
                                                         eq(PROCESSOR_NAME_PROPERTY_NAME, processorName),
                                                         eq(SEGMENT_PROPERTY_NAME, segment),
                                                         eq(OWNER_PROPERTY_NAME, nodeId)
                                                 ), set(OWNER_PROPERTY_NAME, null));
        if (updateResult.getMatchedCount() == 0) {
            logger.warn("Releasing claim of token {}/{} failed. It was owned by another node.", processorName, segment);
        }
    }

    @Override
    public void initializeSegment(TrackingToken token,
                                  String processorName,
                                  int segment) throws UnableToInitializeTokenException {
        try {
            AbstractTokenEntry<?> tokenEntry =
                    new GenericTokenEntry<>(token, serializer, contentType, processorName, segment);

            mongoTemplate.trackingTokensCollection()
                         .insertOne(tokenEntryToDocument(tokenEntry));
        } catch (MongoWriteException exception) {
            if (ErrorCategory.fromErrorCode(exception.getError().getCode()) == ErrorCategory.DUPLICATE_KEY) {
                throw new UnableToInitializeTokenException(
                        format("Unable to initialize token '%s[%s]'", processorName, segment)
                );
            }
        }
    }

    @Override
    public void deleteToken(String processorName, int segment) throws UnableToClaimTokenException {
        DeleteResult deleteResult = mongoTemplate.trackingTokensCollection()
                                                 .deleteOne(and(
                                                         eq(PROCESSOR_NAME_PROPERTY_NAME, processorName),
                                                         eq(SEGMENT_PROPERTY_NAME, segment),
                                                         eq(OWNER_PROPERTY_NAME, nodeId)
                                                 ));

        if (deleteResult.getDeletedCount() == 0) {
            throw new UnableToClaimTokenException("Unable to remove token. It is not owned by " + nodeId);
        }
    }

    @Override
    public boolean requiresExplicitSegmentInitialization() {
        return true;
    }

    @Override
    public int[] fetchSegments(String processorName) {
        ArrayList<Integer> segments = mongoTemplate.trackingTokensCollection()
                                                   .find(eq(PROCESSOR_NAME_PROPERTY_NAME, processorName))
                                                   .sort(ascending(SEGMENT_PROPERTY_NAME))
                                                   .projection(fields(include(SEGMENT_PROPERTY_NAME), excludeId()))
                                                   .map(d -> d.get(SEGMENT_PROPERTY_NAME, Integer.class))
                                                   .into(new ArrayList<>());
        // toArray doesn't work because of autoboxing limitations
        int[] ints = new int[segments.size()];
        for (int i = 0; i < ints.length; i++) {
            ints[i] = segments.get(i);
        }
        return ints;
    }

    @Override
    public Optional<String> retrieveStorageIdentifier() throws UnableToRetrieveIdentifierException {
        try {
            return Optional.of(getConfig()).map(configToken -> configToken.get("id"));
        } catch (Exception e) {
            throw new UnableToRetrieveIdentifierException(
                    "Exception occurred while trying to establish storage identifier", e
            );
        }
    }

    private ConfigToken getConfig() {
        Document document = mongoTemplate.trackingTokensCollection()
                                         .find(and(
                                                 eq(PROCESSOR_NAME_PROPERTY_NAME, CONFIG_TOKEN_ID),
                                                 eq(SEGMENT_PROPERTY_NAME, CONFIG_SEGMENT)
                                         ))
                                         .first();
        AbstractTokenEntry<?> token;

        if (Objects.isNull(document)) {
            token = new GenericTokenEntry<>(
                    new ConfigToken(Collections.singletonMap("id", UUID.randomUUID().toString())),
                    serializer, contentType, CONFIG_TOKEN_ID, CONFIG_SEGMENT
            );
            mongoTemplate.trackingTokensCollection()
                         .insertOne(tokenEntryToDocument(token));
        } else {
            token = documentToTokenEntry(document);
        }

        return (ConfigToken) token.getToken(serializer);
    }

    /**
     * Creates a filter that allows you to retrieve a claimable token entry with a given processor name and segment.
     *
     * @param processorName the processor name of the token entry
     * @param segment       the segment of the token entry
     * @return a {@link Bson} defining the filter
     */
    private Bson claimableTokenEntryFilter(String processorName, int segment) {
        return and(
                eq(PROCESSOR_NAME_PROPERTY_NAME, processorName),
                eq(SEGMENT_PROPERTY_NAME, segment),
                or(
                        eq(OWNER_PROPERTY_NAME, nodeId),
                        eq(OWNER_PROPERTY_NAME, null),
                        lt(TIMESTAMP_PROPERTY_NAME, clock.instant().minus(claimTimeout).toEpochMilli())
                )
        );
    }

    private Document tokenEntryToDocument(AbstractTokenEntry<?> tokenEntry) {
        return new Document(PROCESSOR_NAME_PROPERTY_NAME, tokenEntry.getProcessorName())
                .append(SEGMENT_PROPERTY_NAME, tokenEntry.getSegment())
                .append(OWNER_PROPERTY_NAME, tokenEntry.getOwner())
                .append(TIMESTAMP_PROPERTY_NAME, tokenEntry.timestamp().toEpochMilli())
                .append(TOKEN_PROPERTY_NAME,
                        tokenEntry.getSerializedToken() == null ? null : tokenEntry.getSerializedToken().getData())
                .append(TOKEN_TYPE_PROPERTY_NAME,
                        tokenEntry.getSerializedToken() == null ? null : tokenEntry.getSerializedToken()
                                                                                   .getType().getName());
    }

    private AbstractTokenEntry<?> documentToTokenEntry(Document document) {
        return new GenericTokenEntry<>(
                readSerializedData(document),
                document.getString(TOKEN_TYPE_PROPERTY_NAME),
                Instant.ofEpochMilli(document.getLong(TIMESTAMP_PROPERTY_NAME)).toString(),
                document.getString(OWNER_PROPERTY_NAME),
                document.getString(PROCESSOR_NAME_PROPERTY_NAME),
                document.getInteger(SEGMENT_PROPERTY_NAME),
                contentType
        );
    }

    @SuppressWarnings("unchecked")
    private <T> T readSerializedData(Document document) {
        if (byte[].class.equals(contentType)) {
            Binary token = document.get(TOKEN_PROPERTY_NAME, Binary.class);
            return (T) ((token != null) ? token.getData() : null);
        }
        return (T) document.get(TOKEN_PROPERTY_NAME, contentType);
    }

    /**
     * Creates the indexes required to work with the TokenStore.
     *
     * @deprecated this method is now called by the constructor instead of the dependency injection framework running
     * the @PostConstruct. i.e. You no longer have to call it manually if you don't use a dependency injection
     * framework.
     */
    @Deprecated
    public void ensureIndexes() {
        mongoTemplate.trackingTokensCollection().createIndex(Indexes.ascending(PROCESSOR_NAME_PROPERTY_NAME,
                                                                               SEGMENT_PROPERTY_NAME),
                                                             new IndexOptions().unique(true));
    }

    /**
     * Builder class to instantiate a {@link MongoTokenStore}.
     * <p>
     * The {@code claimTimeout} is defaulted to a 10 seconds duration (by using {@link Duration#ofSeconds(long)}, {@code
     * nodeId} is defaulted to the {@code ManagementFactory#getRuntimeMXBean#getName} output, the {@code contentType} to
     * a {@code byte[]} {@link Class}, and the {@code ensureIndexes} to {@code true}. The {@link MongoTemplate} and
     * {@link Serializer} are <b>hard requirements</b> and as such should be provided.
     */
    public static class Builder {

        private MongoTemplate mongoTemplate;
        private Serializer serializer;
        private TemporalAmount claimTimeout = Duration.ofSeconds(10);
        private String nodeId = ManagementFactory.getRuntimeMXBean().getName();
        private Class<?> contentType = byte[].class;
        private boolean ensureIndexes = true;

        /**
         * Sets the {@link MongoTemplate} providing access to the collection which stores the {@link TrackingToken}s.
         *
         * @param mongoTemplate the {@link MongoTemplate} providing access to the collection which stores the {@link
         *                      TrackingToken}s
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder mongoTemplate(MongoTemplate mongoTemplate) {
            assertNonNull(mongoTemplate, "MongoTemplate may not be null");
            this.mongoTemplate = mongoTemplate;
            return this;
        }

        /**
         * Sets the {@link Serializer} used to de-/serialize {@link TrackingToken}s with.
         *
         * @param serializer a {@link Serializer} used to de-/serialize {@link TrackingToken}s with
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder serializer(Serializer serializer) {
            assertNonNull(serializer, "Serializer may not be null");
            this.serializer = serializer;
            return this;
        }

        /**
         * Sets the {@code claimTimeout} specifying the amount of time this process will wait after which this process
         * will force a claim of a {@link TrackingToken}. Thus if a claim has not been updated for the given {@code
         * claimTimeout}, this process will 'steal' the claim. Defaults to a duration of 10 seconds.
         *
         * @param claimTimeout a timeout specifying the time after which this process will force a claim
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder claimTimeout(TemporalAmount claimTimeout) {
            assertNonNull(claimTimeout, "The claim timeout may not be null");
            this.claimTimeout = claimTimeout;
            return this;
        }

        /**
         * Sets the {@code nodeId} to identify ownership of the tokens. Defaults to {@code
         * ManagementFactory#getRuntimeMXBean#getName} output as the node id.
         *
         * @param nodeId the id as a {@link String} to identify ownership of the tokens
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder nodeId(String nodeId) {
            BuilderUtils.assertNonEmpty(nodeId, "The nodeId may not be null or empty");
            this.nodeId = nodeId;
            return this;
        }

        /**
         * Sets the {@code contentType} to which a {@link TrackingToken} should be serialized. Defaults to a {@code
         * byte[]} {@link Class} type.
         *
         * @param contentType the content type as a {@link Class} to which a {@link TrackingToken} should be serialized
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder contentType(Class<?> contentType) {
            assertNonNull(contentType, "The content type may not be null");
            this.contentType = contentType;
            return this;
        }

        /**
         * Sets the {@code ensureIndexes} to tell the builder whether to create or not the indexes required to work with
         * the TokenStore. Defaults to {@code true}. If set to {@code false}, the developer is responsible for the
         * creation of the indexes defined in the {@link MongoTokenStore#ensureIndexes()} method beforehand.
         *
         * @param ensureIndexes the boolean to indicate if the indexes should be created.
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder ensureIndexes(boolean ensureIndexes) {
            this.ensureIndexes = ensureIndexes;
            return this;
        }

        /**
         * Initializes a {@link MongoTokenStore} as specified through this Builder.
         *
         * @return a {@link MongoTokenStore} as specified through this Builder
         */
        public MongoTokenStore build() {
            return new MongoTokenStore(this);
        }

        /**
         * Validates whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException if one field is asserted to be incorrect according to the Builder's
         *                                    specifications
         */
        protected void validate() throws AxonConfigurationException {
            assertNonNull(mongoTemplate, "The MongoTemplate is a hard requirement and should be provided");
            assertNonNull(serializer, "The Serializer is a hard requirement and should be provided");
        }
    }
}
