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

import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.thoughtworks.xstream.XStream;
import org.axonframework.eventhandling.GlobalSequenceTrackingToken;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventhandling.tokenstore.AbstractTokenEntry;
import org.axonframework.eventhandling.tokenstore.ConfigToken;
import org.axonframework.eventhandling.tokenstore.GenericTokenEntry;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.UnableToClaimTokenException;
import org.axonframework.eventhandling.tokenstore.UnableToInitializeTokenException;
import org.axonframework.extensions.mongo.MongoTemplate;
import org.axonframework.extensions.mongo.util.MongoTemplateFactory;
import org.axonframework.extensions.mongo.utils.TestSerializer;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.json.JacksonSerializer;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link MongoTokenStore}.
 *
 * @author Joris van der Kallen
 */
@Testcontainers
class MongoTokenStoreTest {

    @Container
    private static final MongoDBContainer MONGO_CONTAINER = new MongoDBContainer("mongo");

    private MongoTokenStore tokenStore;
    private MongoTokenStore tokenStoreDifferentOwner;

    private MongoTemplate mongoTemplate;
    private MongoCollection<Document> trackingTokensCollection;
    private Serializer serializer;
    private final TemporalAmount claimTimeout = Duration.ofSeconds(5);
    private final Class<byte[]> contentType = byte[].class;
    private final boolean ensureIndexes = true;

    private final String testProcessorName = "testProcessorName";
    private final int testSegment = 9;
    private final int testSegmentCount = 10;
    private final String testOwner = "testOwner";

    @BeforeEach
    void setUp() {
        mongoTemplate = MongoTemplateFactory.build(
                MONGO_CONTAINER.getHost(), MONGO_CONTAINER.getFirstMappedPort()
        );
        trackingTokensCollection = mongoTemplate.trackingTokensCollection();
        trackingTokensCollection.drop();

        serializer = TestSerializer.xStreamSerializer();
        MongoTokenStore.Builder tokenStoreBuilder = MongoTokenStore.builder()
                                                                   .mongoTemplate(mongoTemplate)
                                                                   .serializer(serializer)
                                                                   .claimTimeout(claimTimeout)
                                                                   .contentType(contentType)
                                                                   .ensureIndexes(ensureIndexes);
        tokenStore = tokenStoreBuilder.nodeId(testOwner).build();
        tokenStoreDifferentOwner = tokenStoreBuilder.nodeId("anotherOwner").build();
    }

    @AfterEach
    void tearDown() {
        trackingTokensCollection.drop();
    }

    @Test
    void testClaimAndUpdateToken() {
        tokenStore.initializeTokenSegments(testProcessorName, testSegment + 1);
        assertNull(tokenStore.fetchToken(testProcessorName, testSegment));
        TrackingToken token = new GlobalSequenceTrackingToken(1L);
        tokenStore.storeToken(token, testProcessorName, testSegment);
        assertEquals(token, tokenStore.fetchToken(testProcessorName, testSegment));
    }

    @Test
    void testInitializeTokens() {
        tokenStore.initializeTokenSegments("test1", 7);

        int[] actual = tokenStore.fetchSegments("test1");
        Arrays.sort(actual);
        assertArrayEquals(new int[]{0, 1, 2, 3, 4, 5, 6}, actual);
    }

    @SuppressWarnings("Duplicates")
    @Test
    void testInitializeTokensAtGivenPosition() {
        tokenStore.initializeTokenSegments("test1", 7, new GlobalSequenceTrackingToken(10));

        int[] actual = tokenStore.fetchSegments("test1");
        Arrays.sort(actual);
        assertArrayEquals(new int[]{0, 1, 2, 3, 4, 5, 6}, actual);

        for (int segment : actual) {
            assertEquals(new GlobalSequenceTrackingToken(10), tokenStore.fetchToken("test1", segment));
        }
    }

    @Test
    void testInitializeTokensWhileAlreadyPresent() {
        tokenStore.initializeTokenSegments(testProcessorName, testSegmentCount);
        assertThrows(
                UnableToClaimTokenException.class,
                () -> tokenStore.initializeTokenSegments(testProcessorName, testSegmentCount)
        );
    }

    @Test
    void testAttemptToClaimAlreadyClaimedToken() {
        tokenStore.initializeTokenSegments(testProcessorName, testSegmentCount);
        assertNull(tokenStore.fetchToken(testProcessorName, testSegment));

        TrackingToken token = new GlobalSequenceTrackingToken(1L);
        tokenStore.storeToken(token, testProcessorName, testSegment);

        assertThrows(
                UnableToClaimTokenException.class,
                () -> tokenStoreDifferentOwner.storeToken(token, testProcessorName, testSegment)
        );
    }

    @Test
    void testAttemptToExtendClaimOnAlreadyClaimedToken() {
        tokenStore.initializeTokenSegments(testProcessorName, testSegmentCount);
        assertNull(tokenStore.fetchToken(testProcessorName, testSegment));

        assertThrows(
                UnableToClaimTokenException.class,
                () -> tokenStoreDifferentOwner.extendClaim(testProcessorName, testSegment)
        );
    }

    @Test
    void testClaimAndExtend() {
        TrackingToken token = new GlobalSequenceTrackingToken(1L);
        tokenStore.initializeSegment(token, testProcessorName, testSegment);

        tokenStore.storeToken(token, testProcessorName, testSegment);

        try {
            tokenStoreDifferentOwner.fetchToken(testProcessorName, testSegment);
            fail("Expected UnableToClaimTokenException");
        } catch (UnableToClaimTokenException exception) {
            // Expected UnableToClaimTokenException
        }

        tokenStore.extendClaim(testProcessorName, testSegment);
    }

    @Test
    void testReleaseClaimAndExtendClaim() {
        tokenStore.initializeTokenSegments(testProcessorName, testSegmentCount);
        TrackingToken token = new GlobalSequenceTrackingToken(1L);
        tokenStore.storeToken(token, testProcessorName, testSegment);

        try {
            tokenStoreDifferentOwner.fetchToken(testProcessorName, testSegment);
            fail("Expected UnableToClaimTokenException");
        } catch (UnableToClaimTokenException exception) {
            // Expected UnableToClaimTokenException
        }

        tokenStore.releaseClaim(testProcessorName, testSegment);
        assertThrows(
                UnableToClaimTokenException.class,
                () -> tokenStoreDifferentOwner.extendClaim(testProcessorName, testSegment)
        );
    }

    @Test
    void testFetchSegments() {
        tokenStore.initializeTokenSegments("processor1", 3);
        tokenStore.initializeTokenSegments("processor2", 1);

        assertArrayEquals(new int[]{0, 1, 2}, tokenStore.fetchSegments("processor1"));
        assertArrayEquals(new int[]{0}, tokenStore.fetchSegments("processor2"));
        assertArrayEquals(new int[0], tokenStore.fetchSegments("processor3"));
    }

    @Test
    void testConcurrentAccess() throws Exception {
        final int attempts = 10;
        ExecutorService executorService = Executors.newFixedThreadPool(attempts);

        List<Future<Integer>> futures = new ArrayList<>();
        for (int i = 0; i < attempts; i++) {
            final int iteration = i;
            Future<Integer> future = executorService.submit(() -> {
                try {
                    String owner = String.valueOf(iteration);
                    TokenStore tokenStore = MongoTokenStore.builder()
                                                           .mongoTemplate(mongoTemplate)
                                                           .serializer(serializer)
                                                           .claimTimeout(claimTimeout)
                                                           .nodeId(owner)
                                                           .contentType(contentType)
                                                           .ensureIndexes(ensureIndexes)
                                                           .build();
                    GlobalSequenceTrackingToken token = new GlobalSequenceTrackingToken(iteration);
                    tokenStore.initializeSegment(token, testProcessorName, testSegment);
                    tokenStore.storeToken(token, testProcessorName, testSegment);
                    return iteration;
                } catch (UnableToClaimTokenException exception) {
                    return null;
                }
            });
            futures.add(future);
        }
        executorService.shutdown();
        assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS));

        List<Future<Integer>> successfulAttempts = futures.stream()
                                                          .filter(future -> {
                                                              try {
                                                                  return future.get() != null;
                                                              } catch (InterruptedException | ExecutionException e) {
                                                                  return false;
                                                              }
                                                          })
                                                          .collect(Collectors.toList());
        assertEquals(1, successfulAttempts.size());

        Integer iterationOfSuccessfulAttempt = successfulAttempts.get(0)
                                                                 .get();
        TokenStore tokenStore = MongoTokenStore.builder()
                                               .mongoTemplate(mongoTemplate)
                                               .serializer(serializer)
                                               .claimTimeout(claimTimeout)
                                               .nodeId(String.valueOf(iterationOfSuccessfulAttempt))
                                               .contentType(contentType)
                                               .ensureIndexes(ensureIndexes)
                                               .build();

        assertEquals(new GlobalSequenceTrackingToken(iterationOfSuccessfulAttempt),
                     tokenStore.fetchToken(testProcessorName, testSegment));
    }

    @Test
    void testStoreAndFetchTokenResultsInTheSameTokenWithXStreamSerializer() {
        TokenStore tokenStore = MongoTokenStore.builder()
                                               .serializer(XStreamSerializer.builder()
                                                                            .xStream(new XStream())
                                                                            .build())
                                               .mongoTemplate(mongoTemplate)
                                               .claimTimeout(claimTimeout)
                                               .contentType(contentType)
                                               .nodeId(testOwner)
                                               .ensureIndexes(ensureIndexes)
                                               .build();
        GlobalSequenceTrackingToken testToken = new GlobalSequenceTrackingToken(100);
        String testProcessorName = "processorName";
        int testSegment = 0;

        tokenStore.initializeSegment(testToken, testProcessorName, testSegment);

        tokenStore.storeToken(testToken, testProcessorName, testSegment);
        TrackingToken resultToken = tokenStore.fetchToken(testProcessorName, testSegment);
        assertEquals(testToken, resultToken);
    }

    @Test
    void testStoreAndFetchTokenResultsInTheSameTokenWithJacksonSerializer() {
        TokenStore tokenStore = MongoTokenStore.builder()
                                               .serializer(JacksonSerializer.builder().build())
                                               .mongoTemplate(mongoTemplate)
                                               .claimTimeout(claimTimeout)
                                               .contentType(contentType)
                                               .nodeId(testOwner)
                                               .ensureIndexes(ensureIndexes)
                                               .build();
        GlobalSequenceTrackingToken testToken = new GlobalSequenceTrackingToken(100);
        String testProcessorName = "processorName";
        int testSegment = 0;

        tokenStore.initializeSegment(testToken, testProcessorName, testSegment);

        tokenStore.storeToken(testToken, testProcessorName, testSegment);
        TrackingToken resultToken = tokenStore.fetchToken(testProcessorName, testSegment);
        assertEquals(testToken, resultToken);
    }

    @Test
    void testRequiresExplicitSegmentInitializationReturnsTrue() {
        assertTrue(tokenStore.requiresExplicitSegmentInitialization());
    }

    @Test
    void testInitializeSegmentForNullTokenOnlyCreatesSegments() {
        tokenStore.initializeSegment(null, testProcessorName, testSegment);

        int[] actual = tokenStore.fetchSegments(testProcessorName);
        Arrays.sort(actual);
        assertArrayEquals(new int[]{testSegment}, actual);

        assertNull(tokenStore.fetchToken(testProcessorName, testSegment));
    }

    @Test
    void testInitializeSegmentInsertsTheProvidedTokenAndInitializesTheGivenSegment() {
        GlobalSequenceTrackingToken testToken = new GlobalSequenceTrackingToken(100);
        tokenStore.initializeSegment(testToken, testProcessorName, testSegment);

        int[] actual = tokenStore.fetchSegments(testProcessorName);
        Arrays.sort(actual);
        assertArrayEquals(new int[]{testSegment}, actual);

        TrackingToken resultToken = tokenStore.fetchToken(testProcessorName, testSegment);
        assertEquals(testToken, resultToken);
    }

    @Test
    void testInitializeSegmentThrowsUnableToInitializeTokenExceptionForDuplicateKey() {
        GlobalSequenceTrackingToken testToken = new GlobalSequenceTrackingToken(100);
        tokenStore.initializeSegment(testToken, testProcessorName, testSegment);
        // Initializes the given token twice, causing the exception
        assertThrows(
                UnableToInitializeTokenException.class,
                () -> tokenStore.initializeSegment(testToken, testProcessorName, testSegment)
        );
    }

    @Test
    void testDeleteTokenRemovesTheSpecifiedToken() {
        tokenStore.initializeSegment(null, testProcessorName, testSegment);
        // Claim the token by fetching it to be able to delete it
        tokenStore.fetchToken(testProcessorName, testSegment);
        // Verify whether there is a document stored under the given processor name and segment
        MongoCursor<Document> resultIterator =
                mongoTemplate.trackingTokensCollection()
                             .find(and(eq("processorName", testProcessorName), eq("segment", testSegment)))
                             .iterator();
        assertTrue(resultIterator.hasNext());


        tokenStore.deleteToken(testProcessorName, testSegment);

        resultIterator =
                mongoTemplate.trackingTokensCollection()
                             .find(and(eq("processorName", testProcessorName), eq("segment", testSegment)))
                             .iterator();
        assertFalse(resultIterator.hasNext());
    }

    @Test
    void testDeleteTokenThrowsUnableToClaimTokenExceptionIfTheCallingProcessDoesNotOwnTheToken() {
        tokenStore.initializeSegment(null, testProcessorName, testSegment);
        // The token should be fetched to be claimed by somebody, so this should throw a UnableToClaimTokenException
        assertThrows(
                UnableToClaimTokenException.class,
                () -> tokenStore.deleteToken(testProcessorName, testSegment)
        );
    }

    @Test
    void testEnsureIndexCreation() {
        ListIndexesIterable<Document> listIndexes = trackingTokensCollection.listIndexes();
        boolean indexFound = false;
        for (Document index : listIndexes) {
            if (Objects.equals("processorName_1_segment_1", index.getString("name"))) {
                // The index with this name exists, meaning it was created on MongoTokenStore build() method.
                indexFound = true;
                break;
            }
        }
        assertTrue(indexFound);
    }

    @Test
    void testRetrieveStorageIdentifierCreatesAndReturnsConfigTokenIdentifier() {
        String expectedConfigTokenProcessorName = "__config";
        int expectedConfigTokenSegmentId = 0;
        Bson configTokenBson =
                and(eq("processorName", expectedConfigTokenProcessorName), eq("segment", expectedConfigTokenSegmentId));

        assertNull(trackingTokensCollection.find(configTokenBson).first());


        Optional<String> result = tokenStore.retrieveStorageIdentifier();

        assertTrue(result.isPresent());
        assertFalse(result.get().isEmpty());

        assertNotNull(trackingTokensCollection.find(configTokenBson).first());
    }

    @Test
    void testRetrieveStorageIdentifierReturnsExistingConfigTokenIdentifier() {
        String expectedStorageIdentifier = UUID.randomUUID().toString();
        String expectedConfigTokenProcessorName = "__config";
        int expectedConfigTokenSegmentId = 0;

        AbstractTokenEntry<?> testTokenEntry = new GenericTokenEntry<>(
                new ConfigToken(Collections.singletonMap("id", expectedStorageIdentifier)),
                serializer, contentType, expectedConfigTokenProcessorName, expectedConfigTokenSegmentId
        );
        Document testConfigTokenDocument = new Document("processorName", testTokenEntry.getProcessorName())
                .append("segment", testTokenEntry.getSegment())
                .append("owner", testTokenEntry.getOwner())
                .append("timestamp", testTokenEntry.timestamp().toEpochMilli())
                .append("token", testTokenEntry.getSerializedToken().getData())
                .append("tokenType", testTokenEntry.getSerializedToken().getType().getName());

        mongoTemplate.trackingTokensCollection()
                     .insertOne(testConfigTokenDocument);

        Optional<String> result = tokenStore.retrieveStorageIdentifier();

        assertTrue(result.isPresent());
        String resultStorageIdentifier = result.get();
        assertEquals(expectedStorageIdentifier, resultStorageIdentifier);
    }

    @Test
    void storeTokenConcurrently() throws InterruptedException {
        tokenStore.initializeSegment(null, testProcessorName, testSegment);
        TrackingToken someToken = new GlobalSequenceTrackingToken(42);
        testConcurrency(
                () -> {
                    tokenStore.storeToken(someToken, testProcessorName, testSegment);
                    return true;
                },
                () -> {
                    tokenStoreDifferentOwner.storeToken(someToken, testProcessorName, testSegment);
                    return true;
                });
    }

    @Test
    void deleteTokenConcurrently() throws InterruptedException {
        tokenStore.initializeSegment(null, testProcessorName, testSegment);
        TrackingToken someToken = new GlobalSequenceTrackingToken(42);
        tokenStore.storeToken(someToken, testProcessorName, testSegment);
        testConcurrency(
                () -> {
                    tokenStore.deleteToken(testProcessorName, testSegment);
                    return true;
                },
                () -> {
                    tokenStoreDifferentOwner.deleteToken(testProcessorName, testSegment);
                    return true;
                });
    }

    @Test
    void fetchTokenConcurrently() throws InterruptedException {
        tokenStore.initializeSegment(null, testProcessorName, testSegment);
        TrackingToken someToken = new GlobalSequenceTrackingToken(42);
        tokenStore.storeToken(someToken, testProcessorName, testSegment);
        tokenStore.releaseClaim(testProcessorName, testSegment);
        TrackingToken result = testConcurrency(
                () -> tokenStore.fetchToken(testProcessorName, testSegment),
                () -> tokenStoreDifferentOwner.fetchToken(testProcessorName, testSegment)
        );
        assertEquals(someToken, result);
    }

    @Test
    void initializeSegmentWithTokenConcurrently() throws InterruptedException {
        TrackingToken initialToken = new GlobalSequenceTrackingToken(42);
        testConcurrency(
                () -> {
                    tokenStore.initializeSegment(initialToken, testProcessorName, testSegment);
                    return true;
                },
                () -> {
                    tokenStoreDifferentOwner.initializeSegment(initialToken, testProcessorName, testSegment);
                    return true;
                });
    }

    private <T> T testConcurrency(Supplier<T> s1, Supplier<T> s2) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        AtomicReference<T> r1 = new AtomicReference<>(null);
        AtomicReference<T> r2 = new AtomicReference<>(null);
        executor.execute(() -> r1.set(s1.get()));
        executor.execute(() -> r2.set(s2.get()));
        executor.shutdown();
        boolean done = executor.awaitTermination(6L, TimeUnit.SECONDS);
        assertTrue(done, "should complete in 6 seconds");
        if (r1.get() == null) {
            assertNotNull(r2.get(), "at least one of the results should be valid");
            return r2.get();
        } else {
            assertNull(r2.get(), "only one of the results should be valid");
            return r1.get();
        }
    }
}
