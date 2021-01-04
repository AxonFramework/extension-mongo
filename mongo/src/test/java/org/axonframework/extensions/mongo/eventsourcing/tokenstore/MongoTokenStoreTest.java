/*
 * Copyright (c) 2010-2020. Axon Framework
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

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.axonframework.eventhandling.GlobalSequenceTrackingToken;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.UnableToClaimTokenException;
import org.axonframework.eventhandling.tokenstore.UnableToInitializeTokenException;
import org.axonframework.extensions.mongo.MongoTemplate;
import org.axonframework.extensions.mongo.util.MongoTemplateFactory;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.json.JacksonSerializer;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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

        serializer = XStreamSerializer.defaultSerializer();
        MongoTokenStore.Builder tokenStoreBuilder = MongoTokenStore.builder()
                                                                   .mongoTemplate(mongoTemplate)
                                                                   .serializer(serializer)
                                                                   .claimTimeout(claimTimeout)
                                                                   .contentType(contentType);
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
                                               .build();

        assertEquals(new GlobalSequenceTrackingToken(iterationOfSuccessfulAttempt),
                     tokenStore.fetchToken(testProcessorName, testSegment));
    }

    @Test
    void testStoreAndFetchTokenResultsInTheSameTokenWithXStreamSerializer() {
        TokenStore tokenStore = MongoTokenStore.builder()
                                               .serializer(XStreamSerializer.builder().build())
                                               .mongoTemplate(mongoTemplate)
                                               .claimTimeout(claimTimeout)
                                               .contentType(contentType)
                                               .nodeId(testOwner)
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
}
