/*
 * Copyright (c) 2010-2023. Axon Framework
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

package org.axonframework.extensions.mongo.integration;

import com.mongodb.BasicDBObject;
import org.axonframework.eventhandling.GlobalSequenceTrackingToken;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.extensions.mongo.MongoTemplate;
import org.axonframework.extensions.mongo.eventsourcing.tokenstore.MongoTokenStore;
import org.axonframework.extensions.mongo.util.MongoTemplateFactory;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class TokenStoreIntegrationTest {

    @Container
    private static final MongoDBContainer MONGO_CONTAINER = new MongoDBContainer("mongo:5");
    private ApplicationContextRunner testApplicationContext;

    @BeforeEach
    void setUp() {
        testApplicationContext = new ApplicationContextRunner();
        MongoTemplate mongoTemplate = MongoTemplateFactory.build(
                MONGO_CONTAINER.getHost(), MONGO_CONTAINER.getFirstMappedPort()
        );
        mongoTemplate.trackingTokensCollection().deleteMany(new BasicDBObject());
    }

    @Test
    void tokenStoreWillUseMongo() {
        testApplicationContext
                .withPropertyValues("axon.axonserver.enabled=false")
                .withPropertyValues("axon.mongo.token-store.enabled=true")
                .withPropertyValues("axon.mongo.event-store.enabled=false")
                .withPropertyValues("axon.mongo.saga-store.enabled=false")
                .withPropertyValues("spring.data.mongodb.uri=mongodb://" + MONGO_CONTAINER.getHost() + ':'
                                            + MONGO_CONTAINER.getFirstMappedPort() + "/test")
                .withUserConfiguration(DefaultContext.class)
                .run(context -> {
                    TokenStore tokenStore = context.getBean(TokenStore.class);
                    assertNotNull(tokenStore);
                    assertInstanceOf(MongoTokenStore.class, tokenStore);
                    testTokenStore(tokenStore);
                });
    }

    private void testTokenStore(TokenStore tokenStore) {
        String testProcessorName = "testProcessorName";
        int testSegment = 9;
        tokenStore.initializeTokenSegments(testProcessorName, testSegment + 1);
        assertNull(tokenStore.fetchToken(testProcessorName, testSegment));
        TrackingToken token = new GlobalSequenceTrackingToken(1L);
        tokenStore.storeToken(token, testProcessorName, testSegment);
        assertEquals(token, tokenStore.fetchToken(testProcessorName, testSegment));
    }

    @ContextConfiguration
    @EnableAutoConfiguration
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    public static class DefaultContext {

    }
}
