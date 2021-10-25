/*
 * Copyright (c) 2010-2021. Axon Framework
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
import org.axonframework.extensions.mongo.MongoTemplate;
import org.axonframework.extensions.mongo.util.MongoTemplateFactory;
import org.axonframework.extensions.mongo.utils.TestSerializer;
import org.axonframework.serialization.Serializer;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.temporal.TemporalAmount;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link MongoTokenStore} when skipping index creation.
 *
 * @author erikrz
 */
@Testcontainers
class MongoTokenStoreSkipIndexTest {

    @Container
    private static final MongoDBContainer MONGO_CONTAINER = new MongoDBContainer("mongo");

    private MongoCollection<Document> trackingTokensCollection;
    private final TemporalAmount claimTimeout = Duration.ofSeconds(5);
    private final Class<byte[]> contentType = byte[].class;

    @BeforeEach
    void setUp() {
        MongoTemplate mongoTemplate = MongoTemplateFactory.build(
                MONGO_CONTAINER.getHost(), MONGO_CONTAINER.getFirstMappedPort()
        );
        trackingTokensCollection = mongoTemplate.trackingTokensCollection();
        trackingTokensCollection.drop();
        String testOwner = "testOwner";
        Serializer serializer = TestSerializer.xStreamSerializer();
        MongoTokenStore.Builder tokenStoreBuilder = MongoTokenStore.builder()
                                                                   .mongoTemplate(mongoTemplate)
                                                                   .serializer(serializer)
                                                                   .claimTimeout(claimTimeout)
                                                                   .contentType(contentType)
                                                                   .ensureIndexes(false)
                                                                   .nodeId(testOwner);
        tokenStoreBuilder.build();
    }

    @AfterEach
    void tearDown() {
        trackingTokensCollection.drop();
    }

    @Test
    void testSkipIndexCreation() {
        ListIndexesIterable<Document> listIndexes = trackingTokensCollection.listIndexes();
        for(Document index : listIndexes){
            // The index with this name does not exist, meaning it was not created on MongoTokenStore build() method.
            assertNotEquals("processorName_1_segment_1",index.getString("name"));
        }
    }
}
