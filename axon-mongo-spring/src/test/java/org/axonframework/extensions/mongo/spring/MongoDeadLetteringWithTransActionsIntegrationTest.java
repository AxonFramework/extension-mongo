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

package org.axonframework.extensions.mongo.spring;

import com.mongodb.BasicDBObject;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.deadletter.DeadLetteringEventIntegrationTest;
import org.axonframework.extensions.mongo.MongoTemplate;
import org.axonframework.extensions.mongo.eventhandling.deadletter.MongoSequencedDeadLetterQueue;
import org.axonframework.extensions.mongo.utils.TestSerializer;
import org.axonframework.messaging.deadletter.SequencedDeadLetterQueue;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class MongoDeadLetteringWithTransActionsIntegrationTest extends DeadLetteringEventIntegrationTest {

    @Container
    private static final MongoDBContainer MONGO_CONTAINER = new MongoDBContainer("mongo:5");
    private static final String PROCESSING_GROUP = "processing-group";
    private static final int MAX_SEQUENCES_AND_SEQUENCE_SIZE = 128;

    @Override
    protected SequencedDeadLetterQueue<EventMessage<?>> buildDeadLetterQueue() {
        String connectionString =
                "mongodb://" + MONGO_CONTAINER.getHost() + ":" + MONGO_CONTAINER.getFirstMappedPort() + "/dit";
        MongoDatabaseFactory factory = new SimpleMongoClientDatabaseFactory(connectionString);
        MongoTemplate mongoTemplate = SpringMongoTemplate.builder().factory(factory).build();
        mongoTemplate.deadLetterCollection().deleteMany(new BasicDBObject());
        return MongoSequencedDeadLetterQueue
                .builder()
                .processingGroup(PROCESSING_GROUP)
                .maxSequences(MAX_SEQUENCES_AND_SEQUENCE_SIZE)
                .maxSequenceSize(MAX_SEQUENCES_AND_SEQUENCE_SIZE)
                .serializer(TestSerializer.xStreamSerializer())
                .mongoTemplate(mongoTemplate)
                .transactionManager(new SpringMongoTransactionManager(new MongoTransactionManager(factory)))
                .build();
    }
}
