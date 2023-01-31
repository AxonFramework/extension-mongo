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
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.extensions.mongo.MongoTemplate;
import org.axonframework.extensions.mongo.eventsourcing.eventstore.MongoEventStorageEngine;
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

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.axonframework.eventsourcing.utils.EventStoreTestUtils.createEvent;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class EventStoreIntegrationTest {

    @Container
    private static final MongoDBContainer MONGO_CONTAINER = new MongoDBContainer("mongo:5");
    private ApplicationContextRunner testApplicationContext;

    @BeforeEach
    void setUp() {
        testApplicationContext = new ApplicationContextRunner();
        MongoTemplate mongoTemplate = MongoTemplateFactory.build(
                MONGO_CONTAINER.getHost(), MONGO_CONTAINER.getFirstMappedPort()
        );
        mongoTemplate.eventCollection().deleteMany(new BasicDBObject());
    }

    @Test
    void storageEngineWillUseMongo() {
        testApplicationContext
                .withPropertyValues("axon.axonserver.enabled=false")
                .withPropertyValues("axon.mongo.token-store.enabled=false")
                .withPropertyValues("axon.mongo.event-store.enabled=true")
                .withPropertyValues("axon.mongo.saga-store.enabled=false")
                .withPropertyValues("spring.data.mongodb.uri=mongodb://" + MONGO_CONTAINER.getHost() + ':'
                                            + MONGO_CONTAINER.getFirstMappedPort() + "/test")
                .withUserConfiguration(DefaultContext.class)
                .run(context -> {
                    EventStorageEngine storageEngine = context.getBean(EventStorageEngine.class);
                    assertNotNull(storageEngine);
                    assertInstanceOf(MongoEventStorageEngine.class, storageEngine);
                    testStorageEngine(storageEngine);
                });
    }

    private void testStorageEngine(EventStorageEngine storageEngine) {
        DomainEventMessage<String> event1 = createEvent(0, Instant.parse("2007-12-03T10:15:30.00Z"));
        DomainEventMessage<String> event2 = createEvent(1, Instant.parse("2007-12-03T10:15:40.00Z"));
        DomainEventMessage<String> event3 = createEvent(2, Instant.parse("2007-12-03T10:15:50.00Z"));
        DomainEventMessage<String> event4 = createEvent(3, Instant.parse("2007-12-03T10:15:45.00Z"));
        DomainEventMessage<String> event5 = createEvent(4, Instant.parse("2007-12-03T10:15:42.00Z"));

        storageEngine.appendEvents(event1, event2, event3, event4, event5);

        TrackingToken tokenAt = storageEngine.createTokenAt(Instant.parse("2007-12-03T10:15:45.00Z"));

        List<EventMessage<?>> readEvents = storageEngine.readEvents(tokenAt, false).collect(toList());

        assertEventStreamsById(Arrays.asList(event4, event3), readEvents);
    }

    private void assertEventStreamsById(List<EventMessage<?>> s1, List<EventMessage<?>> s2) {
        Assertions.assertEquals(s1.stream().map(EventMessage::getIdentifier).collect(Collectors.toList()),
                                s2.stream().map(EventMessage::getIdentifier).collect(Collectors.toList()));
    }

    @ContextConfiguration
    @EnableAutoConfiguration
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    public static class DefaultContext {

    }
}
