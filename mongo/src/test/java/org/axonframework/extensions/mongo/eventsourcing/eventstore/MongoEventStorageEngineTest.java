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

package org.axonframework.extensions.mongo.eventsourcing.eventstore;

import com.mongodb.BasicDBObject;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.extensions.mongo.MongoTemplate;
import org.axonframework.extensions.mongo.util.MongoTemplateFactory;
import org.axonframework.extensions.mongo.utils.TestSerializer;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

import static java.util.stream.Collectors.toList;
import static org.axonframework.eventsourcing.utils.EventStoreTestUtils.AGGREGATE;
import static org.axonframework.eventsourcing.utils.EventStoreTestUtils.createEvent;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link MongoEventStorageEngine}.
 *
 * @author Rene de Waele
 */
@Testcontainers
class MongoEventStorageEngineTest extends AbstractMongoEventStorageEngineTest {

    @Container
    private static final MongoDBContainer MONGO_CONTAINER = new MongoDBContainer("mongo");

    private MongoTemplate mongoTemplate;
    private MongoEventStorageEngine testSubject;

    @BeforeEach
    void setUp() {
        mongoTemplate = MongoTemplateFactory.build(
                MONGO_CONTAINER.getHost(), MONGO_CONTAINER.getFirstMappedPort()
        );

        setTestSubject(testSubject = createEngine());
    }

    @Override
    @AfterEach
    public void tearDown() {
        mongoTemplate.eventCollection().dropIndexes();
        mongoTemplate.snapshotCollection().dropIndexes();
        mongoTemplate.eventCollection().deleteMany(new BasicDBObject());
        mongoTemplate.snapshotCollection().deleteMany(new BasicDBObject());
    }

    @Test
    void testOnlySingleSnapshotRemains() {
        testSubject.storeSnapshot(createEvent(0));
        testSubject.storeSnapshot(createEvent(1));
        testSubject.storeSnapshot(createEvent(2));

        assertEquals(1, mongoTemplate.snapshotCollection().countDocuments());
    }

    @Test
    void testFetchHighestSequenceNumber() {
        testSubject.appendEvents(createEvent(0), createEvent(1));
        testSubject.appendEvents(createEvent(2));

        Optional<Long> optionalResult = testSubject.lastSequenceNumberFor(AGGREGATE);
        assertTrue(optionalResult.isPresent());
        assertEquals(2, (long) optionalResult.get());
        assertFalse(testSubject.lastSequenceNumberFor("not_exist").isPresent());
    }

    /**
     * Mongo orders events in time instead of per global index. Thus events with mixing timestamps will be read in the
     * time based order, if the {@link org.axonframework.extensions.mongo.eventsourcing.eventstore.documentperevent.DocumentPerEventStorageStrategy}
     * is used.
     */
    @Test
    @Override
    public void testCreateTokenAtTimeBeforeFirstEvent() {
        Instant dateTimeBeforeFirstEvent = Instant.parse("2006-12-03T10:15:30.00Z");

        DomainEventMessage<String> event1 = createEvent(0, Instant.parse("2007-12-03T10:15:30.00Z"));
        DomainEventMessage<String> event2 = createEvent(1, Instant.parse("2007-12-03T10:15:40.00Z"));
        DomainEventMessage<String> event3 = createEvent(2, Instant.parse("2007-12-03T10:15:35.00Z"));
        testSubject.appendEvents(event1, event2, event3);

        TrackingToken result = testSubject.createTokenAt(dateTimeBeforeFirstEvent);

        List<EventMessage<?>> readEvents = testSubject.readEvents(result, false).collect(toList());

        assertEventStreamsById(Arrays.asList(event1, event3, event2), readEvents);
    }

    @Override
    protected MongoEventStorageEngine createEngine(UnaryOperator<MongoEventStorageEngine.Builder> customization) {
        MongoEventStorageEngine.Builder engineBuilder =
                MongoEventStorageEngine.builder()
                                       .snapshotSerializer(TestSerializer.xStreamSerializer())
                                       .eventSerializer(TestSerializer.xStreamSerializer())
                                       .mongoTemplate(mongoTemplate);
        return customization.apply(engineBuilder).build();
    }
}
