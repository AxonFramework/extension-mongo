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

package org.axonframework.extensions.mongo.eventsourcing.eventstore;

import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventsourcing.eventstore.BatchingEventStorageEngineTest;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.axonframework.eventsourcing.utils.EventStoreTestUtils.AGGREGATE;
import static org.axonframework.eventsourcing.utils.EventStoreTestUtils.createEvent;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Abstract test for {@link MongoEventStorageEngine} tests.
 *
 * @author Milan Savic
 */
public abstract class AbstractMongoEventStorageEngineTest
        extends BatchingEventStorageEngineTest<MongoEventStorageEngine, MongoEventStorageEngine.Builder> {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private MongoEventStorageEngine testSubject;

    @Test
    @Override
    public void uniqueKeyConstraintOnEventIdentifier() {
        logger.info("Unique event identifier is not currently guaranteed in the Mongo Event Storage Engine");
    }

    @Test
    @Override
    public void uniqueKeyConstraintOnFirstEventIdentifierThrowsAggregateIdentifierAlreadyExistsException() {
        logger.info("Unique event identifier is not currently guaranteed in the Mongo Event Storage Engine");
    }

    @Test
    @Override
    public void createTailToken() {
        DomainEventMessage<String> event1 = createEvent(0, Instant.parse("2007-12-03T10:15:10.00Z"));
        testSubject.appendEvents(event1);

        DomainEventMessage<String> event2 = createEvent(1, Instant.parse("2007-12-03T10:15:40.00Z"));
        testSubject.appendEvents(event2);

        DomainEventMessage<String> event3 = createEvent(2, Instant.parse("2007-12-03T10:15:35.00Z"));
        testSubject.appendEvents(event3);

        TrackingToken headToken = testSubject.createTailToken();

        List<EventMessage<?>> readEvents = testSubject.readEvents(headToken, false)
                                                      .collect(toList());

        assertEventStreamsById(Arrays.asList(event1, event3, event2), readEvents);
    }

    @Test
    @Override
    public void createTokenAt() {
        DomainEventMessage<String> event1 = createEvent(0, Instant.parse("2007-12-03T10:15:00.01Z"));
        testSubject.appendEvents(event1);

        DomainEventMessage<String> event2 = createEvent(1, Instant.parse("2007-12-03T10:15:40.00Z"));
        testSubject.appendEvents(event2);

        DomainEventMessage<String> event3 = createEvent(2, Instant.parse("2007-12-03T10:15:35.00Z"));
        testSubject.appendEvents(event3);

        TrackingToken tokenAt = testSubject.createTokenAt(Instant.parse("2007-12-03T10:15:30.00Z"));

        List<EventMessage<?>> readEvents = testSubject.readEvents(tokenAt, false)
                                                      .collect(toList());

        assertEventStreamsById(Arrays.asList(event3, event2), readEvents);
    }

    @Test
    @Override
    public void createTokenAtExactTime() {
        DomainEventMessage<String> event1 = createEvent(0, Instant.parse("2007-12-03T10:15:30.00Z"));
        DomainEventMessage<String> event2 = createEvent(1, Instant.parse("2007-12-03T10:15:40.01Z"));
        DomainEventMessage<String> event3 = createEvent(2, Instant.parse("2007-12-03T10:15:35.00Z"));

        testSubject.appendEvents(event1, event2, event3);

        TrackingToken tokenAt = testSubject.createTokenAt(Instant.parse("2007-12-03T10:15:30.00Z"));

        List<EventMessage<?>> readEvents = testSubject.readEvents(tokenAt, false)
                                                      .collect(toList());

        assertEventStreamsById(Arrays.asList(event1, event3, event2), readEvents);
    }

    @Test
    @Override
    public void createTokenWithUnorderedEvents() {
        DomainEventMessage<String> event1 = createEvent(0, Instant.parse("2007-12-03T10:15:30.00Z"));
        DomainEventMessage<String> event2 = createEvent(1, Instant.parse("2007-12-03T10:15:40.00Z"));
        DomainEventMessage<String> event3 = createEvent(2, Instant.parse("2007-12-03T10:15:50.00Z"));
        DomainEventMessage<String> event4 = createEvent(3, Instant.parse("2007-12-03T10:15:45.00Z"));
        DomainEventMessage<String> event5 = createEvent(4, Instant.parse("2007-12-03T10:15:42.00Z"));

        testSubject.appendEvents(event1, event2, event3, event4, event5);

        TrackingToken tokenAt = testSubject.createTokenAt(Instant.parse("2007-12-03T10:15:45.00Z"));

        List<EventMessage<?>> readEvents = testSubject.readEvents(tokenAt, false)
                                                      .collect(toList());

        assertEventStreamsById(Arrays.asList(event4, event3), readEvents);
    }

    @Test
    public void storeAndLoadSnapshot() {
        testSubject.storeSnapshot(createEvent(0));
        testSubject.storeSnapshot(createEvent(1));
        testSubject.storeSnapshot(createEvent(3));
        testSubject.storeSnapshot(createEvent(2));
        assertTrue(testSubject.readSnapshot(AGGREGATE).isPresent());
        // note - MongoDB stores the last inserted snapshot, as opposed to the one with the largest sequence number
        assertEquals(2, testSubject.readSnapshot(AGGREGATE).get().getSequenceNumber());
    }

    protected void setTestSubject(MongoEventStorageEngine testSubject) {
        super.setTestSubject(this.testSubject = testSubject);
    }
}
