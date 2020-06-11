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

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import org.axonframework.common.jdbc.PersistenceExceptionResolver;
import org.axonframework.eventsourcing.eventstore.AbstractEventStorageEngine;
import org.axonframework.extensions.mongo.DefaultMongoTemplate;
import org.axonframework.extensions.mongo.MongoTestContext;
import org.axonframework.extensions.mongo.utils.MongoLauncher;
import org.axonframework.serialization.upcasting.event.EventUpcaster;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.IOException;
import java.util.Optional;

import static org.axonframework.eventsourcing.utils.EventStoreTestUtils.AGGREGATE;
import static org.axonframework.eventsourcing.utils.EventStoreTestUtils.createEvent;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;

/**
 * Test class validating the {@link MongoEventStorageEngine}.
 *
 * @author Rene de Waele
 */
@DirtiesContext
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = MongoTestContext.class)
class MongoEventStorageEngineTest extends AbstractMongoEventStorageEngineTest {

    private static MongodExecutable mongoExe;
    private static MongodProcess mongod;

    @Autowired
    private ApplicationContext context;
    private DefaultMongoTemplate mongoTemplate;

    private MongoEventStorageEngine testSubject;

    @BeforeAll
    static void start() throws IOException {
        mongoExe = MongoLauncher.prepareExecutable();
        mongod = mongoExe.start();
    }

    @AfterAll
    static void shutdown() {
        if (mongod != null) {
            mongod.stop();
        }
        if (mongoExe != null) {
            mongoExe.stop();
        }
    }

    @BeforeEach
    void setUp() {
        MongoClient mongoClient = null;
        try {
            mongoClient = context.getBean(MongoClient.class);
        } catch (Exception e) {
            assumeTrue(true, "No Mongo instance found. Ignoring test.");
        }
        mongoTemplate = DefaultMongoTemplate.builder().mongoDatabase(mongoClient).build();
        testSubject = context.getBean(MongoEventStorageEngine.class);
        setTestSubject(testSubject);
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

    @Override
    protected AbstractEventStorageEngine createEngine(EventUpcaster upcasterChain) {
        return MongoEventStorageEngine.builder()
                                      .upcasterChain(upcasterChain)
                                      .mongoTemplate(mongoTemplate)
                                      .build();
    }

    @Override
    protected AbstractEventStorageEngine createEngine(PersistenceExceptionResolver persistenceExceptionResolver) {
        return MongoEventStorageEngine.builder()
                                      .persistenceExceptionResolver(persistenceExceptionResolver)
                                      .mongoTemplate(mongoTemplate)
                                      .build();
    }
}
