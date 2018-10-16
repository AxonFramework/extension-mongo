/*
 * Copyright (c) 2010-2018. Axon Framework
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

package org.axonframework.mongo.eventhandling.saga.repository;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import org.axonframework.modelling.saga.AssociationValue;
import org.axonframework.modelling.saga.AssociationValues;
import org.axonframework.modelling.saga.AssociationValuesImpl;
import org.axonframework.modelling.saga.repository.SagaStore;
import org.axonframework.mongo.DefaultMongoTemplate;
import org.axonframework.mongo.MongoTemplate;
import org.axonframework.mongo.MongoTestContext;
import org.axonframework.mongo.eventsourcing.eventstore.MongoEventStorageEngine;
import org.axonframework.mongo.utils.MongoLauncher;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.*;
import org.junit.runner.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.Assert.*;

/**
 * @author Jettro Coenradie
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = MongoTestContext.class)
public class MongoSagaStoreTest {

    private final static Logger logger = LoggerFactory.getLogger(MongoSagaStoreTest.class);

    private static MongodProcess mongod;
    private static MongodExecutable mongoExe;

    @Autowired
    private MongoSagaStore sagaStore;

    @Autowired
    @Qualifier("sagaMongoTemplate")
    private MongoTemplate mongoTemplate;

    @Autowired
    private ApplicationContext context;

    @BeforeClass
    public static void start() throws IOException {
        mongoExe = MongoLauncher.prepareExecutable();
        mongod = mongoExe.start();
        if (mongod == null) {
            // we're using an existing mongo instance. Make sure it's clean
            DefaultMongoTemplate template = DefaultMongoTemplate.builder().mongoDatabase(new MongoClient()).build();
            template.eventCollection().drop();
            template.snapshotCollection().drop();
        }
    }

    @AfterClass
    public static void shutdown() {
        if (mongod != null) {
            mongod.stop();
        }
        if (mongoExe != null) {
            mongoExe.stop();
        }
    }

    @Before
    public void setUp() {
        try {
            context.getBean(Mongo.class);
            context.getBean(MongoEventStorageEngine.class);
        } catch (Exception e) {
            logger.error("No Mongo instance found. Ignoring test.");
            Assume.assumeNoException(e);
        }
        mongoTemplate.sagaCollection().drop();
    }

    @DirtiesContext
    @Test
    public void testLoadSagaOfDifferentTypesWithSameAssociationValue_SagaFound() {
        MyTestSaga testSaga = new MyTestSaga();
        MyOtherTestSaga otherTestSaga = new MyOtherTestSaga();
        AssociationValue associationValue = new AssociationValue("key", "value");
        sagaStore.insertSaga(MyTestSaga.class, "test1", testSaga, singleton(associationValue));
        sagaStore.insertSaga(MyTestSaga.class, "test2", otherTestSaga, singleton(associationValue));
        Set<String> actual = sagaStore.findSagas(MyTestSaga.class, associationValue);
        assertEquals(1, actual.size());
        assertEquals(
                MyTestSaga.class,
                sagaStore.loadSaga(MyTestSaga.class, actual.iterator().next()).saga().getClass()
        );

        Set<String> actual2 = sagaStore.findSagas(MyOtherTestSaga.class, associationValue);
        assertEquals(1, actual2.size());
        assertEquals(
                MyOtherTestSaga.class,
                sagaStore.loadSaga(MyOtherTestSaga.class, actual2.iterator().next()).saga().getClass()
        );

        Bson sagaQuery = SagaEntry.queryByIdentifier("test1");
        FindIterable<Document> sagaCursor = mongoTemplate.sagaCollection().find(sagaQuery);
        assertEquals(
                "Amount of found sagas is not as expected",
                1,
                StreamSupport.stream(sagaCursor.spliterator(), false).count()
        );
    }

    @DirtiesContext
    @Test
    public void testLoadSagaOfDifferentTypesWithSameAssociationValue_NoSagaFound() {
        MyTestSaga testSaga = new MyTestSaga();
        AssociationValue associationValue = new AssociationValue("key", "value");
        MyOtherTestSaga otherTestSaga = new MyOtherTestSaga();
        sagaStore.insertSaga(MyTestSaga.class, "test1", testSaga, singleton(associationValue));
        sagaStore.insertSaga(MyTestSaga.class, "test2", otherTestSaga, singleton(associationValue));
        Set<String> actual = sagaStore.findSagas(NonExistentSaga.class, new AssociationValue("key", "value"));
        assertTrue("Didn't expect any sagas", actual.isEmpty());
    }

    @Test
    @DirtiesContext
    public void testLoadSagaOfDifferentTypesWithSameAssociationValue_SagaDeleted() {
        MyTestSaga testSaga = new MyTestSaga();
        MyOtherTestSaga otherTestSaga = new MyOtherTestSaga();
        AssociationValue associationValue = new AssociationValue("key", "value");
        sagaStore.insertSaga(MyTestSaga.class, "test1", testSaga, singleton(associationValue));

        sagaStore.insertSaga(MyTestSaga.class, "test2", otherTestSaga, singleton(associationValue));
        sagaStore.deleteSaga(MyTestSaga.class, "test1", singleton(associationValue));
        Set<String> actual = sagaStore.findSagas(MyTestSaga.class, associationValue);
        assertTrue("Didn't expect any sagas", actual.isEmpty());

        Bson sagaQuery = SagaEntry.queryByIdentifier("test1");
        FindIterable<Document> sagaCursor = mongoTemplate.sagaCollection().find(sagaQuery);
        assertEquals(
                "No saga is expected after .end and .commit",
                0,
                StreamSupport.stream(sagaCursor.spliterator(), false).count()
        );
    }

    @DirtiesContext
    @Test
    public void testAddAndLoadSaga_ByIdentifier() {
        MyTestSaga saga = new MyTestSaga();
        AssociationValue associationValue = new AssociationValue("key", "value");
        sagaStore.insertSaga(MyTestSaga.class, "test1", saga, singleton(associationValue));
        SagaStore.Entry<MyTestSaga> loaded = sagaStore.loadSaga(MyTestSaga.class, "test1");
        assertEquals(singleton(associationValue), loaded.associationValues());
        assertEquals(MyTestSaga.class, loaded.saga().getClass());
        assertNotNull(mongoTemplate.sagaCollection().find(SagaEntry.queryByIdentifier("test1")));
    }

    @DirtiesContext
    @Test
    public void testAddAndLoadSaga_ByAssociationValue() {
        MyTestSaga saga = new MyTestSaga();
        AssociationValue associationValue = new AssociationValue("key", "value");
        sagaStore.insertSaga(MyTestSaga.class, "test1", saga, singleton(associationValue));
        Set<String> loaded = sagaStore.findSagas(MyTestSaga.class, associationValue);
        assertEquals(1, loaded.size());
        SagaStore.Entry<MyTestSaga> loadedSaga = sagaStore.loadSaga(MyTestSaga.class, loaded.iterator().next());
        assertEquals(singleton(associationValue), loadedSaga.associationValues());
        assertNotNull(mongoTemplate.sagaCollection().find(SagaEntry.queryByIdentifier("test1")));
    }

    @SuppressWarnings("UnusedAssignment")
    @Test
    @DirtiesContext
    public void testAddAndLoadSaga_MultipleHitsByAssociationValue() {
        String identifier1 = UUID.randomUUID().toString();
        String identifier2 = UUID.randomUUID().toString();
        MyTestSaga saga1 = new MyTestSaga();
        MyOtherTestSaga saga2 = new MyOtherTestSaga();
        AssociationValue associationValue = new AssociationValue("key", "value");
        sagaStore.insertSaga(MyTestSaga.class, identifier1, saga1, singleton(associationValue));
        sagaStore.insertSaga(MyOtherTestSaga.class, identifier2, saga2, singleton(associationValue));

        // load saga1
        Set<String> loaded1 = sagaStore.findSagas(MyTestSaga.class, new AssociationValue("key", "value"));
        assertEquals(1, loaded1.size());
        SagaStore.Entry<MyTestSaga> loadedSaga1 = sagaStore.loadSaga(MyTestSaga.class, loaded1.iterator().next());
        assertEquals(singleton(associationValue), loadedSaga1.associationValues());
        assertNotNull(mongoTemplate.sagaCollection().find(SagaEntry.queryByIdentifier(identifier1)));

        // load saga2
        Set<String> loaded2 = sagaStore.findSagas(MyOtherTestSaga.class, new AssociationValue("key", "value"));
        assertEquals(1, loaded2.size());
        SagaStore.Entry<MyOtherTestSaga> loadedSaga2 =
                sagaStore.loadSaga(MyOtherTestSaga.class, loaded2.iterator().next());
        assertEquals(singleton(associationValue), loadedSaga2.associationValues());
        assertNotNull(mongoTemplate.sagaCollection().find(SagaEntry.queryByIdentifier(identifier2)));
    }

    @Test
    @DirtiesContext
    public void testAddAndLoadSaga_AssociateValueAfterStorage() {
        AssociationValue associationValue = new AssociationValue("key", "value");
        String identifier = UUID.randomUUID().toString();
        MyTestSaga saga = new MyTestSaga();
        sagaStore.insertSaga(MyTestSaga.class, identifier, saga, singleton(associationValue));

        Set<String> loaded = sagaStore.findSagas(MyTestSaga.class, new AssociationValue("key", "value"));
        assertEquals(1, loaded.size());
        SagaStore.Entry<MyTestSaga> loadedSaga = sagaStore.loadSaga(MyTestSaga.class, loaded.iterator().next());
        assertEquals(singleton(associationValue), loadedSaga.associationValues());
        assertNotNull(mongoTemplate.sagaCollection().find(SagaEntry.queryByIdentifier(identifier)));
    }

    @Test
    public void testLoadSaga_NotFound() {
        assertNull(sagaStore.loadSaga(MyTestSaga.class, "123456"));
    }

    @DirtiesContext
    @Test
    public void testLoadSaga_AssociationValueRemoved() {
        String identifier = UUID.randomUUID().toString();
        MyTestSaga saga = new MyTestSaga();
        AssociationValue associationValue = new AssociationValue("key", "value");
        SagaEntry<MyTestSaga> testSagaEntry = new SagaEntry<>(
                identifier, saga, singleton(associationValue), XStreamSerializer.builder().build()
        );
        mongoTemplate.sagaCollection().insertOne(testSagaEntry.asDocument());

        SagaStore.Entry<MyTestSaga> loaded = sagaStore.loadSaga(MyTestSaga.class, identifier);
        AssociationValues av = new AssociationValuesImpl(loaded.associationValues());
        av.remove(associationValue);
        sagaStore.updateSaga(MyTestSaga.class, identifier, loaded.saga(), av);
        Set<String> found = sagaStore.findSagas(MyTestSaga.class, new AssociationValue("key", "value"));
        assertEquals(0, found.size());
    }

    @DirtiesContext
    @Test
    public void testSaveSaga() {
        String identifier = UUID.randomUUID().toString();
        MyTestSaga saga = new MyTestSaga();
        XStreamSerializer serializer = XStreamSerializer.builder().build();
        mongoTemplate.sagaCollection()
                     .insertOne(new SagaEntry<>(identifier, saga, emptySet(), serializer).asDocument());
        SagaStore.Entry<MyTestSaga> loaded = sagaStore.loadSaga(MyTestSaga.class, identifier);
        loaded.saga().counter = 1;
        sagaStore.updateSaga(MyTestSaga.class,
                             identifier,
                             loaded.saga(),
                             new AssociationValuesImpl(loaded.associationValues()));

        SagaEntry entry = new SagaEntry(mongoTemplate.sagaCollection()
                                                     .find(SagaEntry.queryByIdentifier(identifier))
                                                     .first());
        MyTestSaga actualSaga = (MyTestSaga) entry.getSaga(serializer);
        assertNotSame(loaded, actualSaga);
        assertEquals(1, actualSaga.counter);
    }

    @DirtiesContext
    @Test
    public void testEndSaga() {
        String identifier = UUID.randomUUID().toString();
        MyTestSaga saga = new MyTestSaga();
        AssociationValue associationValue = new AssociationValue("key", "value");
        SagaEntry<MyTestSaga> testSagaEntry = new SagaEntry<>(
                identifier, saga, singleton(associationValue), XStreamSerializer.builder().build()
        );
        mongoTemplate.sagaCollection().insertOne(testSagaEntry.asDocument());
        sagaStore.deleteSaga(MyTestSaga.class, identifier, singleton(associationValue));

        assertNull(mongoTemplate.sagaCollection().find(SagaEntry.queryByIdentifier(identifier)).first());
    }

    private static class MyTestSaga {

        private int counter = 0;
    }

    private static class MyOtherTestSaga {

    }

    private class NonExistentSaga {

    }
}
