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

package org.axonframework.extensions.mongo.eventhandling.saga.repository;

import com.mongodb.client.FindIterable;
import org.axonframework.extensions.mongo.MongoTemplate;
import org.axonframework.extensions.mongo.util.MongoTemplateFactory;
import org.axonframework.modelling.saga.AssociationValue;
import org.axonframework.modelling.saga.AssociationValues;
import org.axonframework.modelling.saga.AssociationValuesImpl;
import org.axonframework.modelling.saga.repository.SagaStore;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Set;
import java.util.UUID;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link MongoSagaStore}.
 *
 * @author Jettro Coenradie
 */
@Testcontainers
class MongoSagaStoreTest {

    @Container
    private static final MongoDBContainer MONGO_CONTAINER = new MongoDBContainer("mongo");

    private MongoTemplate mongoTemplate;
    private MongoSagaStore testSubject;

    @BeforeEach
    void setUp() {
        mongoTemplate = MongoTemplateFactory.build(
                MONGO_CONTAINER.getHost(), MONGO_CONTAINER.getFirstMappedPort()
        );
        mongoTemplate.sagaCollection().drop();
        testSubject = MongoSagaStore.builder()
                                    .mongoTemplate(mongoTemplate)
                                    .build();
    }

    @Test
    void testLoadSagaOfDifferentTypesWithSameAssociationValue_SagaFound() {
        MyTestSaga testSaga = new MyTestSaga();
        MyOtherTestSaga otherTestSaga = new MyOtherTestSaga();
        AssociationValue associationValue = new AssociationValue("key", "value");
        testSubject.insertSaga(MyTestSaga.class, "test1", testSaga, singleton(associationValue));
        testSubject.insertSaga(MyTestSaga.class, "test2", otherTestSaga, singleton(associationValue));
        Set<String> actual = testSubject.findSagas(MyTestSaga.class, associationValue);
        assertEquals(1, actual.size());
        assertEquals(
                MyTestSaga.class,
                testSubject.loadSaga(MyTestSaga.class, actual.iterator().next()).saga().getClass()
        );

        Set<String> actual2 = testSubject.findSagas(MyOtherTestSaga.class, associationValue);
        assertEquals(1, actual2.size());
        assertEquals(
                MyOtherTestSaga.class,
                testSubject.loadSaga(MyOtherTestSaga.class, actual2.iterator().next()).saga().getClass()
        );

        Bson sagaQuery = SagaEntry.queryByIdentifier("test1");
        FindIterable<Document> sagaCursor = mongoTemplate.sagaCollection().find(sagaQuery);
        assertEquals(
                1,
                StreamSupport.stream(sagaCursor.spliterator(), false).count(),
                "Amount of found sagas is not as expected"
        );
    }

    @Test
    void testLoadSagaOfDifferentTypesWithSameAssociationValue_NoSagaFound() {
        MyTestSaga testSaga = new MyTestSaga();
        AssociationValue associationValue = new AssociationValue("key", "value");
        MyOtherTestSaga otherTestSaga = new MyOtherTestSaga();
        testSubject.insertSaga(MyTestSaga.class, "test1", testSaga, singleton(associationValue));
        testSubject.insertSaga(MyTestSaga.class, "test2", otherTestSaga, singleton(associationValue));
        Set<String> actual = testSubject.findSagas(NonExistentSaga.class, new AssociationValue("key", "value"));
        assertTrue(actual.isEmpty(), "Didn't expect any sagas");
    }

    @Test
    void testLoadSagaOfDifferentTypesWithSameAssociationValue_SagaDeleted() {
        MyTestSaga testSaga = new MyTestSaga();
        MyOtherTestSaga otherTestSaga = new MyOtherTestSaga();
        AssociationValue associationValue = new AssociationValue("key", "value");
        testSubject.insertSaga(MyTestSaga.class, "test1", testSaga, singleton(associationValue));

        testSubject.insertSaga(MyTestSaga.class, "test2", otherTestSaga, singleton(associationValue));
        testSubject.deleteSaga(MyTestSaga.class, "test1", singleton(associationValue));
        Set<String> actual = testSubject.findSagas(MyTestSaga.class, associationValue);
        assertTrue(actual.isEmpty(), "Didn't expect any sagas");

        Bson sagaQuery = SagaEntry.queryByIdentifier("test1");
        FindIterable<Document> sagaCursor = mongoTemplate.sagaCollection().find(sagaQuery);
        assertEquals(
                0,
                StreamSupport.stream(sagaCursor.spliterator(), false).count(),
                "No saga is expected after .end and .commit"
        );
    }

    @Test
    void testAddAndLoadSaga_ByIdentifier() {
        MyTestSaga saga = new MyTestSaga();
        AssociationValue associationValue = new AssociationValue("key", "value");
        testSubject.insertSaga(MyTestSaga.class, "test1", saga, singleton(associationValue));
        SagaStore.Entry<MyTestSaga> loaded = testSubject.loadSaga(MyTestSaga.class, "test1");
        assertEquals(singleton(associationValue), loaded.associationValues());
        assertEquals(MyTestSaga.class, loaded.saga().getClass());
        assertNotNull(mongoTemplate.sagaCollection().find(SagaEntry.queryByIdentifier("test1")));
    }

    @Test
    void testAddAndLoadSaga_ByAssociationValue() {
        MyTestSaga saga = new MyTestSaga();
        AssociationValue associationValue = new AssociationValue("key", "value");
        testSubject.insertSaga(MyTestSaga.class, "test1", saga, singleton(associationValue));
        Set<String> loaded = testSubject.findSagas(MyTestSaga.class, associationValue);
        assertEquals(1, loaded.size());
        SagaStore.Entry<MyTestSaga> loadedSaga = testSubject.loadSaga(MyTestSaga.class, loaded.iterator().next());
        assertEquals(singleton(associationValue), loadedSaga.associationValues());
        assertNotNull(mongoTemplate.sagaCollection().find(SagaEntry.queryByIdentifier("test1")));
    }

    @Test
    void testAddAndLoadSaga_MultipleHitsByAssociationValue() {
        String identifier1 = UUID.randomUUID().toString();
        String identifier2 = UUID.randomUUID().toString();
        MyTestSaga saga1 = new MyTestSaga();
        MyOtherTestSaga saga2 = new MyOtherTestSaga();
        AssociationValue associationValue = new AssociationValue("key", "value");
        testSubject.insertSaga(MyTestSaga.class, identifier1, saga1, singleton(associationValue));
        testSubject.insertSaga(MyOtherTestSaga.class, identifier2, saga2, singleton(associationValue));

        // load saga1
        Set<String> loaded1 = testSubject.findSagas(MyTestSaga.class, new AssociationValue("key", "value"));
        assertEquals(1, loaded1.size());
        SagaStore.Entry<MyTestSaga> loadedSaga1 = testSubject.loadSaga(MyTestSaga.class, loaded1.iterator().next());
        assertEquals(singleton(associationValue), loadedSaga1.associationValues());
        assertNotNull(mongoTemplate.sagaCollection().find(SagaEntry.queryByIdentifier(identifier1)));

        // load saga2
        Set<String> loaded2 = testSubject.findSagas(MyOtherTestSaga.class, new AssociationValue("key", "value"));
        assertEquals(1, loaded2.size());
        SagaStore.Entry<MyOtherTestSaga> loadedSaga2 =
                testSubject.loadSaga(MyOtherTestSaga.class, loaded2.iterator().next());
        assertEquals(singleton(associationValue), loadedSaga2.associationValues());
        assertNotNull(mongoTemplate.sagaCollection().find(SagaEntry.queryByIdentifier(identifier2)));
    }

    @Test
    void testAddAndLoadSaga_AssociateValueAfterStorage() {
        AssociationValue associationValue = new AssociationValue("key", "value");
        String identifier = UUID.randomUUID().toString();
        MyTestSaga saga = new MyTestSaga();
        testSubject.insertSaga(MyTestSaga.class, identifier, saga, singleton(associationValue));

        Set<String> loaded = testSubject.findSagas(MyTestSaga.class, new AssociationValue("key", "value"));
        assertEquals(1, loaded.size());
        SagaStore.Entry<MyTestSaga> loadedSaga = testSubject.loadSaga(MyTestSaga.class, loaded.iterator().next());
        assertEquals(singleton(associationValue), loadedSaga.associationValues());
        assertNotNull(mongoTemplate.sagaCollection().find(SagaEntry.queryByIdentifier(identifier)));
    }

    @Test
    void testLoadSaga_NotFound() {
        assertNull(testSubject.loadSaga(MyTestSaga.class, "123456"));
    }

    @Test
    void testLoadSaga_AssociationValueRemoved() {
        String identifier = UUID.randomUUID().toString();
        MyTestSaga saga = new MyTestSaga();
        AssociationValue associationValue = new AssociationValue("key", "value");
        SagaEntry<MyTestSaga> testSagaEntry = new SagaEntry<>(
                identifier, saga, singleton(associationValue), XStreamSerializer.defaultSerializer()
        );
        mongoTemplate.sagaCollection().insertOne(testSagaEntry.asDocument());

        SagaStore.Entry<MyTestSaga> loaded = testSubject.loadSaga(MyTestSaga.class, identifier);
        AssociationValues av = new AssociationValuesImpl(loaded.associationValues());
        av.remove(associationValue);
        testSubject.updateSaga(MyTestSaga.class, identifier, loaded.saga(), av);
        Set<String> found = testSubject.findSagas(MyTestSaga.class, new AssociationValue("key", "value"));
        assertEquals(0, found.size());
    }

    @Test
    void testSaveSaga() {
        String identifier = UUID.randomUUID().toString();
        MyTestSaga saga = new MyTestSaga();
        Serializer serializer = XStreamSerializer.defaultSerializer();
        mongoTemplate.sagaCollection()
                     .insertOne(new SagaEntry<>(identifier, saga, emptySet(), serializer).asDocument());
        SagaStore.Entry<MyTestSaga> loaded = testSubject.loadSaga(MyTestSaga.class, identifier);
        loaded.saga().counter = 1;
        testSubject.updateSaga(MyTestSaga.class,
                               identifier,
                               loaded.saga(),
                               new AssociationValuesImpl(loaded.associationValues()));

        Document sagaDocument = mongoTemplate.sagaCollection()
                                             .find(SagaEntry.queryByIdentifier(identifier))
                                             .first();
        assertNotNull(sagaDocument);
        SagaEntry<MyTestSaga> entry = new SagaEntry<>(sagaDocument);
        MyTestSaga actualSaga = entry.getSaga(serializer);
        assertNotSame(loaded.saga(), actualSaga);
        assertEquals(1, actualSaga.counter);
    }

    @Test
    void testEndSaga() {
        String identifier = UUID.randomUUID().toString();
        MyTestSaga saga = new MyTestSaga();
        AssociationValue associationValue = new AssociationValue("key", "value");
        SagaEntry<MyTestSaga> testSagaEntry = new SagaEntry<>(
                identifier, saga, singleton(associationValue), XStreamSerializer.defaultSerializer()
        );
        mongoTemplate.sagaCollection().insertOne(testSagaEntry.asDocument());
        testSubject.deleteSaga(MyTestSaga.class, identifier, singleton(associationValue));

        assertNull(mongoTemplate.sagaCollection().find(SagaEntry.queryByIdentifier(identifier)).first());
    }

    private static class MyTestSaga {

        private int counter = 0;
    }

    private static class MyOtherTestSaga {

    }

    private static class NonExistentSaga {

    }
}
