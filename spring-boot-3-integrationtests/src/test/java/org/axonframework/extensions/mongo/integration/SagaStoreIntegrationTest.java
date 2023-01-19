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
import org.axonframework.extensions.mongo.MongoTemplate;
import org.axonframework.extensions.mongo.eventhandling.saga.repository.MongoSagaStore;
import org.axonframework.extensions.mongo.eventhandling.saga.repository.MyTestSaga;
import org.axonframework.extensions.mongo.eventhandling.saga.repository.NonExistentSaga;
import org.axonframework.extensions.mongo.util.MongoTemplateFactory;
import org.axonframework.modelling.saga.AssociationValue;
import org.axonframework.modelling.saga.repository.SagaStore;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Set;

import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class SagaStoreIntegrationTest {

    @Container
    private static final MongoDBContainer MONGO_CONTAINER = new MongoDBContainer("mongo:5");
    private ApplicationContextRunner testApplicationContext;

    @BeforeEach
    void setUp() {
        testApplicationContext = new ApplicationContextRunner();
        MongoTemplate mongoTemplate = MongoTemplateFactory.build(
                MONGO_CONTAINER.getHost(), MONGO_CONTAINER.getFirstMappedPort()
        );
        mongoTemplate.sagaCollection().deleteMany(new BasicDBObject());
    }

    @Test
    void tokenStoreWillUseMongo() {
        testApplicationContext
                .withPropertyValues("axon.axonserver.enabled=false")
                .withPropertyValues("axon.mongo.token-store.enabled=false")
                .withPropertyValues("axon.mongo.event-store.enabled=false")
                .withPropertyValues("axon.mongo.saga-store.enabled=true")
                .withPropertyValues("spring.data.mongodb.uri=mongodb://" + MONGO_CONTAINER.getHost() + ':'
                                            + MONGO_CONTAINER.getFirstMappedPort() + "/test")
                .withUserConfiguration(DefaultContext.class)
                .run(context -> {
                    SagaStore sagaStore = context.getBean(SagaStore.class);
                    assertNotNull(sagaStore);
                    assertInstanceOf(MongoSagaStore.class, sagaStore);
                    testSagaStore(sagaStore);
                });
    }

    private void testSagaStore(SagaStore sagaStore) {
        MyTestSaga testSaga = new MyTestSaga();
        AssociationValue associationValue = new AssociationValue("key", "value");
        MyTestSaga otherTestSaga = new MyTestSaga();
        sagaStore.insertSaga(MyTestSaga.class, "test1", testSaga, singleton(associationValue));
        sagaStore.insertSaga(MyTestSaga.class, "test2", otherTestSaga, singleton(associationValue));
        Set<String> actual = sagaStore.findSagas(MyTestSaga.class, new AssociationValue("key", "value"));
        assertEquals(2, actual.size(), "Would expect two saga's");
        actual = sagaStore.findSagas(NonExistentSaga.class, new AssociationValue("key", "value"));
        assertTrue(actual.isEmpty(), "Didn't expect any sagas");
    }

    @ContextConfiguration
    @EnableAutoConfiguration
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    public static class DefaultContext {

    }
}
