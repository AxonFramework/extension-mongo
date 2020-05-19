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
import com.mongodb.WriteConcern;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import org.axonframework.common.jdbc.PersistenceExceptionResolver;
import org.axonframework.eventsourcing.eventstore.AbstractEventStorageEngine;
import org.axonframework.extensions.mongo.DefaultMongoTemplate;
import org.axonframework.extensions.mongo.MongoTemplate;
import org.axonframework.extensions.mongo.serialization.DBObjectXStreamSerializer;
import org.axonframework.extensions.mongo.utils.MongoLauncher;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.upcasting.event.EventUpcaster;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.IOException;

import static org.junit.jupiter.api.Assumptions.*;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link MongoEventStorageEngine} with the {@link DBObjectXStreamSerializer}.
 *
 * @author Rene de Waele
 */
@DirtiesContext
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = MongoEventStorageEngineTest_DBObjectSerialization.TestContext.class)
class MongoEventStorageEngineTest_DBObjectSerialization extends AbstractMongoEventStorageEngineTest {

    private static MongodExecutable mongoExe;
    private static MongodProcess mongod;

    @Autowired
    private ApplicationContext context;
    private DefaultMongoTemplate mongoTemplate;

    @SuppressWarnings("FieldCanBeLocal")
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
        mongoTemplate.eventCollection().deleteMany(new BasicDBObject());
        mongoTemplate.snapshotCollection().deleteMany(new BasicDBObject());
        mongoTemplate.eventCollection().dropIndexes();
        mongoTemplate.snapshotCollection().dropIndexes();
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

    @Override
    protected AbstractEventStorageEngine createEngine(EventUpcaster upcasterChain) {
        Serializer serializer = context.getBean(Serializer.class);
        return MongoEventStorageEngine.builder()
                                      .snapshotSerializer(serializer)
                                      .upcasterChain(upcasterChain)
                                      .eventSerializer(serializer)
                                      .mongoTemplate(mongoTemplate)
                                      .build();
    }

    @Override
    protected AbstractEventStorageEngine createEngine(PersistenceExceptionResolver persistenceExceptionResolver) {
        Serializer serializer = context.getBean(Serializer.class);
        return MongoEventStorageEngine.builder()
                                      .snapshotSerializer(serializer)
                                      .persistenceExceptionResolver(persistenceExceptionResolver)
                                      .eventSerializer(serializer)
                                      .mongoTemplate(mongoTemplate)
                                      .build();
    }

    @Configuration
    public static class TestContext {

        @Bean
        public MongoEventStorageEngine mongoEventStorageEngine(Serializer serializer, MongoTemplate mongoTemplate) {
            return MongoEventStorageEngine.builder()
                                          .snapshotSerializer(serializer)
                                          .eventSerializer(serializer)
                                          .mongoTemplate(mongoTemplate)
                                          .build();
        }

        @Bean
        public Serializer serializer() {
            return DBObjectXStreamSerializer.builder().build();
        }

        @Bean
        public MongoTemplate mongoTemplate(MongoClient mongoClient) {
            return DefaultMongoTemplate.builder()
                                       .mongoDatabase(mongoClient)
                                       .build();
        }

        @Bean
        public MongoClient mongoClient(MongoFactory mongoFactory) {
            return mongoFactory.createMongo();
        }

        @Bean
        public MongoFactory mongoFactoryBean(MongoOptionsFactory mongoOptionsFactory) {
            MongoFactory mongoFactory = new MongoFactory();
            mongoFactory.setMongoOptions(mongoOptionsFactory.createMongoOptions());

            return mongoFactory;
        }

        @Bean
        public MongoOptionsFactory mongoOptionsFactory() {
            MongoOptionsFactory mongoOptionsFactory = new MongoOptionsFactory();
            mongoOptionsFactory.setConnectionsPerHost(100);
            mongoOptionsFactory.setWriteConcern(WriteConcern.JOURNALED);
            return mongoOptionsFactory;
        }

        @Bean
        public PlatformTransactionManager transactionManager() {
            return mock(PlatformTransactionManager.class);
        }
    }
}
