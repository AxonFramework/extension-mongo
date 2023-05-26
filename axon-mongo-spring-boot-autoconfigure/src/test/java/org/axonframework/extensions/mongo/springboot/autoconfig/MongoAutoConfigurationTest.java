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

package org.axonframework.extensions.mongo.springboot.autoconfig;

import com.mongodb.ClientSessionOptions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.axonframework.common.ReflectionUtils;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.config.EventProcessingModule;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.extensions.mongo.eventhandling.deadletter.MongoSequencedDeadLetterQueue;
import org.axonframework.extensions.mongo.eventsourcing.eventstore.StorageStrategy;
import org.axonframework.extensions.mongo.eventsourcing.tokenstore.MongoTokenStore;
import org.axonframework.messaging.deadletter.SequencedDeadLetterQueue;
import org.axonframework.modelling.saga.repository.SagaStore;
import org.axonframework.serialization.Serializer;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.test.context.ContextConfiguration;

import java.time.Duration;
import java.time.temporal.TemporalAmount;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MongoAutoConfigurationTest {

    private ApplicationContextRunner testApplicationContext;
    private static ClientSession mockSession;
    private static MongoDatabase mockDatabase;
    private static MongoCollection<Document> mockCollection;
    private static MongoDatabaseFactory mockFactory;

    @BeforeAll
    @SuppressWarnings("unchecked")
    static void createMocks() {
        mockSession = mock(ClientSession.class);
        mockDatabase = mock(MongoDatabase.class);
        mockCollection = mock(MongoCollection.class);
        mockFactory = mock(MongoDatabaseFactory.class);
    }

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        reset(mockSession);
        reset(mockDatabase);
        reset(mockCollection);
        reset(mockFactory);
        testApplicationContext = new ApplicationContextRunner()
                .withPropertyValues("axon.axonserver.enabled=false");
    }

    @Test
    void whenNoPropertiesSetShouldCreateTheComponentsThatAreEnabledByDefault() {
        testApplicationContext
                .withUserConfiguration(DefaultContext.class)
                .run(context -> {
                    assertEquals(1, context.getBeansOfType(TransactionManager.class).size());
                    assertEquals(1,
                                 context.getBeansOfType(org.axonframework.extensions.mongo.MongoTemplate.class).size());
                    assertEquals(1, context.getBeansOfType(TokenStore.class).size());
                    assertEquals(0, context.getBeansOfType(StorageStrategy.class).size());
                    assertEquals(0, context.getBeansOfType(EventStorageEngine.class).size());
                    assertEquals(1, context.getBeansOfType(SagaStore.class).size());
                    verify(mockFactory).getMongoDatabase();
                });
    }

    @Test
    void whenPropertiesSetShouldCreateTheComponentsThatAreEnabledByThoseProperties() {
        testApplicationContext
                .withUserConfiguration(DefaultContext.class)
                .withPropertyValues("axon.mongo.database-name=testdb")
                .withPropertyValues("axon.mongo.token-store.enabled=false")
                .withPropertyValues("axon.mongo.event-store.enabled=true")
                .withPropertyValues("axon.mongo.saga-store.enabled=false")
                .run(context -> {
                    assertEquals(1, context.getBeansOfType(TransactionManager.class).size());
                    assertEquals(1,
                                 context.getBeansOfType(org.axonframework.extensions.mongo.MongoTemplate.class).size());
                    assertEquals(0, context.getBeansOfType(TokenStore.class).size());
                    assertEquals(1, context.getBeansOfType(StorageStrategy.class).size());
                    assertEquals(1, context.getBeansOfType(EventStorageEngine.class).size());
                    assertEquals(0, context.getBeansOfType(SagaStore.class).size());
                    verify((mockFactory).getMongoDatabase("testdb"), times(2));
                });
    }

    @Test
    void whenBogusStorageStrategyFallBackToDefault() {
        testApplicationContext
                .withUserConfiguration(DefaultContext.class)
                .withPropertyValues("axon.mongo.token-store.enabled=false")
                .withPropertyValues("axon.mongo.event-store.enabled=true")
                .withPropertyValues("axon.mongo.event-store.storage-strategy=foo.bar.StorageStrategy")
                .withPropertyValues("axon.mongo.saga-store.enabled=false")
                .run(context -> {
                    assertEquals(0, context.getBeansOfType(TokenStore.class).size());
                    assertEquals(1, context.getBeansOfType(EventStorageEngine.class).size());
                    assertEquals(0, context.getBeansOfType(SagaStore.class).size());
                });
    }

    @Test
    void setTokenStoreClaimTimeout() {
        testApplicationContext
                .withUserConfiguration(DefaultContext.class)
                .withPropertyValues("axon.eventhandling.tokenstore.claim-timeout=15s")
                .run(context -> {
                    Map<String, TokenStore> tokenStores =
                            context.getBeansOfType(TokenStore.class);
                    assertTrue(tokenStores.containsKey("tokenStore"));
                    TokenStore tokenStore = tokenStores.get("tokenStore");
                    TemporalAmount tokenClaimInterval = ReflectionUtils.getFieldValue(
                            MongoTokenStore.class.getDeclaredField("claimTimeout"), tokenStore
                    );
                    assertEquals(Duration.ofSeconds(15L), tokenClaimInterval);
                });
    }

    @Test
    void sequencedDeadLetterQueueCanBeSetViaSpringConfiguration() {
        testApplicationContext
                .withUserConfiguration(DefaultContext.class)
                .withPropertyValues("axon.eventhandling.processors.first.dlq.enabled=true")
                .run(context -> {
                    EventProcessingModule eventProcessingConfig = context.getBean(EventProcessingModule.class);
                    assertNotNull(eventProcessingConfig);
                    Optional<SequencedDeadLetterQueue<EventMessage<?>>> dlq =
                            eventProcessingConfig.deadLetterQueue("first");
                    assertTrue(dlq.isPresent());
                    assertTrue(dlq.get() instanceof MongoSequencedDeadLetterQueue);
                    dlq = eventProcessingConfig.deadLetterQueue("second");
                    assertFalse(dlq.isPresent());
                });
    }

    @Test
    void deadLetterProviderCanBeDisabled() {
        testApplicationContext
                .withUserConfiguration(DefaultContext.class)
                .withPropertyValues(
                        "axon.mongo.event-handling.dlq-enabled=false",
                        "axon.eventhandling.processors.first.dlq.enabled=true"
                )
                .run(context -> {
                    EventProcessingModule eventProcessingConfig = context.getBean(EventProcessingModule.class);
                    assertNotNull(eventProcessingConfig);
                    Optional<SequencedDeadLetterQueue<EventMessage<?>>> dlq =
                            eventProcessingConfig.deadLetterQueue("first");
                    assertFalse(dlq.isPresent());
                });
    }

    @ContextConfiguration
    @EnableAutoConfiguration(exclude = {
            MongoDataAutoConfiguration.class
    })
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    public static class DefaultContext {

        @Bean
        @Primary
        public Serializer mockSerializer() {
            return mock(Serializer.class);
        }

        @Bean
        @Qualifier("eventSerializer")
        public Serializer mockEventSerializer() {
            return mock(Serializer.class);
        }

        @Bean
        public MongoDatabaseFactory mockMongoDatabaseFactory() {
            when(mockFactory.getSession(any(ClientSessionOptions.class))).thenReturn(mockSession);
            when(mockFactory.getMongoDatabase()).thenReturn(mockDatabase);
            when(mockFactory.getMongoDatabase("testdb")).thenReturn(mockDatabase);
            when(mockFactory.withSession(mockSession)).thenReturn(mockFactory);
            when(mockDatabase.getCollection(anyString())).thenReturn(mockCollection);
            return mockFactory;
        }

        @Bean
        public MongoTemplate mockMongoTemplate() {
            return mock(MongoTemplate.class);
        }

        @Bean
        MappingMongoConverter mockMappingMongoConverter() {
            MappingMongoConverter converter = mock(MappingMongoConverter.class);
            when(converter.with(any(MongoDatabaseFactory.class))).thenReturn(converter);
            return converter;
        }
    }
}
