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

import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.eventhandling.EventBus;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.extensions.mongo.MongoTemplate;
import org.axonframework.extensions.mongo.eventhandling.saga.repository.MongoSagaStore;
import org.axonframework.extensions.mongo.eventsourcing.eventstore.MongoEventStorageEngine;
import org.axonframework.extensions.mongo.eventsourcing.eventstore.StorageStrategy;
import org.axonframework.extensions.mongo.eventsourcing.eventstore.documentperevent.DocumentPerEventStorageStrategy;
import org.axonframework.extensions.mongo.eventsourcing.tokenstore.MongoTokenStore;
import org.axonframework.extensions.mongo.spring.SpringMongoTemplate;
import org.axonframework.extensions.mongo.spring.SpringMongoTransactionManager;
import org.axonframework.extensions.mongo.springboot.AxonMongoProperties;
import org.axonframework.modelling.saga.repository.SagaStore;
import org.axonframework.serialization.Serializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;

import static java.util.Objects.isNull;

/**
 * Mongo autoconfiguration class for Axon Framework application. Constructs the components which can be supplied by the
 * {@code axon-mongo} module when enabled and not otherwise already being created. These are the token store
 * ({@link  MongoTokenStore}), event store ({@link MongoEventStorageEngine}) and saga store ({@link MongoSagaStore}).
 * <p>
 * It will use the Spring versions of the transaction manager, to make sure transactions work as expected.
 * <p>
 * Please note the dead letter queue
 * ({@link org.axonframework.extensions.mongo.eventhandling.deadletter.MongoSequencedDeadLetterQueue}) will never be
 * automatically created, and requires manual setup if you want to use it. You can use the transaction manager
 * ({@link SpringMongoTransactionManager}) and mongo template ({@link SpringMongoTemplate}) provided by this class when
 * doing so.
 *
 * @author Gerard Klijs
 * @since 4.7.0
 */
@AutoConfiguration
@EnableConfigurationProperties(AxonMongoProperties.class)
@AutoConfigureAfter(name = {
        "org.axonframework.springboot.autoconfig.TransactionAutoConfiguration",
        "org.axonframework.springboot.autoconfig.JdbcAutoConfiguration"
})
public class MongoAutoConfiguration {

    private final AxonMongoProperties axonMongoProperties;

    public MongoAutoConfiguration(AxonMongoProperties axonMongoProperties) {
        this.axonMongoProperties = axonMongoProperties;
    }

    @Bean
    @ConditionalOnMissingBean
    public MongoTransactionManager mongoTransactionManager(MongoDatabaseFactory factory) {
        return new MongoTransactionManager(
                factory,
                TransactionOptions.builder().writeConcern(WriteConcern.MAJORITY).build()
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public TransactionManager transactionManager(MongoTransactionManager manager) {
        return new SpringMongoTransactionManager(manager);
    }

    @Bean
    @ConditionalOnMissingBean
    public MongoTemplate axonMongoTemplate(MongoDatabaseFactory factory) {
        String databaseName = axonMongoProperties.getDatabaseName();
        if (isNull(databaseName)) {
            return SpringMongoTemplate.builder()
                                      .factory(factory)
                                      .build();
        } else {
            return SpringMongoTemplate.builder()
                                      .factory(factory)
                                      .databaseName(databaseName)
                                      .build();
        }
    }

    @Bean("tokenStore")
    @ConditionalOnMissingBean(TokenStore.class)
    @ConditionalOnProperty(value = "axon.mongo.token-store.enabled", matchIfMissing = true)
    public TokenStore tokenStore(
            MongoTemplate mongoTemplate,
            TransactionManager transactionManager,
            Serializer serializer
    ) {
        return MongoTokenStore.builder()
                              .mongoTemplate(mongoTemplate)
                              .transactionManager(transactionManager)
                              .serializer(serializer)
                              .build();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "axon.mongo.event-store.enabled")
    public StorageStrategy storageStrategy() {
        return new DocumentPerEventStorageStrategy();
    }

    @Bean
    @ConditionalOnMissingBean({EventStorageEngine.class, EventBus.class})
    @ConditionalOnProperty(value = "axon.mongo.event-store.enabled")
    public EventStorageEngine eventStorageEngine(
            MongoTemplate mongoTemplate,
            TransactionManager transactionManager,
            Serializer snapshotSerializer,
            @Qualifier("eventSerializer") Serializer serializer,
            StorageStrategy storageStrategy,
            org.axonframework.config.Configuration configuration
    ) {
        return MongoEventStorageEngine.builder()
                                      .mongoTemplate(mongoTemplate)
                                      .transactionManager(transactionManager)
                                      .snapshotSerializer(snapshotSerializer)
                                      .eventSerializer(serializer)
                                      .storageStrategy(storageStrategy)
                                      .upcasterChain(configuration.upcasterChain())
                                      .build();
    }

    @Bean
    @ConditionalOnMissingBean(SagaStore.class)
    @ConditionalOnProperty(value = "axon.mongo.saga-store.enabled", matchIfMissing = true)
    public MongoSagaStore sagaStore(
            MongoTemplate mongoTemplate,
            TransactionManager transactionManager,
            Serializer serializer
    ) {
        return MongoSagaStore.builder()
                             .mongoTemplate(mongoTemplate)
                             .transactionManager(transactionManager)
                             .serializer(serializer)
                             .build();
    }
}
