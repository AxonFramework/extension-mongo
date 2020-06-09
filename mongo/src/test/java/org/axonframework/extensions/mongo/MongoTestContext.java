package org.axonframework.extensions.mongo;

import com.mongodb.client.MongoClient;
import org.axonframework.extensions.mongo.eventhandling.saga.repository.MongoSagaStore;
import org.axonframework.extensions.mongo.eventsourcing.eventstore.MongoEventStorageEngine;
import org.axonframework.extensions.mongo.eventsourcing.eventstore.MongoFactory;
import org.axonframework.extensions.mongo.eventsourcing.eventstore.MongoSettingsFactory;
import org.axonframework.spring.saga.SpringResourceInjector;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import static org.mockito.Mockito.*;

@Configuration
public class MongoTestContext {

    @Bean
    public MongoEventStorageEngine mongoEventStorageEngine(MongoTemplate mongoTemplate) {
        return MongoEventStorageEngine.builder().mongoTemplate(mongoTemplate).build();
    }

    @Bean
    public MongoTemplate mongoTemplate(MongoClient mongoClient) {
        return DefaultMongoTemplate.builder().mongoDatabase(mongoClient).build();
    }

    @Bean
    public MongoClient mongoClient(MongoFactory mongoFactory) {
        return mongoFactory.createMongo();
    }

    @Bean
    public MongoFactory mongoFactoryBean(MongoSettingsFactory mongoSettingsFactory) {
        MongoFactory mongoFactory = new MongoFactory();
        mongoFactory.setMongoClientSettings(mongoSettingsFactory.createMongoClientSettings());
        return mongoFactory;
    }

    @Bean
    public MongoSettingsFactory mongoSettingsFactory() {
        MongoSettingsFactory mongoOptionsFactory = new MongoSettingsFactory();
        mongoOptionsFactory.setConnectionsPerHost(100);
        return mongoOptionsFactory;
    }

    @Bean
    public SpringResourceInjector springResourceInjector() {
        return new SpringResourceInjector();
    }

    @Bean
    public MongoSagaStore mongoSagaStore(MongoTemplate sagaMongoTemplate) {
        return MongoSagaStore.builder()
                             .mongoTemplate(sagaMongoTemplate)
                             .build();
    }

    @Bean
    public MongoTemplate sagaMongoTemplate(MongoClient mongoClient) {
        return DefaultMongoTemplate.builder().mongoDatabase(mongoClient).build();
    }

    @Bean
    public PlatformTransactionManager transactionManager() {
        return mock(PlatformTransactionManager.class);
    }
}
