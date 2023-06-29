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

package org.axonframework.extensions.mongo.util;

import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import org.axonframework.extensions.mongo.DefaultMongoTemplate;
import org.axonframework.extensions.mongo.MongoTemplate;
import org.axonframework.extensions.mongo.eventsourcing.eventstore.MongoFactory;
import org.axonframework.extensions.mongo.eventsourcing.eventstore.MongoSettingsFactory;

import java.util.Collections;
import java.util.function.Consumer;

/**
 * Utility class providing a factory for the {@link MongoTemplate} for testing.
 *
 * @author Steven van Beelen
 */
public abstract class MongoTemplateFactory {

    private MongoTemplateFactory() {
        // Test utility class
    }

    /**
     * Constructs a {@link MongoTemplate} connecting with the given {@code host} and {@code port}. Used for testing
     * purposes.
     *
     * @param host a {@link String} specifying the host of the MongoDb instance to connect with
     * @param port an {@code int} specifying the port of the MongoDb instance to connect with
     * @return a {@link MongoTemplate} connecting with the given {@code host} and {@code port} to be used during testing
     */
    public static MongoTemplate build(String host, int port) {
        return build(host, port, mongoSettingsFactory -> { });
    }

    /**
     * Constructs a {@link MongoTemplate} connecting with the given {@code host} and {@code port}. The {@code
     * settingsCustomization} is used to further fine tune the settings for creatin the {@code MongoTemplate}. Used for
     * testing purposes.
     *
     * @param host                  a {@link String} specifying the host of the MongoDb instance to connect with
     * @param port                  an {@code int} specifying the port of the MongoDb instance to connect with
     * @param settingsCustomization {@link Consumer} of the {@link MongoSettingsFactory} to allow for adding custom
     *                              settings
     * @return a {@link MongoTemplate} connecting with the given {@code host} and {@code port} to be used during testing
     */
    public static MongoTemplate build(String host,
                                      int port,
                                      Consumer<MongoSettingsFactory> settingsCustomization) {
        MongoSettingsFactory mongoSettingsFactory = new MongoSettingsFactory();
        ServerAddress containerAddress =
                new ServerAddress(host, port);
        mongoSettingsFactory.setMongoAddresses(Collections.singletonList(containerAddress));
        mongoSettingsFactory.setConnectionsPerHost(100);
        settingsCustomization.accept(mongoSettingsFactory);
        MongoFactory mongoFactory = new MongoFactory();
        mongoFactory.setMongoClientSettings(mongoSettingsFactory.createMongoClientSettings());
        MongoClient mongoClient = mongoFactory.createMongo();
        return DefaultMongoTemplate.builder()
                                   .mongoDatabase(mongoClient)
                                   .build();
    }
}
