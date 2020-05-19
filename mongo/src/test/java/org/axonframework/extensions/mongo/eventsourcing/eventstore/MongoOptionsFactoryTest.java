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

import com.mongodb.MongoClientSettings;
import org.junit.jupiter.api.*;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link MongoOptionsFactory}.
 *
 * @author Jettro Coenradie
 */
class MongoOptionsFactoryTest {

    private MongoOptionsFactory factory;

    @BeforeEach
    void setUp() {
        factory = new MongoOptionsFactory();
    }

    @Test
    void testCreateMongoOptions_defaults() {
        MongoClientSettings options = factory.createMongoOptions();
        MongoClientSettings defaults = MongoClientSettings.builder().build();

        assertEquals(defaults.getConnectionPoolSettings().getMaxWaitTime(TimeUnit.MILLISECONDS), options.getConnectionPoolSettings().getMaxWaitTime(TimeUnit.MILLISECONDS));
        assertEquals(defaults.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS), options.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS));
        assertEquals(defaults.getConnectionPoolSettings().getMaxSize(), options.getConnectionPoolSettings().getMaxSize());
        // TODO connect time and socket timeout is identical
//        assertEquals(defaults.getConnectTimeout(), options.getConnectTimeout());
        // TODO deprecated
//        assertEquals(defaults.getThreadsAllowedToBlockForConnectionMultiplier(),
//                     options.getThreadsAllowedToBlockForConnectionMultiplier());
    }

    @Test
    void testCreateMongoOptions_customSet() {
        factory.setConnectionsPerHost(9);
        factory.setConnectionTimeout(11);
        factory.setMaxWaitTime(3);
        factory.setSocketTimeOut(23);
//        factory.setThreadsAllowedToBlockForConnectionMultiplier(31);

        MongoClientSettings options = factory.createMongoOptions();
        assertEquals(3, options.getConnectionPoolSettings().getMaxWaitTime(TimeUnit.MILLISECONDS));
        assertEquals(23, options.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS));
        assertEquals(9, options.getConnectionPoolSettings().getMaxSize());

        // TODO connect time and socket timeout is identical
//        assertEquals(11, options.getConnectTimeout());
        // TODO deprecated
//        assertEquals(31, options.getThreadsAllowedToBlockForConnectionMultiplier());
    }
}
