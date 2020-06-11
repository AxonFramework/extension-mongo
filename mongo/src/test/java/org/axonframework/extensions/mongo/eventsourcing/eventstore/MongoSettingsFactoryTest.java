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
 * Test class validating the {@link MongoSettingsFactory}.
 *
 * @author Jettro Coenradie
 */
class MongoSettingsFactoryTest {

    private MongoSettingsFactory factory;

    @BeforeEach
    void setUp() {
        factory = new MongoSettingsFactory();
    }

    @Test
    void testCreateMongoClientSettings_defaults() {
        MongoClientSettings options = factory.createMongoClientSettings();
        MongoClientSettings defaults = MongoClientSettings.builder().build();

        assertEquals(defaults.getConnectionPoolSettings().getMaxWaitTime(TimeUnit.MILLISECONDS), options.getConnectionPoolSettings().getMaxWaitTime(TimeUnit.MILLISECONDS));
        assertEquals(defaults.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS), options.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS));
    }

    @Test
    void testCreateMongoClientSettings_customSet() {
        factory.setConnectionsPerHost(9);
        factory.setSocketConnectTimeout(11);
        factory.setMaxWaitTime(3);
        factory.setSocketReadTimeOut(23);

        MongoClientSettings options = factory.createMongoClientSettings();
        assertEquals(3, options.getConnectionPoolSettings().getMaxWaitTime(TimeUnit.MILLISECONDS));
        assertEquals(11, options.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS));
        assertEquals(23, options.getSocketSettings().getReadTimeout(TimeUnit.MILLISECONDS));
        assertEquals(9, options.getConnectionPoolSettings().getMaxSize());
    }
}
