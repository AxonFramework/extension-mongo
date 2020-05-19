/*
 * Copyright (c) 2010-2016. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.extensions.mongo.eventsourcing.eventstore;

import com.mongodb.client.MongoClient;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClients;

import java.util.Collections;
import java.util.List;

/**
 * Convenience class for creating Mongo instances. It helps configuring a Mongo instance with a WriteConcern safe to use
 * in combination with the given server addresses.
 * <p/>
 * Depending on the number of addresses provided, the factory defaults to either {@link WriteConcern#W2} when
 * more than one address is provided, or {@link WriteConcern#JOURNALED} when only one server is available. The idea of
 * these defaults is that data must be able to survive a (not too heavy) crash without loss of data. We wouldn't want to
 * publish untraceable events, would we...
 *
 * @author Jettro Coenradie
 * @since 2.0 (in incubator since 0.7)
 */
public class MongoFactory {

    private MongoClientSettings mongoOptions = MongoClientSettings.builder().build();

    /**
     * Creates a mongo instance based on the provided configuration. Read javadoc of the class to learn about the
     * configuration options. A new Mongo instance is created each time this method is called.
     *
     * @return a new Mongo instance each time this method is called.
     */
    public MongoClient createMongo() {
        return MongoClients.create(mongoOptions);
    }

    /**
     * Provide an instance of MongoOptions to be used for the connections. Defaults to a MongoOptions with all its
     * default settings.
     *
     * @param mongoOptions MongoOptions to overrule the default
     */
    public void setMongoOptions(MongoClientSettings mongoOptions) {
        this.mongoOptions = mongoOptions;
    }

}
