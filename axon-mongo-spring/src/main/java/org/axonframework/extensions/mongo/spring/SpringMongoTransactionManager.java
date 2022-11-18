/*
 * Copyright (c) 2010-2022. Axon Framework
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

package org.axonframework.extensions.mongo.spring;

import org.axonframework.spring.messaging.unitofwork.SpringTransactionManager;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoDatabaseUtils;
import org.springframework.data.mongodb.MongoTransactionManager;

/**
 * Transaction manager instance to be used with Spring and MongoDB. To make sure the transaction work correctly you also
 * need to either use the {@link SpringMongoTemplate}, or a custom template which will use
 * {@link MongoDatabaseUtils#getDatabase(MongoDatabaseFactory)} to get access to the collections.
 *
 * @author Gerard Klijs
 * @since 4.7.0
 */
public class SpringMongoTransactionManager extends SpringTransactionManager {

    /**
     * Creates a new {@link SpringMongoTransactionManager} using the supplied {@link MongoTransactionManager}.
     *
     * @param manager the {@link MongoTransactionManager} used to instantiate a {@link SpringMongoTransactionManager}
     *                instance
     */
    public SpringMongoTransactionManager(MongoTransactionManager manager) {
        super(manager);
    }
}
