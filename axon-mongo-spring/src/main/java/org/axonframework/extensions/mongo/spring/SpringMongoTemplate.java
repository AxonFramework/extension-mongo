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

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.extensions.mongo.DefaultMongoTemplate;
import org.axonframework.extensions.mongo.MongoTemplate;
import org.bson.Document;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoDatabaseUtils;

import java.util.function.Supplier;

import static org.axonframework.common.BuilderUtils.assertNonEmpty;
import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * MongoTemplate instance giving direct access to several collections via a given MongoDatabaseFactory instance. Will
 * use the default name for the collection, when none is set for the specific collections. By using the
 * {@link MongoDatabaseUtils} it makes sure transactionality works when the {@link SpringMongoTransactionManager} is
 * used as transaction manager.
 * </p>
 * The defaults are {@code domainevents} for the domain events, {@code snapshotevents} for the snapshots events,
 * {@code trackingtokens} for the tracking tokens, {@code sagas} for the sagas and {@code deadletters} for the dead
 * letters.
 *
 * @author Gerard Klijs
 * @since 4.7.0
 */
public class SpringMongoTemplate implements MongoTemplate {

    private final Supplier<MongoDatabase> databaseSupplier;
    private final String trackingTokensCollectionName;
    private final String domainEventsCollectionName;
    private final String snapshotEventsCollectionName;

    private final String sagasCollectionName;
    private final String deadLetterCollectionName;

    /**
     * Instantiate a {@link SpringMongoTemplate} based on the fields contained in the {@link Builder}.
     * <p>
     * Will assert that the {@link MongoDatabaseFactory} is not {@code null}, and will throw an
     * {@link AxonConfigurationException} if it is {@code null}.
     *
     * @param builder the {@link Builder} used to instantiate a {@link SpringMongoTemplate} instance
     */
    protected SpringMongoTemplate(Builder builder) {
        builder.validate();
        this.databaseSupplier = builder.databaseSupplier();
        this.trackingTokensCollectionName = builder.trackingTokensCollectionName;
        this.domainEventsCollectionName = builder.domainEventsCollectionName;
        this.snapshotEventsCollectionName = builder.snapshotEventsCollectionName;
        this.sagasCollectionName = builder.sagasCollectionName;
        this.deadLetterCollectionName = builder.deadLetterCollectionName;
    }

    /**
     * Instantiate a Builder to be able to create a {@link SpringMongoTemplate}.
     * <p>
     * The {@code domainEventsCollectionName}, {@code snapshotEventsCollectionName},
     * {@code trackingTokensCollectionName} {@code sagasCollectionName} and {@code deadLetterCollectionName} are
     * respectively defaulted to {@code trackingtokens}, {@code domainevents}, {@code snapshotevents}, {@code sagas} and
     * {@code deadletters}.
     * <p>
     * The {@link MongoDatabaseFactory} is a <b>hard requirement</b> and as such should be provided.
     *
     * @return a Builder to be able to create a {@link DefaultMongoTemplate}
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public MongoCollection<Document> trackingTokensCollection() {
        return databaseSupplier.get().getCollection(trackingTokensCollectionName);
    }

    @Override
    public MongoCollection<Document> eventCollection() {
        return databaseSupplier.get().getCollection(domainEventsCollectionName);
    }

    @Override
    public MongoCollection<Document> snapshotCollection() {
        return databaseSupplier.get().getCollection(snapshotEventsCollectionName);
    }

    @Override
    public MongoCollection<Document> sagaCollection() {
        return databaseSupplier.get().getCollection(sagasCollectionName);
    }

    @Override
    public MongoCollection<Document> deadLetterCollection() {
        return databaseSupplier.get().getCollection(deadLetterCollectionName);
    }

    /**
     * A Builder to be able to create a {@link SpringMongoTemplate}.
     * <p>
     * The {@code domainEventsCollectionName}, {@code snapshotEventsCollectionName},
     * {@code trackingTokensCollectionName} {@code sagasCollectionName} and {@code deadLetterCollectionName} are
     * respectively defaulted to {@code trackingtokens}, {@code domainevents}, {@code snapshotevents}, {@code sagas} and
     * {@code deadletters}.
     * <p>
     * The {@link MongoDatabaseFactory} is a <b>hard requirement</b> and as such should be provided.
     */
    public static class Builder {

        private MongoDatabaseFactory factory;
        private String databaseName = null;
        private String trackingTokensCollectionName = "trackingtokens";
        private String domainEventsCollectionName = "domainevents";
        private String snapshotEventsCollectionName = "snapshotevents";

        private String sagasCollectionName = "sagas";
        private String deadLetterCollectionName = "deadletters";

        /**
         * Sets the {@code factory} to use. Needs to be set in order to create an {@link SpringMongoTemplate}.
         *
         * @param factory a {@link MongoDatabaseFactory} used to create a Mongo database
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder factory(MongoDatabaseFactory factory) {
            assertNonNull(factory, "The factory may not be null");
            this.factory = factory;
            return this;
        }

        /**
         * Sets the {@code databaseName} to use as the database. Defaults to the default of the {@code factory}.
         *
         * @param databaseName a {@link String} specifying the database name
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder databaseName(String databaseName) {
            assertNonEmpty(databaseName, "The databaseName may not be empty");
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the {@code trackingTokensCollectionName} to use as the collection name for Tracking Tokens. Defaults to
         * a {@code "trackingtokens"} {@link String}.
         *
         * @param trackingTokensCollectionName a {@link String} specifying the collection name for Tracking Tokens
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder trackingTokensCollectionName(String trackingTokensCollectionName) {
            assertNonEmpty(trackingTokensCollectionName, "The trackingTokensCollectionName may not be empty");
            this.trackingTokensCollectionName = trackingTokensCollectionName;
            return this;
        }

        /**
         * Sets the {@code domainEventsCollectionName} to use as the collection name for Domain Events. Defaults to a
         * {@code "domainevents"} {@link String}.
         *
         * @param domainEventsCollectionName a {@link String} specifying the collection name for Domain Events
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder domainEventsCollectionName(String domainEventsCollectionName) {
            assertNonEmpty(domainEventsCollectionName, "The domainEventsCollectionName may not be empty");
            this.domainEventsCollectionName = domainEventsCollectionName;
            return this;
        }

        /**
         * Sets the {@code snapshotEventsCollectionName} to use as the collection name for Snapshot Events. Defaults to
         * a {@code "snapshotevents"} {@link String}.
         *
         * @param snapshotEventsCollectionName a {@link String} specifying the collection name for Snapshot Events
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder snapshotEventsCollectionName(String snapshotEventsCollectionName) {
            assertNonEmpty(snapshotEventsCollectionName, "The snapshotEventsCollectionName may not be empty");
            this.snapshotEventsCollectionName = snapshotEventsCollectionName;
            return this;
        }

        /**
         * Sets the {@code sagasCollectionName} to use as the collection name for Saga instances. Defaults to a
         * {@code "sagas"} {@link String}.
         *
         * @param sagasCollectionName a {@link String} specifying the collection name for Sagas
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder sagasCollectionName(String sagasCollectionName) {
            assertNonEmpty(sagasCollectionName, "The sagasCollectionName may not be empty");
            this.sagasCollectionName = sagasCollectionName;
            return this;
        }

        /**
         * Sets the {@code deadLetterCollectionName} to use as the collection name for Dead letters. Defaults to a
         * {@code "deadletters"} {@link String}.
         *
         * @param deadLetterCollectionName a {@link String} specifying the collection name for Dead letters
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder deadLetterCollectionName(String deadLetterCollectionName) {
            assertNonEmpty(deadLetterCollectionName, "The deadLetterCollectionName may not be empty");
            this.deadLetterCollectionName = deadLetterCollectionName;
            return this;
        }

        /**
         * Validates whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException if one field is asserted to be incorrect according to the Builder's
         *                                    specifications
         */
        protected void validate() throws AxonConfigurationException {
            assertNonNull(factory, "The MongoDatabaseFactory is a hard requirement and should be provided");
        }

        private Supplier<MongoDatabase> databaseSupplier() {
            return () -> MongoDatabaseUtils.getDatabase(databaseName, factory);
        }

        /**
         * Initializes a {@link SpringMongoTemplate} as specified through this Builder.
         *
         * @return a {@link SpringMongoTemplate} as specified through this Builder
         */
        public SpringMongoTemplate build() {
            return new SpringMongoTemplate(this);
        }
    }
}
