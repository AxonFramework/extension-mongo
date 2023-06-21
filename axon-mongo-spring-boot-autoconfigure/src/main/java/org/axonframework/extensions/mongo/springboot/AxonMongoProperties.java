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

package org.axonframework.extensions.mongo.springboot;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Properties describing the settings for the mongo extension.
 *
 * @author Gerard Klijs
 * @since 4.7.0
 */
@ConfigurationProperties("axon.mongo")
public class AxonMongoProperties {

    /**
     * The database name to use for the Axon Framework Mongo Collections. If not set it will fall back to the factory
     * default, configured via Spring. For example via the {@code spring.data.mongodb.uri} property.
     */
    private String databaseName = null;

    /**
     * The AutoConfiguration settings for the token store
     */
    private TokenStore tokenStore = new TokenStore();

    /**
     * The AutoConfiguration settings for event handling related to this extension
     */
    private EventHandling eventHandling = new EventHandling();

    /**
     * The AutoConfiguration settings for the event store
     */
    private EventStore eventStore = new EventStore();

    /**
     * The AutoConfiguration settings for the saga store
     */
    private SagaStore sagaStore = new SagaStore();

    /**
     * Retrieves the database name to use for the Axon Framework Mongo Collections, will default to the one the
     * {@link org.springframework.data.mongodb.MongoDatabaseFactory} used by default.
     *
     * @return the database name to used for the Axon Framework Mongo Collections
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Sets the database name to use for the Axon Framework Mongo Collections, will default to the one the
     * {@link org.springframework.data.mongodb.MongoDatabaseFactory} used by default.
     *
     * @param databaseName the database name to use for the Axon Framework Mongo Collections
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Retrieves the AutoConfiguration settings for the token store.
     *
     * @return the AutoConfiguration settings for the token store
     */
    public TokenStore getTokenStore() {
        return tokenStore;
    }

    /**
     * Defines the AutoConfiguration settings for the token store.
     *
     * @param tokenStore the AutoConfiguration settings for the token store.
     */
    public void setTokenStore(TokenStore tokenStore) {
        this.tokenStore = tokenStore;
    }

    /**
     * Retrieves the AutoConfiguration settings related to event handling.
     *
     * @return the AutoConfiguration settings related to event handling.
     */
    public EventHandling getEventHandling() {
        return eventHandling;
    }

    /**
     * Defines the AutoConfiguration settings related to event handling.
     *
     * @param eventHandling the AutoConfiguration settings for the token store.
     */
    public void setEventHandling(EventHandling eventHandling) {
        this.eventHandling = eventHandling;
    }

    /**
     * Retrieves the AutoConfiguration settings for the event store.
     *
     * @return the AutoConfiguration settings for the event store
     */
    public EventStore getEventStore() {
        return eventStore;
    }

    /**
     * Defines the AutoConfiguration settings for the event store.
     *
     * @param eventStore the AutoConfiguration settings for the event store.
     */
    public void setEventStore(EventStore eventStore) {
        this.eventStore = eventStore;
    }

    /**
     * Retrieves the AutoConfiguration settings for the saga store.
     *
     * @return the AutoConfiguration settings for the saga store
     */
    public SagaStore getSagaStore() {
        return sagaStore;
    }

    /**
     * Defines the AutoConfiguration settings for the saga store.
     *
     * @param sagaStore the AutoConfiguration settings for the saga store.
     */
    public void setSagaStore(SagaStore sagaStore) {
        this.sagaStore = sagaStore;
    }

    public static class TokenStore {

        /**
         * Enables creation of the {@link org.axonframework.extensions.mongo.eventsourcing.tokenstore.MongoTokenStore}.
         * Defaults to "true".
         */
        private boolean enabled = true;

        /**
         * Indicates whether creating the
         * {@link org.axonframework.extensions.mongo.eventsourcing.tokenstore.MongoTokenStore} is enabled.
         *
         * @return true if creating the token store is enabled, false if otherwise
         */
        public boolean isEnabled() {
            return enabled;
        }

        /**
         * Enables (if {@code true}, default) or disables (if {@code false}) creating the
         * {@link org.axonframework.extensions.mongo.eventsourcing.tokenstore.MongoTokenStore}.
         *
         * @param enabled whether to enable token store creation
         */
        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static class EventHandling {

        /**
         * Enables setting the
         * {@link org.axonframework.extensions.mongo.eventhandling.deadletter.MongoSequencedDeadLetterQueue} as the
         * default {@link org.axonframework.messaging.deadletter.SequencedDeadLetterQueue}. Defaults to "true".
         */
        private boolean dlqEnabled = true;

        /**
         * Indicates whether setting a
         * {@link org.axonframework.extensions.mongo.eventhandling.deadletter.MongoSequencedDeadLetterQueue} as default
         * sequenced dead-letter queue is enabled.
         *
         * @return {@code true} if creating the sequenced dead-letter queue is enabled, {@code false} otherwise.
         */
        public boolean isDlqEnabled() {
            return dlqEnabled;
        }

        /**
         * Enables (if {@code true}, default) or disables (if {@code false}) setting a
         * {@link org.axonframework.extensions.mongo.eventhandling.deadletter.MongoSequencedDeadLetterQueue} as default
         * sequenced dead letter queue.
         *
         * @param dlqEnabled Whether to enable setting a default for the sequenced dead-letter queue.
         */
        public void setDlqEnabled(boolean dlqEnabled) {
            this.dlqEnabled = dlqEnabled;
        }
    }

    public static class EventStore {

        /**
         * Enables creation of the
         * {@link org.axonframework.extensions.mongo.eventsourcing.eventstore.MongoEventStorageEngine}. Defaults to
         * "false".
         */
        private boolean enabled = false;

        /**
         * Indicates whether creating the
         * {@link org.axonframework.extensions.mongo.eventsourcing.eventstore.MongoEventStorageEngine} is enabled.
         *
         * @return true if creating the event store is enabled, false if otherwise
         */
        public boolean isEnabled() {
            return enabled;
        }

        /**
         * Disables (if {@code false}, default) or enables (if {@code true}) creating the
         * {@link org.axonframework.extensions.mongo.eventsourcing.eventstore.MongoEventStorageEngine}.
         *
         * @param enabled whether to enable event store creation
         */
        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static class SagaStore {

        /**
         * Enables creation of the
         * {@link org.axonframework.extensions.mongo.eventhandling.saga.repository.MongoSagaStore}. Defaults to "true".
         */
        private boolean enabled = true;

        /**
         * Indicates whether creating the
         * {@link org.axonframework.extensions.mongo.eventhandling.saga.repository.MongoSagaStore} is enabled.
         *
         * @return true if creating the saga store is enabled, false if otherwise
         */
        public boolean isEnabled() {
            return enabled;
        }

        /**
         * Enables (if {@code true}, default) or disables (if {@code false}) creating the
         * {@link org.axonframework.extensions.mongo.eventhandling.saga.repository.MongoSagaStore}.
         *
         * @param enabled whether to enable saga store creation
         */
        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }
}
