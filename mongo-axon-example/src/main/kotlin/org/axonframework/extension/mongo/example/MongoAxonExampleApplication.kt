/*
 * Copyright (c) 2020. Axon Framework
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

package org.axonframework.extension.mongo.example

import com.mongodb.client.MongoClient
import org.axonframework.config.Configuration
import org.axonframework.config.Configurer
import org.axonframework.eventhandling.tokenstore.TokenStore
import org.axonframework.eventsourcing.EventCountSnapshotTriggerDefinition
import org.axonframework.eventsourcing.SnapshotTriggerDefinition
import org.axonframework.eventsourcing.eventstore.EventStorageEngine
import org.axonframework.extensions.mongo.DefaultMongoTemplate
import org.axonframework.extensions.mongo.eventsourcing.eventstore.MongoEventStorageEngine
import org.axonframework.extensions.mongo.eventsourcing.tokenstore.MongoTokenStore
import org.axonframework.serialization.Serializer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import org.springframework.scheduling.annotation.EnableScheduling


/**
 * Starting point.
 */
fun main(args: Array<String>) {
    SpringApplication.run(MongoAxonExampleApplication::class.java, *args)
}

/**
 * Main application class.
 */
@SpringBootApplication
@EnableScheduling
class MongoAxonExampleApplication {

    /**
     * Uses the Configurer to wire everything together including Mongo as the Event and Token Store.
     */
    @Autowired
    fun configuration(configurer: Configurer, client: MongoClient) {
        configurer
            .configureEmbeddedEventStore { storageEngine(client) }
            .eventProcessing { conf -> conf.registerTokenStore { tokenStore(client, it.serializer()) } }
    }

    /**
     * Create a Mongo based Event Storage Engine.
     */
    fun storageEngine(client: MongoClient): EventStorageEngine = MongoEventStorageEngine.builder()
        .mongoTemplate(
            DefaultMongoTemplate.builder()
                .mongoDatabase(client)
                .build()
        ).build()

    /**
     * Create a Mongo based Token Store.
     */
    fun tokenStore(client: MongoClient, serializer: Serializer): TokenStore = MongoTokenStore.builder()
        .mongoTemplate(
            DefaultMongoTemplate.builder()
                .mongoDatabase(client)
                .build()
        )
        .serializer(serializer)
        .build()

    /**
     * Configures a snapshot trigger to create a Snapshot every 5 events.
     * 5 is an arbitrary number used only for testing purposes just to show how the snapshots are stored on Mongo as well.
     */
    @Bean
    fun mySnapshotTriggerDefinition(configuration: Configuration): SnapshotTriggerDefinition {
        return EventCountSnapshotTriggerDefinition(configuration.snapshotter(), 5)
    }

}
