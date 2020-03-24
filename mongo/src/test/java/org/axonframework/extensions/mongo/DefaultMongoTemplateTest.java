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

package org.axonframework.extensions.mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.*;

import static org.mockito.Mockito.*;

class DefaultMongoTemplateTest {

    private MongoClient mockMongo;
    private MongoDatabase mockDb;
    private DefaultMongoTemplate testSubject;

    @BeforeEach
    void createFixtures() {
        mockMongo = mock(MongoClient.class);
        mockDb = mock(MongoDatabase.class);
        //noinspection unchecked
        MongoCollection<Document> mockCollection = mock(MongoCollection.class);

        when(mockMongo.getDatabase(anyString())).thenReturn(mockDb);
        when(mockDb.getCollection(anyString())).thenReturn(mockCollection);
    }

    @Test
    void testTrackingTokenDefaultValues() {
        testSubject = DefaultMongoTemplate.builder().mongoDatabase(mockMongo).build();

        verify(mockMongo).getDatabase("axonframework");

        testSubject.trackingTokensCollection();
        verify(mockDb).getCollection("trackingtokens");
    }

    @Test
    void testTrackingTokenCustomValues() {
        testSubject = DefaultMongoTemplate.builder()
                                          .mongoDatabase(mockMongo, "customDatabaseName")
                                          .build()
                                          .withTrackingTokenCollection("customCollectionName");

        verify(mockMongo).getDatabase("customDatabaseName");
        testSubject.trackingTokensCollection();
        verify(mockDb).getCollection("customCollectionName");
    }

    @Test
    void testSagasDefaultValues() {
        testSubject = DefaultMongoTemplate.builder().mongoDatabase(mockMongo).build();

        testSubject.sagaCollection();
        verify(mockDb).getCollection("sagas");
    }

    @Test
    void testCustomProvidedNames() {
        testSubject = DefaultMongoTemplate.builder().mongoDatabase(mockMongo).build()
                                          .withSagasCollection("custom-sagas");

        testSubject.sagaCollection();
        verify(mockDb).getCollection("custom-sagas");
    }

    @Test
    void testDomainEvents() {
        testSubject = DefaultMongoTemplate.builder().mongoDatabase(mockMongo).build();

        testSubject.eventCollection();
        verify(mockDb).getCollection("domainevents");
    }

    @Test
    void testSnapshotEvents() {
        testSubject = DefaultMongoTemplate.builder().mongoDatabase(mockMongo).build();

        testSubject.snapshotCollection();

        verify(mockDb).getCollection("snapshotevents");
    }

    @Test
    void testEventsCollectionWithCustomProvidedNames() {
        testSubject = DefaultMongoTemplate.builder().mongoDatabase(mockMongo).build()
                                          .withDomainEventsCollection("custom-events")
                                          .withSnapshotCollection("custom-snapshots");

        testSubject.eventCollection();
        verify(mockDb).getCollection("custom-events");
    }

    @Test
    void testSnapshotsCollectionWithCustomProvidedNames() {
        testSubject = DefaultMongoTemplate.builder().mongoDatabase(mockMongo).build()
                                          .withDomainEventsCollection("custom-events")
                                          .withSnapshotCollection("custom-snapshots");

        testSubject.snapshotCollection();
        verify(mockDb).getCollection("custom-snapshots");
    }
}
