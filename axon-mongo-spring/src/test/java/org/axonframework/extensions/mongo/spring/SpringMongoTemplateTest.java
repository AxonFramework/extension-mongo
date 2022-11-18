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
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.springframework.data.mongodb.MongoDatabaseFactory;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SpringMongoTemplateTest {

    private MongoDatabaseFactory mockFactory;
    private MongoDatabase mockDatabase;
    private SpringMongoTemplate testSubject;

    @BeforeEach
    void createFixtures() {
        mockFactory = mock(MongoDatabaseFactory.class);
        mockDatabase = mock(MongoDatabase.class);
        //noinspection unchecked
        MongoCollection<Document> mockCollection = mock(MongoCollection.class);

        when(mockFactory.getMongoDatabase()).thenReturn(mockDatabase);
        when(mockDatabase.getCollection(anyString())).thenReturn(mockCollection);
    }

    @Test
    void customProvidedCollectionNamesShouldBeUsed() {
        testSubject = SpringMongoTemplate
                .builder()
                .factory(mockFactory)
                .trackingTokensCollectionName("custom-trackingtokens")
                .domainEventsCollectionName("custom-domainevents")
                .snapshotEventsCollectionName("custom-snapshotevents")
                .sagasCollectionName("custom-sagas")
                .deadLetterCollectionName("custom-deadletters")
                .build();


        testSubject.trackingTokensCollection();
        verify(mockDatabase).getCollection("custom-trackingtokens");
        testSubject.eventCollection();
        verify(mockDatabase).getCollection("custom-domainevents");
        testSubject.snapshotCollection();
        verify(mockDatabase).getCollection("custom-snapshotevents");
        testSubject.sagaCollection();
        verify(mockDatabase).getCollection("custom-sagas");
        testSubject.deadLetterCollection();
        verify(mockDatabase).getCollection("custom-deadletters");
        verifyNoMoreInteractions(mockDatabase);
    }

    @Test
    void customProvidedDatabaseNameShouldBeUsed() {
        when(mockFactory.getMongoDatabase("custom-databasename")).thenReturn(mockDatabase);
        testSubject = SpringMongoTemplate
                .builder()
                .factory(mockFactory)
                .databaseName("custom-databasename")
                .build();


        testSubject.trackingTokensCollection();
        verify(mockFactory).getMongoDatabase("custom-databasename");
        verifyNoMoreInteractions(mockFactory);
        verify(mockDatabase).getCollection("trackingtokens");
        verifyNoMoreInteractions(mockDatabase);
    }

    @Test
    void trackingTokensDefaultValue() {
        testSubject = SpringMongoTemplate.builder().factory(mockFactory).build();

        testSubject.trackingTokensCollection();

        verify(mockDatabase).getCollection("trackingtokens");
    }

    @Test
    void domainEventsDefaultValue() {
        testSubject = SpringMongoTemplate.builder().factory(mockFactory).build();

        testSubject.eventCollection();

        verify(mockDatabase).getCollection("domainevents");
    }

    @Test
    void snapshotEventsDefaultValue() {
        testSubject = SpringMongoTemplate.builder().factory(mockFactory).build();

        testSubject.snapshotCollection();

        verify(mockDatabase).getCollection("snapshotevents");
    }

    @Test
    void sagasDefaultValue() {
        testSubject = SpringMongoTemplate.builder().factory(mockFactory).build();

        testSubject.sagaCollection();

        verify(mockDatabase).getCollection("sagas");
    }

    @Test
    void deadLetterDefaultValue() {
        testSubject = SpringMongoTemplate.builder().factory(mockFactory).build();

        testSubject.deadLetterCollection();

        verify(mockDatabase).getCollection("deadletters");
    }

    @Test
    void settingNullFactoryShouldThrowError() {
        SpringMongoTemplate.Builder builder = SpringMongoTemplate.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.factory(null));
    }

    @Test
    void settingNullDatabaseNameShouldThrowError() {
        SpringMongoTemplate.Builder builder = SpringMongoTemplate.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.databaseName(null));
    }

    @Test
    void settingNullTrackingTokensCollectionNameShouldThrowError() {
        SpringMongoTemplate.Builder builder = SpringMongoTemplate.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.trackingTokensCollectionName(null));
    }

    @Test
    void settingNullDomainEventsCollectionNameShouldThrowError() {
        SpringMongoTemplate.Builder builder = SpringMongoTemplate.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.domainEventsCollectionName(null));
    }

    @Test
    void settingNullSnapshotEventsCollectionNameShouldThrowError() {
        SpringMongoTemplate.Builder builder = SpringMongoTemplate.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.snapshotEventsCollectionName(null));
    }

    @Test
    void settingNullSagasCollectionNameShouldThrowError() {
        SpringMongoTemplate.Builder builder = SpringMongoTemplate.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.sagasCollectionName(null));
    }

    @Test
    void settingNullDeadLetterCollectionNameShouldThrowError() {
        SpringMongoTemplate.Builder builder = SpringMongoTemplate.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.deadLetterCollectionName(null));
    }
}
