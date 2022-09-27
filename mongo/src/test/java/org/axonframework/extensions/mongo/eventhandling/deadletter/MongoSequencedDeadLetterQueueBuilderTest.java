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

package org.axonframework.extensions.mongo.eventhandling.deadletter;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.extensions.mongo.MongoTemplate;
import org.axonframework.serialization.TestSerializer;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MongoSequencedDeadLetterQueueBuilderTest {

    private final MongoTemplate mongoTemplate = mock(MongoTemplate.class);

    @Test
    void buildWithNegativeMaxQueuesThrowsAxonConfigurationException() {
        MongoSequencedDeadLetterQueue.Builder<EventMessage<?>> builderTestSubject = MongoSequencedDeadLetterQueue.builder();

        assertThrows(AxonConfigurationException.class, () -> builderTestSubject.maxSequences(-1));
    }

    @Test
    void buildWithZeroMaxQueuesThrowsAxonConfigurationException() {
        MongoSequencedDeadLetterQueue.Builder<EventMessage<?>> builderTestSubject = MongoSequencedDeadLetterQueue.builder();

        assertThrows(AxonConfigurationException.class, () -> builderTestSubject.maxSequences(0));
    }

    @Test
    void buildWithNegativeMaxQueueSizeThrowsAxonConfigurationException() {
        MongoSequencedDeadLetterQueue.Builder<EventMessage<?>> builderTestSubject = MongoSequencedDeadLetterQueue.builder();

        assertThrows(AxonConfigurationException.class, () -> builderTestSubject.maxSequenceSize(-1));
    }

    @Test
    void buildWithZeroMaxQueueSizeThrowsAxonConfigurationException() {
        MongoSequencedDeadLetterQueue.Builder<EventMessage<?>> builderTestSubject = MongoSequencedDeadLetterQueue.builder();

        assertThrows(AxonConfigurationException.class, () -> builderTestSubject.maxSequenceSize(0));
    }

    @Test
    void canNotSetProcessingGroupToEmpty() {
        MongoSequencedDeadLetterQueue.Builder<EventMessage<?>> builder = MongoSequencedDeadLetterQueue.builder();
        assertThrows(AxonConfigurationException.class, () -> {
            builder.processingGroup("");
        });
    }

    @Test
    void canNotSetProcessingGroupToNull() {
        MongoSequencedDeadLetterQueue.Builder<EventMessage<?>> builder = MongoSequencedDeadLetterQueue.builder();
        assertThrows(AxonConfigurationException.class, () -> {
            builder.processingGroup("");
        });
    }

    @Test
    void canNotSetMongoTemplateToNull() {
        MongoSequencedDeadLetterQueue.Builder<EventMessage<?>> builder = MongoSequencedDeadLetterQueue.builder();
        assertThrows(AxonConfigurationException.class, () -> {
            builder.mongoTemplate(null);
        });
    }

    @Test
    void canNotSetConverterToNull() {
        MongoSequencedDeadLetterQueue.Builder<EventMessage<?>> builder = MongoSequencedDeadLetterQueue.builder();
        assertThrows(AxonConfigurationException.class, () -> {
            builder.addConverter(null);
        });
    }

    @Test
    void canNotSetClaimDurationToNull() {
        MongoSequencedDeadLetterQueue.Builder<EventMessage<?>> builder = MongoSequencedDeadLetterQueue.builder();
        assertThrows(AxonConfigurationException.class, () -> {
            builder.claimDuration(null);
        });
    }

    @Test
    void canNotSetTransactionManagerToNull() {
        MongoSequencedDeadLetterQueue.Builder<EventMessage<?>> builder = MongoSequencedDeadLetterQueue.builder();
        assertThrows(AxonConfigurationException.class, () -> {
            builder.transactionManager(null);
        });
    }

    @Test
    void canNotBuildWithoutProcessingGroup() {
        MongoSequencedDeadLetterQueue.Builder<EventMessage<?>> builder = MongoSequencedDeadLetterQueue
                .builder()
                .mongoTemplate(mongoTemplate)
                .serializer(TestSerializer.JACKSON.getSerializer());
        assertThrows(AxonConfigurationException.class, builder::build);
    }

    @Test
    void canNotBuildWithoutSerializer() {
        MongoSequencedDeadLetterQueue.Builder<EventMessage<?>> builder = MongoSequencedDeadLetterQueue
                .builder()
                .processingGroup("my_processing_Group")
                .mongoTemplate(mongoTemplate);
        assertThrows(AxonConfigurationException.class, builder::build);
    }

    @Test
    void canNotBuildWithoutMongoTemplate() {
        MongoSequencedDeadLetterQueue.Builder<EventMessage<?>> builder = MongoSequencedDeadLetterQueue
                .builder()
                .processingGroup("my_processing_Group")
                .serializer(TestSerializer.JACKSON.getSerializer());
        assertThrows(AxonConfigurationException.class, builder::build);
    }
}
