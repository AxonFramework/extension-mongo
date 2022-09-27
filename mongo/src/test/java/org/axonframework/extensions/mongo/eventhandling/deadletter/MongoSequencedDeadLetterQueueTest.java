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

import com.mongodb.BasicDBObject;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.extensions.mongo.MongoTemplate;
import org.axonframework.extensions.mongo.util.MongoTemplateFactory;
import org.axonframework.extensions.mongo.utils.TestSerializer;
import org.axonframework.messaging.MetaData;
import org.axonframework.messaging.deadletter.DeadLetter;
import org.axonframework.messaging.deadletter.GenericDeadLetter;
import org.axonframework.messaging.deadletter.SequencedDeadLetterQueue;
import org.axonframework.messaging.deadletter.SequencedDeadLetterQueueTest;
import org.axonframework.messaging.deadletter.WrongDeadLetterTypeException;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Clock;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class MongoSequencedDeadLetterQueueTest extends SequencedDeadLetterQueueTest<EventMessage<?>> {

    @Container
    private static final MongoDBContainer MONGO_CONTAINER = new MongoDBContainer("mongo:5");
    private static final String PROCESSING_GROUP = "processing-group";
    private static final int MAX_SEQUENCES_AND_SEQUENCE_SIZE = 128;

    @Override
    protected SequencedDeadLetterQueue<EventMessage<?>> buildTestSubject() {
        MongoTemplate mongoTemplate = MongoTemplateFactory.build(
                MONGO_CONTAINER.getHost(), MONGO_CONTAINER.getFirstMappedPort()
        );
        mongoTemplate.deadLetterCollection().deleteMany(new BasicDBObject());
        return MongoSequencedDeadLetterQueue
                .builder()
                .processingGroup(PROCESSING_GROUP)
                .maxSequences(MAX_SEQUENCES_AND_SEQUENCE_SIZE)
                .maxSequenceSize(MAX_SEQUENCES_AND_SEQUENCE_SIZE)
                .serializer(TestSerializer.xStreamSerializer())
                .mongoTemplate(mongoTemplate)
                .build();
    }

    @Override
    protected long maxSequences() {
        return MAX_SEQUENCES_AND_SEQUENCE_SIZE;
    }

    @Override
    protected long maxSequenceSize() {
        return MAX_SEQUENCES_AND_SEQUENCE_SIZE;
    }

    @Override
    protected DeadLetter<EventMessage<?>> generateInitialLetter() {
        return new GenericDeadLetter<>("sequenceIdentifier", generateEvent(), generateThrowable());
    }

    @Override
    protected DeadLetter<EventMessage<?>> generateFollowUpLetter() {
        return new GenericDeadLetter<>("sequenceIdentifier", generateEvent());
    }

    @Override
    @SuppressWarnings("raw")
    protected DeadLetter<EventMessage<?>> mapToQueueImplementation(DeadLetter<EventMessage<?>> deadLetter) {
        if (deadLetter instanceof MongoDeadLetter) {
            return deadLetter;
        } else if (deadLetter instanceof GenericDeadLetter) {
            return new MongoDeadLetter<>(0L, ((GenericDeadLetter<?>)deadLetter).getSequenceIdentifier().toString(), deadLetter.enqueuedAt(), deadLetter.lastTouched(), deadLetter.cause().orElse(null), deadLetter.diagnostics(), deadLetter.message());
        } else {
            throw new IllegalArgumentException("Can not map dead letter of type " + deadLetter.getClass().getName());
        }
    }

    /**
     * Only checking the epoch milli of timestamp as the rest is lost.
     * @param expected the expected {@link DeadLetter}
     * @param actual the actual {@link DeadLetter}
     */
    @Override
    protected void assertLetter(DeadLetter<? extends EventMessage<?>> expected, DeadLetter<? extends EventMessage<?>> actual) {
        Assertions.assertEquals(expected.message().getIdentifier(), actual.message().getIdentifier());
        Assertions.assertEquals(expected.message().getTimestamp().toEpochMilli(), actual.message().getTimestamp().toEpochMilli());
        Assertions.assertEquals(expected.message().getMetaData(), actual.message().getMetaData());
        Assertions.assertEquals(expected.message().getPayload(), actual.message().getPayload());
        Assertions.assertEquals(expected.message().getPayloadType(), actual.message().getPayloadType());
        Assertions.assertEquals(expected.cause(), actual.cause());
        Assertions.assertEquals(expected.enqueuedAt(), actual.enqueuedAt());
        Assertions.assertEquals(expected.lastTouched(), actual.lastTouched());
        Assertions.assertEquals(expected.diagnostics(), actual.diagnostics());
    }

    @Override
    protected DeadLetter<EventMessage<?>> generateRequeuedLetter(DeadLetter<EventMessage<?>> original,
                                                                 Instant lastTouched, Throwable requeueCause,
                                                                 MetaData diagnostics) {
        setAndGetTime(lastTouched);
        return original.withCause(requeueCause)
                       .withDiagnostics(diagnostics)
                       .markTouched();
    }

    @Override
    protected void setClock(Clock clock) {
        GenericDeadLetter.clock = clock;
    }

    @Test
    void cannotRequeueGenericDeadLetter() {
        SequencedDeadLetterQueue<EventMessage<?>> queue = buildTestSubject();
        DeadLetter<EventMessage<?>> letter = generateInitialLetter();
        assertThrows(WrongDeadLetterTypeException.class, () -> {
            queue.requeue(letter, d -> d);
        });
    }

    @Test
    void cannotEvictGenericDeadLetter() {
        SequencedDeadLetterQueue<EventMessage<?>> queue = buildTestSubject();
        DeadLetter<EventMessage<?>> letter = generateInitialLetter();
        assertThrows(WrongDeadLetterTypeException.class, () -> {
            queue.evict(letter);
        });
    }

    @Test
    void clearConvertorsShouldClearConvertors() {
        MongoTemplate mongoTemplate = MongoTemplateFactory.build(
                MONGO_CONTAINER.getHost(), MONGO_CONTAINER.getFirstMappedPort()
        );
        SequencedDeadLetterQueue<EventMessage<?>> testSubject = MongoSequencedDeadLetterQueue
                .builder()
                .processingGroup(PROCESSING_GROUP)
                .maxSequences(MAX_SEQUENCES_AND_SEQUENCE_SIZE)
                .maxSequenceSize(MAX_SEQUENCES_AND_SEQUENCE_SIZE)
                .serializer(TestSerializer.xStreamSerializer())
                .mongoTemplate(mongoTemplate)
                .clearConverters()
                .build();
        DeadLetter<EventMessage<?>> letter = generateInitialLetter();
        assertThrows(NoMongoConverterFoundException.class, () -> {
            testSubject.enqueue("sequenceIdentifier", letter);
        });
    }
}
