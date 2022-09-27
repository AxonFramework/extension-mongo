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

import org.axonframework.extensions.mongo.eventsourcing.eventstore.documentperevent.EventEntryConfiguration;
import org.axonframework.serialization.TestSerializer;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class DeadLetterEntryTest {

    @Test
    void equalsHashcodeAndToStringShouldWorkCorrectly() {
        DeadLetterEntry original = testEntry();
        DeadLetterEntry copy = new DeadLetterEntry(original.asDocument());
        assertEquals(original, copy);
        assertEquals(original.hashCode(), copy.hashCode());
        assertEquals(original.toString(), copy.toString());
    }

    static DeadLetterEntry testEntry() {
        return new DeadLetterEntry(
                "processingGroup",
                "seqIdentifier",
                0L,
                DeadLetterEventEntryTest.testEntry().asDocument(EventEntryConfiguration.getDefault()),
                Instant.now().minus(Duration.ofMinutes(3L)),
                Instant.now(),
                null,
                null,
                TestSerializer.JACKSON.getSerializer()
        );
    }
}
