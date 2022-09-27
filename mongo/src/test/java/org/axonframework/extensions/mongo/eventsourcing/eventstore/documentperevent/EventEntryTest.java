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

package org.axonframework.extensions.mongo.eventsourcing.eventstore.documentperevent;

import org.axonframework.eventhandling.GenericDomainEventMessage;
import org.axonframework.serialization.TestSerializer;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

class EventEntryTest {

    @Test
    void equalsHashcodeAndToStringShouldWorkCorrectly() {
        EventEntry original = testEntry();
        EventEntry copy = new EventEntry(original.asDocument(EventEntryConfiguration.getDefault()),
                                         EventEntryConfiguration.getDefault());
        assertEquals(original, copy);
        assertEquals(original.hashCode(), copy.hashCode());
        assertEquals(original.toString(), copy.toString());
    }

    static EventEntry testEntry() {
        return new EventEntry(
                new GenericDomainEventMessage<>(null, null, 0L, "foo"),
                TestSerializer.JACKSON.getSerializer()
        );
    }
}
