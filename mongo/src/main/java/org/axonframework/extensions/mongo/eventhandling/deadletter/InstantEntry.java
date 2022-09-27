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

import org.bson.Document;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/**
 * Mongo entity for an {@link Instant} such that no precision is lost.
 *
 * @author Gerard Klijs
 * @since 4.7.0
 */
public class InstantEntry {

    static final String SECONDS_KEY = "seconds";
    static final String NANOSECONDS_KEY = "nanoseconds";
    static final Set<String> KEY_SET = Stream
            .of(SECONDS_KEY, NANOSECONDS_KEY)
            .collect(Collectors.toCollection(HashSet::new));

    private final Instant instant;

    /**
     * The {@link Instant} value represented by this entry.
     *
     * @return the {@link Instant} value.
     */
    public Instant getInstant() {
        return instant;
    }

    /**
     * Initializes an {@link InstantEntry} entry using a DBObject containing the Mongo Document. Throws
     * {@link NotAnInstantDocumentException} if the provided document does not have only {@code seconds} and
     * {@code nanoseconds} as keys.
     *
     * @param dbInstantEntry the {@link Document} representation of the entity.
     */
    public InstantEntry(@Nonnull Document dbInstantEntry) {
        Set<String> entryKeys = dbInstantEntry.keySet();
        if (!entryKeys.equals(KEY_SET)) {
            throw new NotAnInstantDocumentException(
                    String.format("Actual keys on the document are %s and not %s.", entryKeys, KEY_SET)
            );
        }
        long seconds = dbInstantEntry.getLong(SECONDS_KEY);
        int nanoseconds = dbInstantEntry.getInteger(NANOSECONDS_KEY);
        this.instant = Instant.ofEpochSecond(seconds, nanoseconds);
    }

    /**
     * Initializes a {@link InstantEntry} entry using an {@link Instant}.
     *
     * @param instant the {@link Instant} representation of the entity.
     */
    public InstantEntry(Instant instant) {
        this.instant = instant;
    }

    /**
     * Returns the instant as a mongo Document.
     *
     * @return Document representing the instant.
     */
    public Document asDocument() {
        return new Document()
                .append(SECONDS_KEY, instant.getEpochSecond())
                .append(NANOSECONDS_KEY, instant.getNano());
    }
}
