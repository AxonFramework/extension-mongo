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

package org.axonframework.extensions.mongo.serialization;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import org.axonframework.serialization.Revision;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.xml.CompactDriver;
import org.junit.jupiter.api.*;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link DBObjectXStreamSerializer}.
 *
 * @author Allard Buijze
 */
class DBObjectXStreamSerializerTest {

    private static final String SPECIAL__CHAR__STRING = "Special chars: '\"&;\n\\<>/\n\t";

    private DBObjectXStreamSerializer testSubject;

    @BeforeEach
    void setUp() {
        XStream xStream = new XStream(new CompactDriver());
        xStream.registerConverter(new SomeEnumEnumSetConverter());
        this.testSubject = DBObjectXStreamSerializer.builder()
                                                    .xStream(xStream)
                                                    .build();
    }

    @Test
    void testSerializeAndDeserializeDomainEventWithListOfObjects() {
        List<Object> objectList = new ArrayList<>();
        objectList.add("a");
        objectList.add(1L);
        objectList.add("b");
        SerializedObject<String> serializedEvent = testSubject.serialize(new SecondTestEvent("eventName", objectList),
                                                                         String.class);

        Object actualResult = testSubject.deserialize(serializedEvent);
        assertTrue(actualResult instanceof SecondTestEvent);
        SecondTestEvent actualEvent = (SecondTestEvent) actualResult;
        assertEquals(objectList, actualEvent.getStrings());
    }

    // Test for issue AXON-141 - BSONNode - marshalling EnumSet problem
    @Test
    void testSerializeEnumSet() {
        SerializedObject<String> serialized = testSubject.serialize(new TestEventWithEnumSet("testing123"),
                                                                    String.class);

        TestEventWithEnumSet actual = testSubject.deserialize(serialized);
        assertEquals("testing123", actual.getName());
        assertEquals(EnumSet.of(TestEventWithEnumSet.SomeEnum.FIRST, TestEventWithEnumSet.SomeEnum.SECOND),
                     actual.enumSet);
    }

    @Test
    void testSerializeAndDeserializeDomainEvent() {
        SerializedObject<byte[]> serializedEvent = testSubject.serialize(new TestEvent("Henk"), byte[].class);
        Object actualResult = testSubject.deserialize(serializedEvent);
        assertTrue(actualResult instanceof TestEvent);
        TestEvent actualEvent = (TestEvent) actualResult;
        assertEquals("Henk", actualEvent.getName());
    }

    @Test
    void testPackageAlias() {
        testSubject.addPackageAlias("test", "org.axonframework.extensions.mongo.serialization");
        testSubject.addPackageAlias("axon", "org.axonframework");

        SerializedObject<String> serialized = testSubject.serialize(new StubDomainEvent(), String.class);
        String asString = serialized.getData();
        assertFalse(asString.contains("org"), "Package name found in:" + asString);
        StubDomainEvent deserialized = testSubject.deserialize(serialized);
        assertEquals(StubDomainEvent.class, deserialized.getClass());
        assertTrue(asString.contains("test"));
    }

    @Test
    void testAlias() {
        testSubject.addAlias("stub", StubDomainEvent.class);

        SerializedObject<byte[]> serialized = testSubject.serialize(new StubDomainEvent(), byte[].class);
        String asString = new String(serialized.getData(), StandardCharsets.UTF_8);
        assertFalse(asString.contains("org.axonframework.domain"));
        assertTrue(asString.contains("\"stub"));
        StubDomainEvent deserialized = testSubject.deserialize(serialized);
        assertEquals(StubDomainEvent.class, deserialized.getClass());
    }

    @Test
    void testFieldAlias() {
        testSubject.addFieldAlias("relevantPeriod", TestEvent.class, "period");

        SerializedObject<byte[]> serialized = testSubject.serialize(new TestEvent("hello"), byte[].class);
        String asString = new String(serialized.getData(), StandardCharsets.UTF_8);
        assertFalse(asString.contains("period"));
        assertTrue(asString.contains("\"relevantPeriod"));
        TestEvent deserialized = testSubject.deserialize(serialized);
        assertNotNull(deserialized);
    }

    @Test
    void testRevisionNumber_FromAnnotation() {
        SerializedObject<byte[]> serialized = testSubject.serialize(new RevisionSpecifiedEvent(), byte[].class);
        assertNotNull(serialized);
        assertEquals("2", serialized.getType().getRevision());
        assertEquals(RevisionSpecifiedEvent.class.getName(), serialized.getType().getName());
    }

    @SuppressWarnings("Duplicates")
    @Test
    void testSerializedTypeUsesClassAlias() {
        testSubject.addAlias("rse", RevisionSpecifiedEvent.class);
        SerializedObject<byte[]> serialized = testSubject.serialize(new RevisionSpecifiedEvent(), byte[].class);
        assertNotNull(serialized);
        assertEquals("2", serialized.getType().getRevision());
        assertEquals("rse", serialized.getType().getName());
    }

    /**
     * Tests the scenario as described in <a href="http://code.google.com/p/axonframework/issues/detail?id=150">issue
     * #150</a>.
     */
    @Test
    void testSerializeWithSpecialCharacters_WithoutUpcasters() {
        SerializedObject<byte[]> serialized = testSubject.serialize(new TestEvent(SPECIAL__CHAR__STRING), byte[].class);
        TestEvent deserialized = testSubject.deserialize(serialized);
        assertEquals(SPECIAL__CHAR__STRING, deserialized.getName());
    }

    @Revision("2")
    private static class RevisionSpecifiedEvent {

    }

    private static class SecondTestEvent extends TestEvent {

        private static final long serialVersionUID = -4918755275652698472L;

        private final List<Object> objects;

        public SecondTestEvent(String name, List<Object> objects) {
            super(name);
            this.objects = new ArrayList<>(objects);
        }

        public List<Object> getStrings() {
            return objects;
        }
    }


    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private static class TestEvent implements Serializable {

        private static final long serialVersionUID = 1L;
        private final String name;
        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
        private final List<String> someListOfString;
        private final LocalDate date;
        private final Instant dateTime;
        private final Period period;

        public TestEvent(String name) {
            this.name = name;
            this.date = LocalDate.now();
            this.dateTime = Instant.now();
            this.period = Period.ofDays(100);
            this.someListOfString = new ArrayList<>();
            someListOfString.add("First");
            someListOfString.add("Second");
        }

        public String getName() {
            return name;
        }
    }

    private static class TestEventWithEnumSet extends TestEvent {

        private static final long serialVersionUID = -9117549356246820817L;

        private final Set<SomeEnum> enumSet;

        public TestEventWithEnumSet(String name) {
            super(name);
            enumSet = EnumSet.of(SomeEnum.FIRST, SomeEnum.SECOND);
        }


        private enum SomeEnum {
            FIRST,
            SECOND,
            @SuppressWarnings("unused") THIRD
        }
    }

    private static class StubDomainEvent implements Serializable {

        private static final long serialVersionUID = 3209191845532896831L;
    }

    public class SomeEnumEnumSetConverter implements Converter {

        public boolean canConvert(Class type) {
            //actually only if it's a TestEventWithEnumSet set, but fine for the test.
            return type != null && EnumSet.class.isAssignableFrom(type);
        }

        public void marshal(Object source, HierarchicalStreamWriter writer, MarshallingContext context) {
            EnumSet set = (EnumSet) source;
            writer.setValue(this.joinEnumValues(set));
        }

        private String joinEnumValues(EnumSet set) {
            boolean seenFirst = false;
            StringBuffer result = new StringBuffer();

            Enum value;
            for (Iterator iterator = set.iterator(); iterator.hasNext(); result.append(value.name())) {
                value = (Enum) iterator.next();
                if (seenFirst) {
                    result.append(',');
                } else {
                    seenFirst = true;
                }
            }

            return result.toString();
        }

        public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context) {
            EnumSet set = EnumSet.noneOf(TestEventWithEnumSet.SomeEnum.class);
            String[] enumValues = reader.getValue().split(",");
            for (int i = 0; i < enumValues.length; ++i) {
                String enumValue = enumValues[i];
                if (enumValue.length() > 0) {
                    set.add(TestEventWithEnumSet.SomeEnum.valueOf(enumValue));
                }
            }
            return set;
        }
    }
}
