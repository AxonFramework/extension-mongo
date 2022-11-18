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

import org.junit.jupiter.api.*;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SpringMongoTransactionManagerTest {

    @Test
    void whenUsedItWellCallTheCorrectMethodsOnTheManager() {
        MongoTransactionManager manager = mock(MongoTransactionManager.class);
        TransactionStatus status = mock(TransactionStatus.class);
        when(manager.getTransaction(any(TransactionDefinition.class))).thenReturn(status);
        SpringMongoTransactionManager testSubject = new SpringMongoTransactionManager(manager);
        AtomicBoolean check = new AtomicBoolean(false);

        testSubject.executeInTransaction(() -> check.set(true));

        assertTrue(check.get());
        verify(status).isNewTransaction();
        verifyNoMoreInteractions(status);
        verify(manager).getTransaction(any(TransactionDefinition.class));
        verifyNoMoreInteractions(manager);
    }
}
