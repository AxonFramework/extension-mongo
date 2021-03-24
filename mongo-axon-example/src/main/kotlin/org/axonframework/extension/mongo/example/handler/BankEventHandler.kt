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

package org.axonframework.extension.mongo.example.handler

import mu.KLogging
import org.axonframework.eventhandling.EventHandler
import org.axonframework.eventhandling.EventMessage
import org.axonframework.extension.mongo.example.api.AccountBalanceQuery
import org.axonframework.extension.mongo.example.api.BankAccountCreatedEvent
import org.axonframework.extension.mongo.example.api.MoneyAddedEvent
import org.axonframework.queryhandling.QueryHandler
import org.springframework.stereotype.Component

@Component
class BankEventHandler {

    companion object : KLogging()

    private val accountAmount = HashMap<String, Long?>()

    /**
     * Account created event.
     */
    @EventHandler
    fun <T : EventMessage<Any>> on(event: BankAccountCreatedEvent) {
        logger.info { "account created: ${event.id}" }
        accountAmount[event.id] = 0
    }

    /**
     * Add money to the account.
     */
    @EventHandler
    fun <T : EventMessage<Any>> on(event: MoneyAddedEvent) {
        logger.info { "money added to: ${event.bankAccountId}" }
        // sum old amount plus new one
        val amount = accountAmount[event.bankAccountId]?.plus(event.amount)
        accountAmount[event.bankAccountId] = amount
    }

    /**
     * Check account balance.
     */
    @QueryHandler
    fun on(query: AccountBalanceQuery): Long? {
        logger.info { "received query ${query.bankAccountId}" }
        return accountAmount[query.bankAccountId]
    }

}
