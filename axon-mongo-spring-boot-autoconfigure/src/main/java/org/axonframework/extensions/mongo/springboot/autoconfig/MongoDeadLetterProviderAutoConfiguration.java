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

package org.axonframework.extensions.mongo.springboot.autoconfig;

import org.axonframework.axonserver.connector.AxonServerConfiguration;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.config.EventProcessingModule;
import org.axonframework.extensions.mongo.MongoTemplate;
import org.axonframework.extensions.mongo.eventhandling.deadletter.MongoSequencedDeadLetterQueue;
import org.axonframework.extensions.mongo.springboot.AxonMongoProperties;
import org.axonframework.serialization.Serializer;
import org.axonframework.springboot.EventProcessorProperties;
import org.axonframework.springboot.autoconfig.EventProcessingAutoConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import java.util.Optional;

@AutoConfiguration
@EnableConfigurationProperties(value = {AxonMongoProperties.class, EventProcessorProperties.class})
@AutoConfigureAfter(value = {MongoAutoConfiguration.class, EventProcessingAutoConfiguration.class})
public class MongoDeadLetterProviderAutoConfiguration {

    private final AxonMongoProperties axonMongoProperties;
    private final EventProcessorProperties eventProcessorProperties;
    private final AxonServerConfiguration axonServerConfiguration;

    public MongoDeadLetterProviderAutoConfiguration(
            AxonMongoProperties axonMongoProperties,
            EventProcessorProperties eventProcessorProperties,
            AxonServerConfiguration axonServerConfiguration
    ) {
        this.axonMongoProperties = axonMongoProperties;
        this.eventProcessorProperties = eventProcessorProperties;
        this.axonServerConfiguration = axonServerConfiguration;
    }

    @Autowired
    void registerDeadLetterProvider(
            EventProcessingModule processingModule,
            MongoTemplate mongoTemplate,
            Serializer serializer,
            TransactionManager transactionManager
    ) {
        if (!axonMongoProperties.getEventHandling().isDlqEnabled()) {
            return;
        }
        processingModule.registerDeadLetterQueueProvider(
                processingGroup -> {
                    if (dlqEnabled(processingGroup) || persistentStreamDlqEnabled(processingGroup)) {
                        return configuration -> MongoSequencedDeadLetterQueue
                                .builder()
                                .processingGroup(processingGroup)
                                .mongoTemplate(mongoTemplate)
                                .transactionManager(transactionManager)
                                .serializer(serializer)
                                .build();
                    } else {
                        return null;
                    }
                });
    }

    private boolean dlqEnabled(String processingGroup) {
        return Optional.ofNullable(eventProcessorProperties.getProcessors().get(processingGroup))
                       .map(processorSettings -> processorSettings.getDlq().isEnabled())
                       .orElse(false);
    }

    private boolean persistentStreamDlqEnabled(String processingGroup) {
        if (axonServerConfiguration.getEventhandling() == null || axonServerConfiguration.getEventhandling().getPersistentStreamProcessors() == null) {
            return false;
        }

        return Optional.ofNullable (axonServerConfiguration.getEventhandling()
                                                           .getPersistentStreamProcessors().get(processingGroup))
                       .map(AxonServerConfiguration.PersistentStreamProcessorSettings::getDlq)
                       .map(AxonServerConfiguration.Dlq::isEnabled)
                       .orElse(false);
    }

}
