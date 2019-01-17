/*
 *  Copyright (c) 2018 Expedia Group.
 *  * All rights reserved.  http://www.homeaway.com
 *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.homeaway.streamplatform.streamregistry.db.dao.impl;

import com.homeaway.digitalplatform.streamregistry.Source;
import com.homeaway.streamplatform.streamregistry.db.dao.SourceDao;
import com.homeaway.streamplatform.streamregistry.provider.InfraManager;
import com.homeaway.streamplatform.streamregistry.streams.ManagedKStreams;
import com.homeaway.streamplatform.streamregistry.streams.ManagedKafkaProducer;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import java.util.List;
import java.util.Optional;

public class SourceDaoImpl implements SourceDao {

    @NotNull
    private ManagedKafkaProducer kafkaProducer;

    @NotNull
    private final ManagedKStreams kStreams;

    @NotNull
    private final InfraManager infraManager;

    public SourceDaoImpl(ManagedKafkaProducer kafkaProducer, ManagedKStreams kStreams, InfraManager infraManager) {
        this.kafkaProducer = kafkaProducer;
        this.kStreams = kStreams;
        this.infraManager = infraManager;
    }

    @Override
    public Optional<Source> update(String streamName, String sourceName, String region) {
        return Optional.empty();
    }

    @Override
    public Optional<Source> get(String streamName, String sourceName) {
        return Optional.empty();
    }

//        private void deleteProducer(String streamName, String producerName) {
//        Optional<AvroStream> avroStream = getAvroStreamKeyValue(streamName).getValue();
//
//        if (avroStream.isPresent()) {
//            final List<com.homeaway.digitalplatform.streamregistry.Producer> withProducer = avroStream.get().getProducers();
//
//            // Obtains producer list size before  remove consumer
//            final int producerInitialSize = withProducer.size();
//
//            // Obtains filtered producer list not containing the consumer we want to remove
//            List<com.homeaway.digitalplatform.streamregistry.Producer> withoutProducer = withProducer
//                    .stream()
//                    .filter(producer -> !StreamRegistryUtils.hasActorNamed(producerName, producer::getActor))
//                    .collect(Collectors.toList());
//
//            // Update stream's producer list
//            avroStream.get().setProducers(withoutProducer);
//
//            // If filtered producer list size is less than initial size stream will be updated
//            if (avroStream.get().getProducers().size() < producerInitialSize)
//                updateAvroStream(avroStream.get());
//            else
//                throw new ProducerNotFoundException(producerName);
//        } else {
//            throw new StreamNotFoundException(streamName);
//        }
//    }

    @Override
    public void delete(String streamName, String sourceName) {
        Optional<Source> avroStream = getAvroStreamKeyValue(streamName).getValue();

        if (avroStream.isPresent()) {
            final List<com.homeaway.digitalplatform.streamregistry.Producer> withProducer = avroStream.get().getProducers();

            // Obtains producer list size before  remove consumer
            final int producerInitialSize = withProducer.size();

            // Obtains filtered producer list not containing the consumer we want to remove
            List<com.homeaway.digitalplatform.streamregistry.Producer> withoutProducer = withProducer
                    .stream()
                    .filter(producer -> !StreamRegistryUtils.hasActorNamed(producerName, producer::getActor))
                    .collect(Collectors.toList());

            // Update stream's producer list
            avroStream.get().setProducers(withoutProducer);

            // If filtered producer list size is less than initial size stream will be updated
            if (avroStream.get().getProducers().size() < producerInitialSize)
                updateAvroStream(avroStream.get());
            else
                throw new ProducerNotFoundException(producerName);
        } else {
            throw new StreamNotFoundException(streamName);
        }
    }

    @Override
    public List<Source> getAllSources(String streamName) {
        return (List<Source>) kStreams.getAllStreams();
    }
}
