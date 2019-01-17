/* Copyright (c) 2018 Expedia Group.
 * All rights reserved.  http://www.homeaway.com

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *      http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.homeaway.streamplatform.streamregistry.db.dao.impl;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

import lombok.extern.slf4j.Slf4j;

import com.homeaway.digitalplatform.streamregistry.AvroStreamKey;
import com.homeaway.digitalplatform.streamregistry.Sources;
import com.homeaway.streamplatform.streamregistry.db.dao.SourceDao;
import com.homeaway.streamplatform.streamregistry.model.Source;
import com.homeaway.streamplatform.streamregistry.provider.InfraManager;
import com.homeaway.streamplatform.streamregistry.streams.ManagedKStreams;
import com.homeaway.streamplatform.streamregistry.streams.ManagedKafkaProducer;


@Slf4j
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
    public Optional<Source> upsert(Source source) {
        AvroStreamKey avroStreamKey = AvroStreamKey.newBuilder()
                .setStreamName(source.getStreamName())
                .build();

        Optional<Sources> avroSources = kStreams.getAvroStreamForKey(avroStreamKey);

        Optional<com.homeaway.digitalplatform.streamregistry.Source> avroSourceOptional = avroSources
            .get()
            .getSources()
            .stream()
            .filter((sourceAvro) -> sourceAvro.getSourceName()
                    .equalsIgnoreCase(source.getSourceName()))
            .findFirst();

        if (avroSourceOptional.isPresent()) {
            // update source
            com.homeaway.digitalplatform.streamregistry.Source updatedSource = avroSourceOptional.get();
            updatedSource.setSourceName(source.getSourceName());
            updatedSource.setSourceType(source.getSourceType());
            updatedSource.setStreamSourceConfiguration(source.getStreamSourceConfiguration());

            List<com.homeaway.digitalplatform.streamregistry.Source> avroSourcesWithoutTargetItem = avroSources
                    .get()
                    .getSources()
                    .stream()
                    .filter((sourceAvro) -> !sourceAvro.getSourceName()
                            .equalsIgnoreCase(source.getSourceName())).
                            collect(Collectors.toList());
            avroSourcesWithoutTargetItem.add(updatedSource);
            kafkaProducer.log(avroStreamKey, avroSourcesWithoutTargetItem);
        } else {
            // create a new source
            com.homeaway.digitalplatform.streamregistry.Source newSourceAvro = com.homeaway.digitalplatform.streamregistry.Source.newBuilder()
                    .setSourceName(source.getSourceName())
                    .setSourceType(source.getSourceType())
                    .setStreamSourceConfiguration(source.getStreamSourceConfiguration())
                    .build();
            avroSources.get().getSources().add(newSourceAvro);
            kafkaProducer.log(avroStreamKey, avroSources);
        }

        return Optional.of(source);
    }

    @Override
    public Optional<Source> get(String streamName, String sourceName) {
        return Optional.empty();
    }

    @Override
    public void delete(String streamName, String sourceName) {
        // TODO
    }

    @Override
    public List<Source> getAllSources(String streamName) {
        return (List<Source>) kStreams.getAllStreams();
    }
}
