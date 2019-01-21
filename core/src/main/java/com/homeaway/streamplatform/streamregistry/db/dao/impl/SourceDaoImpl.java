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

import com.homeaway.digitalplatform.streamregistry.AvroStreamKey;
import com.homeaway.digitalplatform.streamregistry.Sources;
import com.homeaway.streamplatform.streamregistry.db.dao.SourceDao;
import com.homeaway.streamplatform.streamregistry.exceptions.SourceNotFoundException;
import com.homeaway.streamplatform.streamregistry.model.Source;
import com.homeaway.streamplatform.streamregistry.streams.ManagedKafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;


@Slf4j
public class SourceDaoImpl implements SourceDao {

    @NotNull
    private ManagedKafkaProducer kafkaProducer;

    @NotNull
    private final ReadOnlyKeyValueStore internalStore;

    public SourceDaoImpl(ManagedKafkaProducer kafkaProducer, ReadOnlyKeyValueStore<AvroStreamKey, Sources> internalStore) {
        this.kafkaProducer = kafkaProducer;
        this.internalStore = internalStore;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Optional<Source> upsert(Source givenSource) {


        AvroStreamKey avroStreamKey = getAvroKeyFromString(
                givenSource.getStreamName());

        Optional<Sources> avroSources = Optional.ofNullable((Sources) internalStore.get(avroStreamKey));

        Optional<com.homeaway.digitalplatform.streamregistry.Source> avroSourceOptional = avroSources
                .get()
                .getSources()
                .stream()
                .filter((sourceAvro) -> sourceAvro.getSourceName()
                        .equalsIgnoreCase(givenSource.getSourceName()))
                .findAny();

        if (avroSourceOptional.isPresent()) {
            // update source
            com.homeaway.digitalplatform.streamregistry.Source updatedAvroSource = avroSourceOptional.get();
            updatedAvroSource.setSourceName(givenSource.getStreamName());
            updatedAvroSource.setSourceName(givenSource.getSourceName());
            updatedAvroSource.setSourceType(givenSource.getSourceType());
            updatedAvroSource.setStreamSourceConfiguration(givenSource.getStreamSourceConfiguration());

            List<com.homeaway.digitalplatform.streamregistry.Source> avroSourcesWithoutTargetItem = avroSources
                    .get()
                    .getSources()
                    .stream()
                    .filter((sourceAvro) -> !sourceAvro.getSourceName()
                            .equalsIgnoreCase(givenSource.getSourceName())).
                            collect(Collectors.toList());
            avroSourcesWithoutTargetItem.add(updatedAvroSource);
            kafkaProducer.log(avroStreamKey, avroSourcesWithoutTargetItem);
        } else {
            // create a new source
            com.homeaway.digitalplatform.streamregistry.Source newSourceAvro = com.homeaway.digitalplatform.streamregistry.Source.newBuilder()
                    .setSourceName(givenSource.getSourceName())
                    .setSourceType(givenSource.getSourceType())
                    .setStreamSourceConfiguration(givenSource.getStreamSourceConfiguration())
                    .build();
            avroSources.get().getSources().add(newSourceAvro);
            kafkaProducer.log(avroStreamKey, avroSources);
        }

        return Optional.of(givenSource);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Optional<Source> get(String streamName, String sourceName) {

        AvroStreamKey avroKey = getAvroKeyFromString(streamName);

        Sources avroSources
                = (Sources) internalStore
                .get(avroKey);

        return Optional.of(getModelSourceFromAvroSource(
                avroSources.getSources()
                        .stream()
                        .filter(stream -> stream.getSourceName().equalsIgnoreCase(sourceName))
                        .findAny().get()));
    }


    @Override
    public void delete(String streamName, String sourceName) {
        AvroStreamKey stream = getAvroKeyFromString(streamName);
        Optional<com.homeaway.digitalplatform.streamregistry.Sources> avroSources =
                Optional.ofNullable((Sources) internalStore.get(stream));

        boolean sourceNameMatch = avroSources.get()
                .getSources()
                .stream()
                .anyMatch(source -> source.getSourceName().equalsIgnoreCase(sourceName));

        if (sourceNameMatch) {
            // create a list without givenSource and update the list in the producerStateStore
            List<com.homeaway.digitalplatform.streamregistry.Source> updatedSourcesWithoutGivenSource = avroSources.get()
                    .getSources()
                    .stream()
                    .filter(source -> !source.getSourceName().equalsIgnoreCase(sourceName))
                    .collect(Collectors.toList());

            avroSources.get().setSources(updatedSourcesWithoutGivenSource);
            kafkaProducer.log(stream, avroSources);
        } else {
            // can't delete what you don't have
            throw new SourceNotFoundException(sourceName);
        }

    }

    @Override
    public Optional<List<Source>> getAll(String streamName) {

        AvroStreamKey avroStreamKey = getAvroKeyFromString(streamName);
        Optional<Sources> sources =
                Optional.ofNullable((Sources) internalStore.get(avroStreamKey));

        return Optional.of(sources.get()
                .getSources()
                .stream()
                .map(avroStream -> getModelSourceFromAvroSource(avroStream))
                .collect(Collectors.toList()));

    }

    private static Source getModelSourceFromAvroSource(
            com.homeaway.digitalplatform.streamregistry.Source avroSource) {
        return Source.builder()
                .streamName(avroSource.getStreamName())
                .sourceName(avroSource.getSourceName())
                .sourceType(avroSource.getSourceType())
                .streamSourceConfiguration(avroSource.getStreamSourceConfiguration())
                .build();
    }

    private static Optional<com.homeaway.digitalplatform.streamregistry.Source> getAvroSourceFromModelSource(
            Source modelSource) {
        return Optional.of(com.homeaway.digitalplatform.streamregistry.Source.newBuilder()
                .setStreamName(modelSource.getStreamName())
                .setSourceName(modelSource.getSourceName())
                .setSourceType(modelSource.getSourceType())
                .setStreamSourceConfiguration(modelSource.getStreamSourceConfiguration())
                .build()
        );
    }

    private static AvroStreamKey getAvroKeyFromString(String streamName) {
        return AvroStreamKey.newBuilder()
                .setStreamName(streamName)
                .build();
    }


}
