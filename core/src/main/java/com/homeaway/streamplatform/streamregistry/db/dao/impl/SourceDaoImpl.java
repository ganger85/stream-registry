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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

import com.google.common.base.Preconditions;
import com.homeaway.streamplatform.streamregistry.exceptions.StreamNotFoundException;
import lombok.extern.slf4j.Slf4j;

import com.homeaway.digitalplatform.streamregistry.AvroStreamKey;
import com.homeaway.digitalplatform.streamregistry.Sources;
import com.homeaway.streamplatform.streamregistry.db.dao.SourceDao;
import com.homeaway.streamplatform.streamregistry.exceptions.SourceNotFoundException;
import com.homeaway.streamplatform.streamregistry.model.Source;
import com.homeaway.streamplatform.streamregistry.streams.GenericEventStore;
import com.homeaway.streamplatform.streamregistry.streams.StreamProducer;


@Slf4j
public class SourceDaoImpl implements SourceDao {

    @NotNull
    private StreamProducer<AvroStreamKey, Sources> kafkaProducer;

    @NotNull
    private final GenericEventStore<Sources> sourceEventStore;

    public SourceDaoImpl(StreamProducer<AvroStreamKey,Sources> kafkaProducer, GenericEventStore<Sources> sourceEventStore) {
        this.kafkaProducer = kafkaProducer;
        this.sourceEventStore = sourceEventStore;
    }

    @Override
    public Optional<Source> get(String streamName, String sourceName) {

        AvroStreamKey avroKey = buildAvroKey(streamName);

        Optional<Sources> avroSources = sourceEventStore.get(avroKey);

        //noinspection OptionalGetWithoutIsPresent
        return avroSources.map(sources -> getModelSourceFromAvroSource(
                sources
                .getSources()
                .stream()
                .filter(stream -> stream.getSourceName().equalsIgnoreCase(sourceName))
                .findAny().get()));

    }

    @Override
    public Source insert(Source givenSource) {
        // create a new source for new stream

        // TODO Why use streamName as the key? Why not the sourceName as the key ? Then you don't need a stream-sources collection.
        AvroStreamKey streamKey = buildAvroKey(givenSource.getStreamName());

        Optional<Sources> avroSources = sourceEventStore.get(streamKey);

        Preconditions.checkState(!avroSources.isPresent(), "Insert for sourceName=" + givenSource.getSourceName() + " failed due to existing stream=" + givenSource.getStreamName() +". Update instead");

        List<com.homeaway.digitalplatform.streamregistry.Source> tempList = new ArrayList<>();
        com.homeaway.digitalplatform.streamregistry.Source newAvroSource = toAvro(givenSource);

        tempList.add(newAvroSource);
        Sources sources = toAvro(newAvroSource, tempList);
        kafkaProducer.log(streamKey, sources);
        return givenSource;
    }

    @Override
    public Source update(Source givenSource) {
        AvroStreamKey avroStreamKey = buildAvroKey(givenSource.getStreamName());

        Optional<Sources> avroSources = sourceEventStore.get(avroStreamKey);

        Preconditions.checkState(avroSources.isPresent(), "Update for sourceName=" + givenSource.getSourceName() + " failed due to missing stream=" + givenSource.getStreamName());

        // stream exists, sources exist

        // filter out old avroSource, if it exists and replace with newAvroSource
        com.homeaway.digitalplatform.streamregistry.Source newAvroSource = toAvro(givenSource);

        // filter out old avroSource (keep everything else)
        List<com.homeaway.digitalplatform.streamregistry.Source> avroSourcesList = avroSources
                .get()
                .getSources()
                .stream()
                .filter(sourceAvro -> !sourceAvro.getSourceName().equalsIgnoreCase(givenSource.getSourceName()))
                .collect(Collectors.toList());

        // add the new one
        avroSourcesList.add(newAvroSource);

        Sources newAvroSources = toAvro(newAvroSource, avroSourcesList);

        kafkaProducer.log(avroStreamKey, newAvroSources);
        return givenSource;
    }

    private Sources toAvro(com.homeaway.digitalplatform.streamregistry.Source updatedAvroSource, List<com.homeaway.digitalplatform.streamregistry.Source> avroSourcesWithoutTargetItem) {
        return Sources
                .newBuilder()
                .setStreamName(updatedAvroSource.getStreamName())
                .setSources(avroSourcesWithoutTargetItem)
                .build();
    }

    private com.homeaway.digitalplatform.streamregistry.Source toAvro(Source givenSource) {
        return com.homeaway.digitalplatform.streamregistry.Source.newBuilder()
                .setStreamName(givenSource.getStreamName())
                .setSourceName(givenSource.getSourceName())
                .setSourceType(givenSource.getSourceType())
                .setStreamSourceConfiguration(givenSource.getStreamSourceConfiguration())
                .build();
    }

    @Override
    public Source delete(String streamName, String sourceName) {
        AvroStreamKey streamKey = buildAvroKey(streamName);
        Optional<Sources> avroSources = sourceEventStore.get(streamKey);

        Optional<List<com.homeaway.digitalplatform.streamregistry.Source>> avroSourceList = avroSources.map(sources -> sources.getSources()
                .stream()
                .filter(source -> source.getSourceName().equalsIgnoreCase(sourceName))
                .collect(Collectors.toList()));

        // list is not present means no stream
        // empty list means source does not exist
        if (!avroSourceList.isPresent() || avroSourceList.get().isEmpty()) {
            // can't delete what you don't have
            throw new SourceNotFoundException(sourceName);
        }

        // create a list without givenSource and update the list in the producerStateStore
        List<com.homeaway.digitalplatform.streamregistry.Source> updatedSourcesWithoutGivenSource = avroSources.get()
                .getSources()
                .stream()
                .filter(source -> !source.getSourceName().equalsIgnoreCase(sourceName))
                .collect(Collectors.toList());

        avroSources.get().setSources(updatedSourcesWithoutGivenSource);
        kafkaProducer.log(streamKey, avroSources.get());
        return toSource(avroSourceList.get().get(0));
    }

    @Override
    public List<Source> getAll(String streamName) {

        AvroStreamKey avroStreamKey = buildAvroKey(streamName);
        Optional<Sources> sources =
                sourceEventStore.get(avroStreamKey);

        if (!sources.isPresent()) {
            throw new StreamNotFoundException(streamName);
        }

        return sources.get()
                .getSources()
                .stream()
                .map(this::getModelSourceFromAvroSource)
                .collect(Collectors.toList());

    }

    private Source getModelSourceFromAvroSource(
            com.homeaway.digitalplatform.streamregistry.Source avroSource) {
        return Source.builder()
                .streamName(avroSource.getStreamName())
                .sourceName(avroSource.getSourceName())
                .sourceType(avroSource.getSourceType())
                .streamSourceConfiguration(avroSource.getStreamSourceConfiguration())
                .build();
    }

    private Source toSource(com.homeaway.digitalplatform.streamregistry.Source avroSource) {
        return Source.builder()
                .streamName(avroSource.getStreamName())
                .sourceName(avroSource.getSourceName())
                .sourceType(avroSource.getSourceType())
                .streamSourceConfiguration(avroSource.getStreamSourceConfiguration())
                .build();
    }

    private AvroStreamKey buildAvroKey(String streamName) {
        return AvroStreamKey.newBuilder()
                .setStreamName(streamName)
                .build();
    }
}
