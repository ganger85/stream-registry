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
package com.homeaway.streamplatform.streamregistry.streams;

import java.util.Optional;
import java.util.Properties;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import com.homeaway.digitalplatform.streamregistry.AvroStreamKey;

/**
 * This class provides some boiler-plate functionality for an event store.
 * Current functionality supported is lookup-one (get) and lookup-all (getAll).
 * @param <T> The type of the record that will be used to store in the event store.
 */
@Slf4j
public class GenericEventStore<T> {

    @Getter
    private final KafkaStreams streams;

    @Getter
    private final Properties streamProperties;

    @Getter
    private final String topicName;

    @Getter
    private final String stateStoreName;

    @Getter
    private ReadOnlyKeyValueStore<AvroStreamKey, T> view;

    public GenericEventStore(Properties streamProperties, String topicName, String stateStoreName) {
        this(streamProperties, topicName, stateStoreName, null);
    }

    public GenericEventStore(Properties streamProperties, String topicName, String stateStoreName,
                             KStreamsProcessorListener testListener) {

        this.streamProperties = streamProperties;
        this.topicName = topicName;
        this.stateStoreName = stateStoreName;

        KStreamBuilder kStreamBuilder = new KStreamBuilder();

        // use global table to cache all keys across all partitions
        kStreamBuilder.globalTable(topicName, stateStoreName);

        streams = new KafkaStreams(kStreamBuilder, streamProperties);

        // Improve build times by notifying test listener that we are running
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING) {
                if (testListener != null) {
                    testListener.stateStoreInitialized();
                }
            }
        });
        streams.setUncaughtExceptionHandler((t, e) -> log.error("KafkaStreams job failed", e));

        // TODO: Should not start as part of constructor... this should be part of initialization somewhere. Remove.
        start();
    }

    public void start() {
        streams.start();
        log.info("Stream Registry KStreams started.");
        log.info("Stream Registry State Store Name: {}", stateStoreName);
        view = streams.store(stateStoreName, QueryableStoreTypes.keyValueStore());
    }

    public void stop() {
        streams.close();
        log.info("KStreams closed");
    }

    public Optional<T> get(AvroStreamKey key) {
        return Optional.ofNullable(view.get(key));
    }

    public KeyValueIterator<AvroStreamKey, T> getAllValues() {
        return view.all();
    }
}
