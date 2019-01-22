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
package com.homeaway.streamplatform.streamregistry.resource;


import java.util.*;
import java.util.stream.Collectors;

import lombok.Getter;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import com.homeaway.digitalplatform.streamregistry.AvroStreamKey;
import com.homeaway.digitalplatform.streamregistry.Sources;
import com.homeaway.streamplatform.streamregistry.db.dao.SourceDao;
import com.homeaway.streamplatform.streamregistry.db.dao.impl.SourceDaoImpl;
import com.homeaway.streamplatform.streamregistry.model.Source;
import com.homeaway.streamplatform.streamregistry.streams.StreamProducer;

// Unit tests
@RunWith(PowerMockRunner.class)
public class SourceResourceTest {

    private static Map<String, String> configMap;

    private static Map<AvroStreamKey, Sources> keyValueStoreMap;

    @BeforeClass
    public static void setUp() {

        Map<String, String> tempConfigurationMap = new HashMap<>();

        tempConfigurationMap.put("foo", "bar");

        configMap = Collections.unmodifiableMap(tempConfigurationMap);


        keyValueStoreMap = new HashMap<>();

        List<com.homeaway.digitalplatform.streamregistry.Source> sourcesA = new ArrayList<>();
                sourcesA.add(com.homeaway.digitalplatform.streamregistry.Source.newBuilder()
                .setStreamName("streamA")
                .setSourceName("sourceA")
                .setSourceType("kinesis")
                .setStreamSourceConfiguration(configMap)
                .build());

        keyValueStoreMap.put(AvroStreamKey.newBuilder()
                        .setStreamName("streamA").build(),
                Sources.newBuilder()
                        .setStreamName("streamA")
                        .setSources(sourcesA)
                        .build());

        List<com.homeaway.digitalplatform.streamregistry.Source> sourcesB
                = new ArrayList<>();
        sourcesB.add(com.homeaway.digitalplatform.streamregistry.Source.newBuilder()
                                .setStreamName("streamB")
                                .setSourceName("sourceB")
                                .setSourceType("mysql")
                                .setStreamSourceConfiguration(configMap)
                                .build());
        keyValueStoreMap.put(AvroStreamKey.newBuilder()
                        .setStreamName("streamB").build(),
                Sources.newBuilder()
                        .setStreamName("streamC")
                        .setSources(sourcesB)
                        .build());

        List<com.homeaway.digitalplatform.streamregistry.Source> sourcesC
                = new ArrayList<>();
        sourcesC.add(com.homeaway.digitalplatform.streamregistry.Source.newBuilder()
                .setStreamName("streamC")
                .setSourceName("sourceC")
                .setSourceType("kinesis")
                .setStreamSourceConfiguration(configMap)
                .build());

        keyValueStoreMap.put(AvroStreamKey.newBuilder()
                        .setStreamName("streamC").build(),
                Sources.newBuilder()
                        .setStreamName("streamC")
                        .setSources(sourcesC)
                        .build());
    }

    @Test
    public void testGetSourcesByStreamName() {

        ReadOnlyKeyValueStoreStub<AvroStreamKey, Sources> localKeyValueStore = new ReadOnlyKeyValueStoreStub<>(
                Collections.unmodifiableMap(keyValueStoreMap));

        StreamProducer kafkaProducer = new StreamProducerStub(Collections.unmodifiableMap(keyValueStoreMap));
        SourceDao sourceDao = new SourceDaoImpl(kafkaProducer, localKeyValueStore);

        List<Source> sources = sourceDao.getAll("streamA");

        Source sourceA = sources.stream()
                .filter(source -> source.getSourceName().equalsIgnoreCase("sourceA"))
                .findAny().get();

        Assert.assertEquals(1, sources.size());
        Assert.assertEquals("sourceA", sourceA.getSourceName());
        Assert.assertEquals("kinesis", sourceA.getSourceType());
        Assert.assertEquals(configMap.get("foo"), sourceA.getStreamSourceConfiguration().get("foo"));
    }


    @SuppressWarnings("unchecked")
    @Test
    public void testGetSourceByStreamNameAndSourceName() {
        ReadOnlyKeyValueStoreStub<AvroStreamKey, Sources> localKeyValueStore = new ReadOnlyKeyValueStoreStub<>(
                Collections.unmodifiableMap(keyValueStoreMap));

        StreamProducer kafkaProducer = new StreamProducerStub(Collections.unmodifiableMap(keyValueStoreMap));
        SourceDao sourceDao = new SourceDaoImpl(kafkaProducer, localKeyValueStore);
        Source sourceB = sourceDao.get("streamB", "sourceB").get();

        Assert.assertEquals("sourceB", sourceB.getSourceName());
        Assert.assertEquals("mysql", sourceB.getSourceType());
        Assert.assertEquals(configMap.get("foo"), sourceB.getStreamSourceConfiguration().get("foo"));
    }

    @Test
    public void testUpsertSourceForExistingStream() {

        Map<AvroStreamKey, Sources> avroStreamKeySourcesMap = new HashMap<>();
        avroStreamKeySourcesMap.putAll(keyValueStoreMap);


        ReadOnlyKeyValueStoreStub<AvroStreamKey, Sources> localKeyValueStore = new ReadOnlyKeyValueStoreStub<>(
                avroStreamKeySourcesMap);

        StreamProducer kafkaProducer = new StreamProducerStub(avroStreamKeySourcesMap);
        SourceDao sourceDao = new SourceDaoImpl(kafkaProducer, localKeyValueStore);

        Source source = Source.builder()
                .streamName("streamA")
                .sourceName("sourceB")
                .sourceType("mysql")
                .streamSourceConfiguration(configMap)
                .build();

        sourceDao.upsert(source);

        Assert.assertEquals(2, ((StreamProducerStub<AvroStreamKey, Sources>) kafkaProducer).getMap().get(
                AvroStreamKey.newBuilder()
                        .setStreamName("streamA")
                        .build()).getSources().size());

    }

    @Test
    public void testUpsertForNewStream() {

        Map<AvroStreamKey, Sources> avroStreamKeySourcesMap = new HashMap<>();
        avroStreamKeySourcesMap.putAll(keyValueStoreMap);


        ReadOnlyKeyValueStoreStub<AvroStreamKey, Sources> localKeyValueStore = new ReadOnlyKeyValueStoreStub<>(
                avroStreamKeySourcesMap);

        StreamProducer kafkaProducer = new StreamProducerStub(avroStreamKeySourcesMap);
        SourceDao sourceDao = new SourceDaoImpl(kafkaProducer, localKeyValueStore);

        Source source = Source.builder()
                .streamName("streamD")
                .sourceName("sourceA")
                .sourceType("kinesis")
                .streamSourceConfiguration(configMap)
                .build();

        sourceDao.upsert(source);

        AvroStreamKey streamD = AvroStreamKey
                .newBuilder()
                .setStreamName("streamD")
                .build();

        Assert.assertNotNull(localKeyValueStore.get(streamD));
        Assert.assertEquals(4, localKeyValueStore.streams.size());
    }



    @Test
    public void testDeleteSourceByStreamName() {

        Map<AvroStreamKey, Sources> avroStreamKeySourcesMap = new HashMap<>();
        avroStreamKeySourcesMap.putAll(keyValueStoreMap);


        ReadOnlyKeyValueStoreStub<AvroStreamKey, Sources> localKeyValueStore = new ReadOnlyKeyValueStoreStub<>(
                avroStreamKeySourcesMap);

        StreamProducer kafkaProducer = new StreamProducerStub(avroStreamKeySourcesMap);
        SourceDao sourceDao = new SourceDaoImpl(kafkaProducer, localKeyValueStore);

        sourceDao.delete("streamA", "sourceA");

        List<Source> sources = sourceDao.getAll("streamA");

        Assert.assertEquals(0, sources.size());
    }



    private class StreamProducerStub<K, V> implements StreamProducer<K, V> {

        @Getter
        private final Map<K, V> map;


        public StreamProducerStub(Map<K, V> map) {
            this.map = map;
        }


        @Override
        public void log(K key, V value) {
            map.put(key, value);
        }
    }


    private class ReadOnlyKeyValueStoreStub<AvroStreamKey, Sources> implements ReadOnlyKeyValueStore<AvroStreamKey, Sources> {

        @Getter
        private Map<AvroStreamKey, Sources> streams;

        public ReadOnlyKeyValueStoreStub(Map<AvroStreamKey, Sources> streams) {
            this.streams = streams;
        }

        @Override
        public Sources get(AvroStreamKey key) {
            return streams.get(key);
        }

        public void put(AvroStreamKey key, Sources values) {
            streams.put(key, values);
        }

        @Override
        public KeyValueIterator range(Object from, Object to) {
            // Not Implemented
            return null;
        }

        @Override
        public KeyValueIterator<AvroStreamKey, Sources> all() {
            List<KeyValue> keyValues = streams.entrySet()
                    .stream()
                    .map(e -> new KeyValue<>(e.getKey(), e.getValue()))
                    .collect(Collectors.toList());

            return new KeyValueIteratorStub<AvroStreamKey, Sources>(keyValues.iterator());

        }

        @Override
        public long approximateNumEntries() {
            return 0;
        }
    }

    class KeyValueIteratorStub<K, V> implements KeyValueIterator {

        private Iterator<KeyValue> keyValues;


        public KeyValueIteratorStub(Iterator<KeyValue> keyValues) {
            this.keyValues = keyValues;
        }

        @Override
        public void close() {
            // nothing to do
        }

        @Override
        public Object peekNextKey() {
            return null;
        }

        @Override
        public boolean hasNext() {
            return keyValues.hasNext();
        }

        @Override
        public Object next() {
            return keyValues.next();
        }
    }

}
