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


import com.homeaway.digitalplatform.streamregistry.AvroStreamKey;
import com.homeaway.digitalplatform.streamregistry.Sources;
import com.homeaway.streamplatform.streamregistry.db.dao.SourceDao;
import com.homeaway.streamplatform.streamregistry.db.dao.impl.SourceDaoImpl;
import com.homeaway.streamplatform.streamregistry.model.Source;
import com.homeaway.streamplatform.streamregistry.streams.ManagedKafkaProducer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;
import java.util.stream.Collectors;

import static org.powermock.api.mockito.PowerMockito.whenNew;

// Unit tests
@RunWith(PowerMockRunner.class)
public class SourceResourceTest {

    public static Map<String, String> configMap;

    public static Map<AvroStreamKey, Sources> keyValueStore;


    @BeforeClass
    public static void setUp() {

        Map<String, String> tempConfigurationMap = new HashMap<>();

        tempConfigurationMap.put("foo", "bar");

        configMap = Collections.unmodifiableMap(tempConfigurationMap);


        Map<AvroStreamKey, Sources> keyValueStoreMap = new HashMap<>();
        keyValueStoreMap.put(AvroStreamKey.newBuilder()
                        .setStreamName("streamA").build(),
                Sources.newBuilder()
                        .setStreamName("streamA")
                        .setSources(Arrays.asList(com.homeaway.digitalplatform.streamregistry.Source.newBuilder()
                                .setStreamName("streamA")
                                .setSourceName("sourceA")
                                .setSourceType("kinesis")
                                .setStreamSourceConfiguration(configMap)
                                .build()))
                        .build());

        keyValueStoreMap.put(AvroStreamKey.newBuilder()
                        .setStreamName("streamB").build(),
                Sources.newBuilder()
                        .setStreamName("streamB")
                        .setSources(Arrays.asList(com.homeaway.digitalplatform.streamregistry.Source.newBuilder()
                                .setStreamName("streamB")
                                .setSourceName("sourceB")
                                .setSourceType("mysql")
                                .setStreamSourceConfiguration(configMap)
                                .build()))
                        .build());

        keyValueStoreMap.put(AvroStreamKey.newBuilder()
                        .setStreamName("streamC").build(),
                Sources.newBuilder()
                        .setStreamName("streamC")
                        .setSources(Arrays.asList(com.homeaway.digitalplatform.streamregistry.Source.newBuilder()
                                .setStreamName("streamC")
                                .setSourceName("sourceC")
                                .setSourceType("sqlserver")
                                .setStreamSourceConfiguration(configMap)
                                .build()))
                        .build());


        keyValueStore = Collections.unmodifiableMap(keyValueStoreMap);
    }

    @Mock
    private ManagedKafkaProducer kafkaProducer;


    @Test
    public void testGetSourcesByStreamName() throws Exception {

        ReadOnlyKeyValueStoreStub localKeyValueStore = new ReadOnlyKeyValueStoreStub(keyValueStore);
        whenNew(ManagedKafkaProducer.class).withAnyArguments().thenReturn(kafkaProducer);

        SourceDao sourceDao = new SourceDaoImpl(kafkaProducer, localKeyValueStore);
        SourceResource resource = new SourceResource(sourceDao);

        List<Source> sources = (List<Source>) resource.getAllSourcesByStream("streamA").getEntity();

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
    public void testGetSourceByStreamNameAndSourceName() throws Exception {

        ReadOnlyKeyValueStoreStub localKeyValueStore = new ReadOnlyKeyValueStoreStub(keyValueStore);
        whenNew(ManagedKafkaProducer.class).withAnyArguments().thenReturn(kafkaProducer);

        SourceDao sourceDao = new SourceDaoImpl(kafkaProducer, localKeyValueStore);
        SourceResource resource = new SourceResource(sourceDao);

        Source sourceB = (Source) resource.getSource("streamB", "sourceB").getEntity();

        Assert.assertEquals("sourceB", sourceB.getSourceName());
        Assert.assertEquals("mysql", sourceB.getSourceType());
        Assert.assertEquals(configMap.get("foo"), sourceB.getStreamSourceConfiguration().get("foo"));
    }

//    @Test
//    public void upsertSource() throws Exception {
//        ReadOnlyKeyValueStoreStub localKeyValueStore = new ReadOnlyKeyValueStoreStub(keyValueStore);
//        whenNew(ManagedKafkaProducer.class).withAnyArguments().thenReturn(kafkaProducer);
//
//        SourceDao sourceDao = new SourceDaoImpl(kafkaProducer, localKeyValueStore);
//        SourceResource resource = new SourceResource(sourceDao);
//
//        Source sourceD = (Source) resource.upsertSource("streamC", "sourceD").getEntity();
//
//    }


    class ReadOnlyKeyValueStoreStub<AvroStreamKey, Sources> implements ReadOnlyKeyValueStore<AvroStreamKey, Sources> {

        private Map<AvroStreamKey, Sources> sources;

        public ReadOnlyKeyValueStoreStub(Map<AvroStreamKey, Sources> sources) {
            this.sources = sources;
        }

        @Override
        public Sources get(AvroStreamKey key) {
            return sources.get(key);
        }

        public void put(AvroStreamKey key, Sources values) {
            sources.put(key, values);
        }

        @Override
        public KeyValueIterator range(Object from, Object to) {
            // Not Implemented
            return null;
        }

        @Override
        public KeyValueIterator<AvroStreamKey, Sources> all() {
            List<KeyValue> keyValues = sources.entrySet()
                    .stream()
                    .map(e -> new KeyValue<AvroStreamKey, Sources>(e.getKey(), e.getValue()))
                    .collect(Collectors.toList());

            return new KeyValueIteratorStub<AvroStreamKey, Sources>(keyValues.iterator());

        }

        @Override
        public long approximateNumEntries() {
            return 0;
        }
    }

    class KeyValueIteratorStub<AvroStreamKey, Sources> implements KeyValueIterator {

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
