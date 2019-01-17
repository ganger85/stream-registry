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

import com.google.common.collect.Multimap;
import com.homeaway.digitalplatform.streamregistry.ClusterKey;
import com.homeaway.digitalplatform.streamregistry.ClusterValue;
import com.homeaway.streamplatform.streamregistry.db.dao.ClusterDao;
import com.homeaway.streamplatform.streamregistry.db.dao.impl.ClusterDaoImpl;
import com.homeaway.streamplatform.streamregistry.model.Cluster;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class StreamSourceResourceTest {

    private static InfraManagerImplStub infraManager;

    static {

        Map<ClusterKey, ClusterValue> infraManagerMap = new HashMap();

        infraManagerMap.put(ClusterKey.newBuilder()
                        .setEnv("dev")
                        .setHint("primary")
                        .setType("consumer")
                        .setVpc("something").build(),
                ClusterValue.newBuilder()
                        .setClusterProperties(new HashMap<>()).build());

        infraManagerMap.put(ClusterKey.newBuilder()
                        .setEnv("dev")
                        .setHint("primary")
                        .setType("producer")
                        .setVpc("something").build(),
                ClusterValue.newBuilder()
                        .setClusterProperties(new HashMap<>()).build());

        infraManagerMap.put(ClusterKey.newBuilder()
                        .setEnv("dev")
                        .setHint("secondary")
                        .setType("producer")
                        .setVpc("something").build(),
                ClusterValue.newBuilder()
                        .setClusterProperties(new HashMap<>()).build());


        infraManager = new InfraManagerImplStub();

        Collections.unmodifiableMap(infraManagerMap)
                .forEach((k,v) -> infraManager.addCluster(k,v));

    }


    @SuppressWarnings("unchecked")
    @Test
    public void testGetClusters(){

        ClusterDao clusterDao = new ClusterDaoImpl("dev", infraManager);
        ClusterResource resource = new ClusterResource(clusterDao);
        Response response = resource.getClusters();
        Multimap<String, Cluster> clusters = (Multimap<String, Cluster>) response.getEntity();

        Assert.assertEquals(infraManager.getAllClusters().size(), clusters.size());
    }

}
