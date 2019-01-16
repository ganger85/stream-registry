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

import com.homeaway.digitalplatform.streamregistry.ClusterKey;
import com.homeaway.digitalplatform.streamregistry.ClusterValue;
import com.homeaway.streamplatform.streamregistry.db.dao.ClusterDao;
import com.homeaway.streamplatform.streamregistry.db.dao.impl.ClusterDaoImpl;
import com.homeaway.streamplatform.streamregistry.model.Cluster;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.util.*;

public class StreamSourceResourceIT extends BaseResourceIT {
    @SuppressWarnings("unchecked")
    @Test
    public void testGetClusters(){

        ClusterDao clusterDao = new ClusterDaoImpl(infraManager);
        ClusterResource resource = new ClusterResource(clusterDao);
        Response response = resource.getClusters();
        Collection<Cluster> clusters = (Collection<Cluster>) response.getEntity();

        System.out.println(clusters);
//
//        Assert.assertEquals(new HashSet<>(Arrays.asList(US_EAST_REGION)),
//                hints.stream().filter((hint) -> hint.getHint().equalsIgnoreCase(AbstractDao.PRIMARY_HINT)).findFirst().get().getVpcs());
//        Assert.assertEquals(new HashSet<>(Arrays.asList(US_EAST_REGION)),
//                hints.stream().filter((hint) -> hint.getHint().equalsIgnoreCase(BaseResourceIT.OTHER_HINT)).findFirst().get().getVpcs());
//        Assert.assertEquals(new HashSet<>(Arrays.asList(US_WEST_REGION)),
//                hints.stream().filter((hint) -> hint.getHint().equalsIgnoreCase(BaseResourceIT.SOME_HINT)).findFirst().get().getVpcs());

    }




    private static Map<ClusterKey, ClusterValue> dummyInfraMap() {


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

        return Collections.unmodifiableMap(infraManagerMap);
    }

}
