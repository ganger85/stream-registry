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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.homeaway.digitalplatform.streamregistry.ClusterKey;
import com.homeaway.digitalplatform.streamregistry.ClusterValue;
import com.homeaway.streamplatform.streamregistry.db.dao.ClusterDao;
import com.homeaway.streamplatform.streamregistry.model.Cluster;
import com.homeaway.streamplatform.streamregistry.provider.InfraManager;

import javax.validation.constraints.NotNull;
import java.util.*;
import java.util.stream.Collectors;

public class ClusterDaoImpl implements ClusterDao {

    @NotNull
    private final InfraManager infraManager;

    public ClusterDaoImpl(InfraManager infraManager) {
        this.infraManager = infraManager;
    }

    private static Multimap<String, Cluster> clustersByName =  ArrayListMultimap.create();

    @Override
    public Map<String, List<Cluster>> getClusters() {

        Map<ClusterKey, ClusterValue>  clusterByName = infraManager.getAllClusters()
                .entrySet()
                .stream()
                .filter((e -> e.getKey().getEnv().equalsIgnoreCase("dev")))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));

        Iterator it = clusterByName.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<ClusterKey, ClusterValue> pair = (Map.Entry)it.next();
            clustersByName.put(pair.getKey().getHint(), getCluster(pair.getKey(), pair.getValue()));
            it.remove(); // avoids a ConcurrentModificationException
        }

        return null;

    }

    @Override
    public Collection<Cluster> getCluster(String clusterName) {
        return clustersByName.get(clusterName);
    }


    private static Cluster getCluster(ClusterKey key, ClusterValue value) {

        Map<String, String> allProperties = new HashMap<>();
        allProperties.putAll(value.getClusterProperties());
        allProperties.put("env", key.getEnv());
        allProperties.put("vpc", key.getVpc());

        return Cluster.builder()
                .clusterName(key.getHint())
                .clusterType(key.getType())
                .status("ONLINE")
                .clusterConfig(Collections.unmodifiableMap(allProperties))
                .build();
    }
}
