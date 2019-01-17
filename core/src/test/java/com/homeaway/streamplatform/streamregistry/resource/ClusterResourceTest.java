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

import javax.ws.rs.core.Response;

import com.google.common.collect.Multimap;

import org.junit.Assert;
import org.junit.Test;

import com.homeaway.streamplatform.streamregistry.db.dao.ClusterDao;
import com.homeaway.streamplatform.streamregistry.db.dao.impl.ClusterDaoImpl;
import com.homeaway.streamplatform.streamregistry.model.Cluster;

public class ClusterResourceTest {

    @SuppressWarnings("unchecked")
    @Test
    public void testGetClusters(){

        ClusterDao clusterDao = new ClusterDaoImpl("dev", Initializers.infraManagerImplStub);
        ClusterResource resource = new ClusterResource(clusterDao);
        Response response = resource.getClusters();
        Multimap<String, Cluster> clusters = (Multimap<String, Cluster>) response.getEntity();

        Assert.assertEquals(Initializers.infraManagerImplStub.getAllClusters().size(), clusters.size());
    }

}
