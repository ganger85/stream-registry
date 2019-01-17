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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.collect.Multimap;
import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.annotation.Timed;

import io.swagger.annotations.*;

import com.homeaway.streamplatform.streamregistry.db.dao.ClusterDao;
import com.homeaway.streamplatform.streamregistry.model.Cluster;
import com.homeaway.streamplatform.streamregistry.model.Stream;

@Api(value = "Stream-registry API", description = "Stream Registry API, a centralized governance tool for managing streams.")
@Path("/v0/clusters")
@Produces(MediaType.APPLICATION_JSON)
@Slf4j
public class ClusterResource {

    private final ClusterDao clusterDao;

    public ClusterResource(ClusterDao clusterDao) {
        this.clusterDao = clusterDao;
    }

    @GET
    @ApiOperation(
        value = "Get all supported clusters",
        tags = "clusters",
        response = Collection.class)
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Returns all clusters", response = List.class) })
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    public Response getClusters() {
        Multimap<String, Cluster> clusters = clusterDao.getClusters();
        return Response.status(200).entity(clusters).build();
    }

    @GET
    @Path("/{clusterName}")
    @ApiOperation(
            value = "Get cluster",
            notes = "Returns a list of clusters for the clusterName",
            tags = "clusters",
            response = Cluster.class)
    @ApiResponses(value = { @ApiResponse(code = 200, message = "successful operation", response = Stream.class),
            @ApiResponse(code = 500, message = "Error Occurred while getting data"),
            @ApiResponse(code = 404, message = "Stream not found") })
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    public Response getCluster(@ApiParam(value = "Stream name", required = true) @PathParam("clusterName") String clusterName) {
        Collection<Cluster> clusters = clusterDao.getCluster(clusterName);
//        try {
//            if (!stream.isPresent()) {
//                return ResourceUtils.streamNotFound(streamName);
//            }
            return Response.ok().entity(clusters).build();
//        } catch (Exception e) {
//            String message = "Error occurred while getting data from Stream Registry for cluster '" + clusterName + "'";
//            log.error(message, e);
//            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
//                    .entity(new ErrorMessage(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), message))
//                    .build();
//        }
    }

}
