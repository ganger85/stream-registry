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

import java.util.List;
import java.util.Optional;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.annotation.Timed;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import com.homeaway.streamplatform.streamregistry.db.dao.SourceDao;
import com.homeaway.streamplatform.streamregistry.exceptions.SourceNotFoundException;
import com.homeaway.streamplatform.streamregistry.model.Source;
import com.homeaway.streamplatform.streamregistry.utils.ResourceUtils;

@Slf4j
public class SourceResource {

    private final SourceDao sourceDao;

    @SuppressWarnings("WeakerAccess")
    public SourceResource(SourceDao sourceDao) {
        this.sourceDao = sourceDao;
    }

    @PUT
    @ApiOperation(
        value = "Register source",
        notes = "Register a source with a stream",
        tags = "sources",
        response = Source.class)
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Returns Source information", response = Source.class),
        @ApiResponse(code = 404, message = "Stream not found"),
        @ApiResponse(code = 412, message = "Unsupported region"),
        @ApiResponse(code = 500, message = "Error Occurred while getting data") })
    @Path("/{sourceName}")
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    public Response upsertSource(@ApiParam(value = "name of the stream", required = true) @PathParam("streamName") String streamName,
        @ApiParam(value = "name of the source", required = true) @PathParam("sourceName") String sourceName) {
        try {
            Optional<Source> source = sourceDao.get(streamName, sourceName);

            if (!source.isPresent()) {
                return ResourceUtils.notFound(sourceName);
            }

            Optional<Source> sourceOptional = sourceDao.upsert(source.get());
            if (sourceOptional.isPresent()) {
                log.info(" Source upserted, sourceName: " + sourceName);
                return Response.ok().entity(source.get()).build();
            }
        } catch (IllegalArgumentException e) {
            log.error("Input is wrong.", e);
            throw new BadRequestException("Input Validation failed. Message=" + e.getMessage(), e);
        } catch (Exception e) {
            log.error("Error occurred while getting data from Stream Registry.", e);
            throw new InternalServerErrorException("Error occurred while updating the Producer in Stream Registry", e);
        }
        return null;
    }

    @GET
    @Path("/{sourceName}")
    @ApiOperation(
        value = "Get source",
        notes = "Get a source associated with the stream",
        tags = "sources",
        response = Source.class)
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Returns Source information", response = Source.class),
        @ApiResponse(code = 404, message = "Stream or Source not found"),
        @ApiResponse(code = 500, message = "Error Occurred while getting data") })
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    public Response getSource(@ApiParam(value = "name of the stream", required = true) @PathParam("streamName") String streamName,
        @ApiParam(value = "name of the source", required = true) @PathParam("sourceName") String sourceName) {

        Optional<Source> source = sourceDao.get(streamName, sourceName);

        try {
            if (!source.isPresent()) {
                return ResourceUtils.notFound(sourceName);
            }
            Optional<Source> sourceResponse = sourceDao.get(streamName, sourceName);
            if (!sourceResponse.isPresent()) {
                log.warn("Source Not Found: " + sourceName);
                return ResourceUtils.notFound("Producer not found " + sourceName);
            }
            return Response.ok().entity(sourceResponse.get()).build();
        } catch (Exception e) {
            log.error("Error occurred while getting data from Stream Registry", e);
            throw new InternalServerErrorException("Error occurred while getting data from Stream Registry", e);
        }
    }

    @DELETE
    @ApiOperation(
        value = "De-register source",
        notes = "De-Registers a source from a stream",
        tags = "sources")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Source successfully deleted"),
        @ApiResponse(code = 404, message = "Stream or source not found"),
        @ApiResponse(code = 500, message = "Error Occurred while getting data") })
    @Path("/{sourceName}")
    @Timed
    public Response deleteProducer(@ApiParam(value = "name of the stream", required = true) @PathParam("streamName") String streamName,
        @ApiParam(value = "name of the producer", required = true) @PathParam("sourceName") String sourceName) {
        try {
            sourceDao.delete(streamName, sourceName);
        } catch (SourceNotFoundException pe) {
            log.warn("Source not found ", sourceName);
            return ResourceUtils.notFound("Source not found " + sourceName);
        } catch (Exception e) {
            log.error("Error occurred while getting data from Stream Registry.", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
        return Response
            .ok()
            .type("text/plain")
            .entity("Source deleted " + sourceName)
            .build();
    }

    @GET
    @Path("/")
    @ApiOperation(
        value = "Get all sources for a given Stream",
        notes = "Gets a list of sources for a given stream",
        tags = "sources",
        response = Source.class)
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Returns Source information", response = Source.class),
        @ApiResponse(code = 500, message = "Error Occurred while getting data"),
        @ApiResponse(code = 404, message = "Stream not found") })
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    public Response getAllSources(
        @ApiParam(value = "Stream Name corresponding to the source", required = true) @PathParam("streamName") String streamName) {
        try {
            Optional<List<Source>> sources = sourceDao.getAll(streamName);
            if (!sources.isPresent()) {
                return ResourceUtils.streamNotFound(streamName);
            }
            return Response.ok().entity(sources).build();
        } catch (Exception e) {
            log.error("Error occurred while getting data from Stream Registry.", e);
            throw new InternalServerErrorException("Error occurred while getting data from Stream Registry");
        }
    }

}
