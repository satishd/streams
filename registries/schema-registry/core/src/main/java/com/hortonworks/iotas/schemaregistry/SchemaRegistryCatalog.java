/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.iotas.schemaregistry;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import com.hortonworks.iotas.common.util.WSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.Collection;

import static com.hortonworks.iotas.common.catalog.CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND;
import static com.hortonworks.iotas.common.catalog.CatalogResponse.ResponseMessage.EXCEPTION;
import static com.hortonworks.iotas.common.catalog.CatalogResponse.ResponseMessage.SUCCESS;
import static javax.ws.rs.core.Response.Status.CREATED;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;

/**
 *
 */
@Path("/api/v1/catalog/schemaregistry")
@Produces(MediaType.APPLICATION_JSON)
public class SchemaRegistryCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryCatalog.class);

    private final ISchemaRegistry schemaRegistry;

    public SchemaRegistryCatalog(ISchemaRegistry schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    @GET
    @Path("/schemas")
    @Timed
    public Response listSchemas(@Context UriInfo uriInfo) {
        try {
            return WSUtils.respond(OK, SUCCESS, schemaRegistry.list());
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
    }

    @POST
    @Path("/schemas")
    @Timed
    public Response addSchema(SchemaInfo schemaInfo) {
        try {
            SchemaInfo addedSchemaInfo = schemaRegistry.add(schemaInfo);
            return WSUtils.respond(CREATED, SUCCESS, addedSchemaInfo);
        } catch (Exception ex) {
            LOG.error("Error encountered while adding schema", ex);
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
    }

    @GET
    @Path("/schemas/{id}")
    @Timed
    public Response getSchema(@PathParam("id") Long schemaId) {
        Preconditions.checkNotNull(schemaId, "schemaID must not be null");
        try {
            SchemaInfo schemaInfo = schemaRegistry.get(schemaId);
            if (schemaInfo != null) {
                return WSUtils.respond(OK, SUCCESS, schemaInfo);
            }
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, schemaId.toString());
    }

    @DELETE
    @Path("/schemas/{id}")
    @Timed
    public Response removeSchemaInfo(@PathParam("id") Long schemaId) {
        try {
            SchemaInfo removedParser = schemaRegistry.remove(schemaId);
            return WSUtils.respond(OK, SUCCESS, removedParser);
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
    }

    @GET
    @Path("/types/{type}/{name}/schemas")
    @Timed
    public Response listSchemas(@PathParam("name") String name, @PathParam("type") String type) {
        try {
            Collection<SchemaInfo> schemaInfos = schemaRegistry.get(type, name);
            if (schemaInfos != null) {
                return WSUtils.respond(OK, SUCCESS, schemaInfos);
            }
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, name);
    }

    @GET
    @Path("/types/{type}/schemas/{name}/latest")
    @Timed
    public Response getLatestSchema(@PathParam("name") String name, @PathParam("type") String type) {
        try {
            SchemaInfo schemaInfo = schemaRegistry.getLatest(type, name);
            if (schemaInfo != null) {
                return WSUtils.respond(OK, SUCCESS, schemaInfo);
            }
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, name);
    }

    @GET
    @Path("/types/{type}/schemas/{name}/{version}")
    @Timed
    public Response getSchema(@PathParam("name") String name, @PathParam("type") String type, @PathParam("version") Integer version) {
        try {
            SchemaInfo schemaInfo = schemaRegistry.get(type, name, version);
            if (schemaInfo != null) {
                return WSUtils.respond(OK, SUCCESS, schemaInfo);
            }
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, name);
    }
}
