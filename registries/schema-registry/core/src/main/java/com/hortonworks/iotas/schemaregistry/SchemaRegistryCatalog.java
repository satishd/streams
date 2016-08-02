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
import com.hortonworks.iotas.schemaregistry.client.VersionedSchema;
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
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;

import java.io.InputStream;

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
            return WSUtils.respond(OK, SUCCESS, schemaRegistry.listAll());
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
    }

    @POST
    @Path("/schemas")
    @Timed
    public Response addSchema(SchemaDto schemaDto) {
        try {
            SchemaDto addedSchemaDto = SchemaDto.addSchemaDto(schemaRegistry, schemaDto);
            return WSUtils.respond(CREATED, SUCCESS, new SchemaKey(addedSchemaDto.getId(), addedSchemaDto.getVersion()));
        } catch (Exception ex) {
            LOG.error("Error encountered while adding schema", ex);
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
    }

    @POST
    @Path("/schemas/{id}")
    @Timed
    public Response addSchemaInfo(@PathParam("id") Long schemaMetadataId, VersionedSchema versionedSchema) {
        try {
            SchemaInfoStorable schemaInfoStorable = new SchemaInfoStorable();
            schemaInfoStorable.setSchemaMetadataId(schemaMetadataId);
            schemaInfoStorable.setSchemaText(versionedSchema.getSchemaText());
            schemaInfoStorable.setDescription(versionedSchema.getDescription());

            SchemaInfoStorable addedSchemaInfoStorable = schemaRegistry.addSchemaInfo(schemaInfoStorable);
            return WSUtils.respond(CREATED, SUCCESS, new SchemaKey(addedSchemaInfoStorable.getSchemaMetadataId(), addedSchemaInfoStorable.getVersion()));
        } catch (Exception ex) {
            LOG.error("Error encountered while adding schema", ex);
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
    }

    @GET
    @Path("/schemas/{id}")
    @Timed
    public Response getSchema(@PathParam("id") Long schemaMetadataId) {
        Preconditions.checkNotNull(schemaMetadataId, "id must not be null");
        try {
            SchemaInfoStorable schemaInfoStorable = schemaRegistry.getSchemaInfo(schemaMetadataId);
            if (schemaInfoStorable != null) {
                SchemaMetadataStorable schemaMetadataStorable = schemaRegistry.getSchemaMetadata(schemaInfoStorable.getSchemaMetadataId());
                SchemaDto schemaDto = new SchemaDto(schemaMetadataStorable, schemaInfoStorable);
                return WSUtils.respond(OK, SUCCESS, schemaDto);
            }
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, schemaMetadataId.toString());
    }

    @DELETE
    @Path("/schemas/{id}")
    @Timed
    public Response removeSchemaInfo(@PathParam("id") Long schemaMetadataId) {
        try {
            SchemaInfoStorable removedParser = schemaRegistry.removeSchemaInfo(schemaMetadataId);
            return WSUtils.respond(OK, SUCCESS, removedParser);
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
    }

    @GET
    @Path("/schemas/{id}/versions/latest")
    @Timed
    public Response getLatestSchema(@PathParam("id") Long schemaMetadataId) {
        try {
            SchemaMetadataStorable schemaMetadataStorable = schemaRegistry.getSchemaMetadata(schemaMetadataId);
            SchemaInfoStorable schemaInfoStorable = schemaRegistry.getLatestSchemaInfo(schemaMetadataId);
            if (schemaInfoStorable != null) {
                return WSUtils.respond(OK, SUCCESS, new SchemaDto(schemaMetadataStorable, schemaInfoStorable));
            }
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, schemaMetadataId.toString());
    }

    @GET
    @Path("/schemas/{id}/versions/{version}")
    @Timed
    public Response getLatestSchema(@PathParam("id") Long schemaMetadataId, @PathParam("version") Integer version) {
        try {
            SchemaMetadataStorable schemaMetadataStorable = schemaRegistry.getSchemaMetadata(schemaMetadataId);
            SchemaInfoStorable schemaInfoStorable = schemaRegistry.getSchemaInfo(schemaMetadataId, version);
            if (schemaInfoStorable != null) {
                return WSUtils.respond(OK, SUCCESS, new SchemaDto(schemaMetadataStorable, schemaInfoStorable));
            }
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, schemaMetadataId.toString());
    }

    @POST
    @Path("/schemas/{id}/compatible/versions/{version}")
    @Timed
    public Response isCompatible(String schema,
                                 @PathParam("id") Long schemaMetadataId,
                                 @PathParam("version") Integer version) {
        try {
            boolean compatible = schemaRegistry.isCompatible(schemaMetadataId, version, schema);
            return WSUtils.respond(OK, SUCCESS, compatible);
        } catch(SchemaNotFoundException e) {
            return WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, schemaMetadataId.toString());
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
    }

    @GET
    @Path("/schemas/{id}/serializers/")
    @Timed
    public Response getSerializers(@PathParam("id") Long schemaMetadataId) {
        try {
            Iterable<SerDesInfoStorable> serializers = schemaRegistry.getSchemaSerializers(schemaMetadataId);

            if (serializers != null) {
                return WSUtils.respond(OK, SUCCESS, serializers);
            }
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, schemaMetadataId.toString());
    }


    @GET
    @Produces({"application/octet-stream", "application/json"})
    @Path("/schemas/{id}/serializers/{serializerId}")
    @Timed
    public Response downloadSerializer(@PathParam("id") Long schemaMetadataId, @PathParam("serializerId") Long serializerId) {
        try {
            InputStream inputStream = schemaRegistry.downloadSerializer(schemaMetadataId, serializerId);
            if (inputStream != null) {
                StreamingOutput streamOutput = WSUtils.wrapWithStreamingOutput(inputStream);
                return Response.ok(streamOutput).build();
            }
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, schemaMetadataId.toString());
    }


    @POST
    @Path("/serializers")
    @Timed
    public Response addSerializer(SerializerInfo schemaSerDesInfo) {
        try {
            Long serializerId = schemaRegistry.addSerializer(schemaSerDesInfo);
            return WSUtils.respond(OK, SUCCESS, serializerId);
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
    }

    @GET
    @Path("/serializers/{id}")
    @Timed
    public Response getSerializer(@PathParam("id") Long serializerId) {
        try {
            SerDesInfo serializerInfo = schemaRegistry.getSerializer(serializerId);
            return WSUtils.respond(OK, SUCCESS, serializerInfo);
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
    }
}
