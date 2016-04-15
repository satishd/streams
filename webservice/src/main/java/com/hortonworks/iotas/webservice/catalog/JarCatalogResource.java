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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.hortonworks.iotas.webservice.catalog;

import com.codahale.metrics.annotation.Timed;
import com.google.common.io.ByteStreams;
import com.hortonworks.iotas.catalog.Jar;
import com.hortonworks.iotas.parser.Parser;
import com.hortonworks.iotas.service.CatalogService;
import com.hortonworks.iotas.util.JarStorage;
import com.hortonworks.iotas.util.ProxyUtil;
import com.hortonworks.iotas.webservice.IotasConfiguration;
import com.hortonworks.iotas.webservice.util.WSUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.UUID;

import static com.hortonworks.iotas.catalog.CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND;
import static com.hortonworks.iotas.catalog.CatalogResponse.ResponseMessage.EXCEPTION;
import static com.hortonworks.iotas.catalog.CatalogResponse.ResponseMessage.SUCCESS;
import static javax.ws.rs.core.Response.Status.CREATED;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;

/**
 * Catalog resource for jar resources.
 */
@Path("/api/v1/catalog")
@Produces(MediaType.APPLICATION_JSON)
public class JarCatalogResource {
    private static final Logger log = LoggerFactory.getLogger(JarCatalogResource.class);

    private final CatalogService catalogService;
    private final IotasConfiguration configuration;
    private final JarStorage jarStorage;
    private ProxyUtil<Parser> parserProxyUtil;

    public JarCatalogResource(CatalogService catalogService, IotasConfiguration configuration, JarStorage jarStorage) {
        this.catalogService = catalogService;
        this.configuration = configuration;
        this.jarStorage = jarStorage;
    }

    @GET
    @Path("/jars")
    @Timed
    public Response listJars(@Context UriInfo uriInfo) {
        try {
            Collection<Jar> jars = null;
            MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
            if (params == null || params.isEmpty()) {
                jars = catalogService.listJars();
            } else {
                jars = catalogService.listJars(WSUtils.buildQueryParameters(params));
            }
            return WSUtils.respond(OK, SUCCESS, jars);
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
    }

    /**
     * Adds given resource to the configured jar-storage and adds an entry in entity storage.
     *
     * Below example describes how a jar file can be added along with metadata
     * <blockquote><pre>
     * curl -X POST -i -F file=@user-lib.jar -F "jar={\"id\":1234, \"name\":\"jar-1\",\"version\":1};type=application/json"  http://localhost:8080/api/v1/catalog/jars
     *
     * HTTP/1.1 100 Continue
     *
     * HTTP/1.1 201 Created
     * Date: Fri, 15 Apr 2016 10:36:33 GMT
     * Content-Type: application/json
     * Content-Length: 239
     *
     * {"responseCode":1000,"responseMessage":"Success","entity":{"id":1234,"name":"jar-1","className":null,"storagePath":"/tmp/test-hdfs/jar-1-ea41fe3a-12f9-45d4-ae24-818d570b8963.jar","version":1,"timestamp":1460716593157,"auxiliaryInfo":null}}
     * </pre></blockquote>
     *
     * @param inputStream actual file content as {@link InputStream}.
     * @param contentDispositionHeader {@link FormDataContentDisposition} instance of the received file
     * @param jar configuration of the jar resource {@link Jar}
     * @return
     */
    @Timed
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Path("/jars")
    public Response addJar(@FormDataParam("file") final InputStream inputStream,
                              @FormDataParam("file") final FormDataContentDisposition contentDispositionHeader,
                              @FormDataParam("jar") final Jar jar) {

        try {
            log.info("Received jar: [{}]", jar);
            String jarStoragePath = (StringUtils.isBlank(jar.getName()) ? "jar" : jar.getName()) + "-" + UUID.randomUUID().toString() + ".jar";
            String uploadedPath = jarStorage.uploadJar(inputStream, jarStoragePath);
            inputStream.close();
            log.info("Received Jar file is uploaded to [{}]", uploadedPath);

            jar.setTimestamp(System.currentTimeMillis());
            jar.setStoragePath(uploadedPath);
            catalogService.addJar(jar);

            return WSUtils.respond(CREATED, SUCCESS, jar);
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
    }

    /**
     *
     * @param inputStream
     * @param contentDispositionHeader
     * @param jar
     */
    @Timed
    @PUT
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Path("/jars")
    public Response updateJar(@FormDataParam("file") final InputStream inputStream,
                           @FormDataParam("file") final FormDataContentDisposition contentDispositionHeader,
                           @FormDataParam("jar") final Jar jar) {
        try {
            log.info("Received jar: [{}]", jar);
            // todo updation

            return WSUtils.respond(CREATED, SUCCESS, jar);
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
    }


    @GET
    @Path("/jars/{id}")
    @Timed
    public Response getJar(@PathParam("id") Long jarId) {
        try {
            Jar result = catalogService.getJar(jarId);
            if (result != null) {
                return WSUtils.respond(OK, SUCCESS, result);
            }
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, jarId.toString());
    }

    /**
     * Deletes the jar of given {@code jarId}
     *
     * @param jarId
     */
    @DELETE
    @Path("/jars/{id}")
    @Timed
    public Response removeParser(@PathParam("id") Long jarId) {
        try {
            Jar removedParser = catalogService.removeJar(jarId);

            if (removedParser != null) {
                return WSUtils.respond(OK, SUCCESS, removedParser);
            } else {
                return WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, jarId.toString());
            }
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
    }

    /**
     * Downloads a given jar resource for given {@code jarId}
     *
     * @param jarId
     */
    @Timed
    @GET
    @Produces({"application/java-archive", "application/json"})
    @Path("/jars/download/{jarId}")
    public Response downloadParserJar(@PathParam("jarId") Long jarId) {
        try {
            Jar jar = catalogService.getJar(jarId);
            if (jar != null) {
                final InputStream inputStream = jarStorage.downloadJar(jar.getStoragePath());
                StreamingOutput streamOutput = new StreamingOutput() {
                    public void write(OutputStream os) throws IOException, WebApplicationException {
                        try {
                            ByteStreams.copy(inputStream, os);
                        } finally {
                            os.close();
                        }
                    }
                };
                return Response.ok(streamOutput).build();
            }
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, jarId.toString());
    }

}
