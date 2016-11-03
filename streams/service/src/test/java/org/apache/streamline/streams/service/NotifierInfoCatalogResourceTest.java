package org.apache.streamline.streams.service;

import org.apache.registries.common.QueryParam;
import org.apache.streamline.common.catalog.CatalogResponse;
import org.apache.registries.common.util.FileStorage;
import org.apache.streamline.streams.catalog.NotifierInfo;
import org.apache.streamline.streams.catalog.service.StreamCatalogService;
import mockit.Expectations;
import mockit.Injectable;
import mockit.integration.junit4.JMockit;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import java.io.InputStream;
import java.util.Arrays;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static org.junit.Assert.assertEquals;

/**
 * Created by aiyer on 9/28/15.
 */
@RunWith(JMockit.class)
public class NotifierInfoCatalogResourceTest {

    NotifierInfoCatalogResource resource;

    @Injectable
    StreamCatalogService mockCatalogService;

    @Injectable
    UriInfo mockUriInfo;

    @Injectable
    FileStorage mockFileStorage;

    @Injectable
    InputStream mockInputStream;

    @Injectable
    FormDataContentDisposition mockFormDataContentDisposition;

    @Injectable
    FormDataBodyPart mockFormDataBodyPart;

    MultivaluedMap<String, String> multiValuedMap;

    @Before
    public void setUp() throws Exception {
        resource = new NotifierInfoCatalogResource(mockCatalogService, mockFileStorage);
        multiValuedMap = new MultivaluedHashMap<>();
    }

    @Test
    public void testListNotifiers() throws Exception {
        final NotifierInfo notifierInfo = new NotifierInfo();
        final QueryParam expectedQp = new QueryParam("notifierName", "email_notifier_1");
        multiValuedMap.putSingle("notifierName", "email_notifier_1");
        new Expectations() {
            {
                mockUriInfo.getQueryParameters(); times = 1;
                result = multiValuedMap;

                mockCatalogService.listNotifierInfos(Arrays.asList(expectedQp));times=1;
                result = Arrays.asList(notifierInfo);

            }
        };

        CatalogResponse catalogResponse = (CatalogResponse) resource.listNotifiers(mockUriInfo).getEntity();

        assertEquals(CatalogResponse.ResponseMessage.SUCCESS.getCode(), catalogResponse.getResponseCode());
        assertEquals(1, catalogResponse.getEntities().size());
        assertEquals(notifierInfo, catalogResponse.getEntities().iterator().next());
    }

    @Test
    public void testGetNotifierById() throws Exception {
        final NotifierInfo notifierInfo = new NotifierInfo();
        new Expectations() {
            {
                mockCatalogService.getNotifierInfo(anyLong);times=1;
                result = notifierInfo;
            }
        };

        CatalogResponse catalogResponse = (CatalogResponse) resource.getNotifierById(1L).getEntity();

        assertEquals(CatalogResponse.ResponseMessage.SUCCESS.getCode(), catalogResponse.getResponseCode());
        assertEquals(notifierInfo, catalogResponse.getEntity());
    }

    @Test
    public void testAddNotifier() throws Exception {
        final NotifierInfo notifierInfo = new NotifierInfo();
        notifierInfo.setName("test");
        new Expectations() {
            {
                mockCatalogService.addNotifierInfo(notifierInfo);times=1;
                result = notifierInfo;
                mockFormDataBodyPart.getMediaType();
                result = APPLICATION_JSON_TYPE;
                mockFormDataBodyPart.getValueAs(NotifierInfo.class);
                result = notifierInfo;
                mockFileStorage.uploadFile(mockInputStream, anyString);
                result = "uploadedPath";

            }
        };

        CatalogResponse catalogResponse = (CatalogResponse) resource.addNotifier(
                mockInputStream,
                mockFormDataContentDisposition,
                mockFormDataBodyPart).getEntity();

        assertEquals(CatalogResponse.ResponseMessage.SUCCESS.getCode(), catalogResponse.getResponseCode());
        assertEquals(notifierInfo, catalogResponse.getEntity());
    }

}