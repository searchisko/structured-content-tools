/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.elasticsearch.client.Client;
import org.junit.Test;

/**
 * Unit test for {@link StructuredContentPreprocessorFactory}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class StructuredContentPreprocessorFactoryTest {

  @SuppressWarnings("unchecked")
  @Test
  public void createPreprocessor() {
    Client clientMock = mock(Client.class);

    List<Map<String, Object>> preprocessorConfig = (List<Map<String, Object>>) (StructureUtils
        .loadJSONFromJarPackagedFile("/StructuredContentPreprocessorFactory.json")).get("preprocessors");
    StructuredContentPreprocessor preproc = StructuredContentPreprocessorFactory.createPreprocessor(
        preprocessorConfig.get(0), clientMock);

    Assert.assertEquals("Status Normalizer", preproc.getName());
    Assert.assertEquals(clientMock, ((StructuredContentPreprocessorMock) preproc).client);
    Assert.assertEquals("value1", ((StructuredContentPreprocessorMock) preproc).settings.get("some_setting_1_1"));
    Assert.assertEquals("value2", ((StructuredContentPreprocessorMock) preproc).settings.get("some_setting_1_2"));

  }

  @SuppressWarnings("unchecked")
  @Test
  public void createPreprocessors() {
    Client clientMock = mock(Client.class);

    List<Map<String, Object>> preprocessorConfig = (List<Map<String, Object>>) (StructureUtils
        .loadJSONFromJarPackagedFile("/StructuredContentPreprocessorFactory.json")).get("preprocessors");
    List<StructuredContentPreprocessor> preprocs = StructuredContentPreprocessorFactory.createPreprocessors(
        preprocessorConfig, clientMock);
    Assert.assertEquals(2, preprocs.size());
    Assert.assertEquals("Status Normalizer", preprocs.get(0).getName());
    Assert.assertEquals(clientMock, ((StructuredContentPreprocessorMock) preprocs.get(0)).client);
    Assert.assertEquals("value1",
        ((StructuredContentPreprocessorMock) preprocs.get(0)).settings.get("some_setting_1_1"));
    Assert.assertEquals("value2",
        ((StructuredContentPreprocessorMock) preprocs.get(0)).settings.get("some_setting_1_2"));
    Assert.assertEquals("Issue type Normalizer", preprocs.get(1).getName());
    Assert.assertEquals(clientMock, ((StructuredContentPreprocessorMock) preprocs.get(1)).client);
    Assert.assertEquals("value1",
        ((StructuredContentPreprocessorMock) preprocs.get(1)).settings.get("some_setting_2_1"));
    Assert.assertEquals("value2",
        ((StructuredContentPreprocessorMock) preprocs.get(1)).settings.get("some_setting_2_2"));
  }

}
