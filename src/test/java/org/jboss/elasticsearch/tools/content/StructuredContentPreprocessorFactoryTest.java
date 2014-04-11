/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.elasticsearch.client.Client;
import org.jboss.elasticsearch.tools.content.testtools.TestUtils;
import org.junit.Test;

import static org.mockito.Mockito.mock;

/**
 * Unit test for {@link StructuredContentPreprocessorFactory}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class StructuredContentPreprocessorFactoryTest {

	@Test
	public void createPreprocessor() {
		Client clientMock = mock(Client.class);

		StructuredContentPreprocessor preproc = StructuredContentPreprocessorFactory.createPreprocessor(
				getTestingPreprocessorConfig(), clientMock);

		Assert.assertEquals("Status Normalizer", preproc.getName());
		Assert.assertEquals(clientMock, ((StructuredContentPreprocessorMock) preproc).client);
		Assert.assertEquals("value1", ((StructuredContentPreprocessorMock) preproc).settings.get("some_setting_1_1"));
		Assert.assertEquals("value2", ((StructuredContentPreprocessorMock) preproc).settings.get("some_setting_1_2"));

	}

	@Test(expected = IllegalArgumentException.class)
	public void createPreprocessor_name_missing() {
		Client clientMock = mock(Client.class);

		Map<String, Object> preprocessorConfig = getTestingPreprocessorConfig();
		preprocessorConfig.remove(StructuredContentPreprocessorFactory.CFG_NAME);
		StructuredContentPreprocessorFactory.createPreprocessor(preprocessorConfig, clientMock);
	}

	@Test(expected = IllegalArgumentException.class)
	public void createPreprocessor_name_empty() {
		Client clientMock = mock(Client.class);

		Map<String, Object> preprocessorConfig = getTestingPreprocessorConfig();
		preprocessorConfig.put(StructuredContentPreprocessorFactory.CFG_NAME, " ");
		StructuredContentPreprocessorFactory.createPreprocessor(preprocessorConfig, clientMock);
	}

	@Test(expected = IllegalArgumentException.class)
	public void createPreprocessor_class_missing() {
		Client clientMock = mock(Client.class);

		Map<String, Object> preprocessorConfig = getTestingPreprocessorConfig();
		preprocessorConfig.remove(StructuredContentPreprocessorFactory.CFG_CLASS);
		StructuredContentPreprocessorFactory.createPreprocessor(preprocessorConfig, clientMock);
	}

	@Test(expected = IllegalArgumentException.class)
	public void createPreprocessor_class_empty() {
		Client clientMock = mock(Client.class);

		Map<String, Object> preprocessorConfig = getTestingPreprocessorConfig();
		preprocessorConfig.put(StructuredContentPreprocessorFactory.CFG_CLASS, " ");
		StructuredContentPreprocessorFactory.createPreprocessor(preprocessorConfig, clientMock);
	}

	@Test(expected = IllegalArgumentException.class)
	public void createPreprocessor_class_unknown() {
		Client clientMock = mock(Client.class);

		Map<String, Object> preprocessorConfig = getTestingPreprocessorConfig();
		preprocessorConfig.put(StructuredContentPreprocessorFactory.CFG_CLASS, "org.unknown.Test");
		StructuredContentPreprocessorFactory.createPreprocessor(preprocessorConfig, clientMock);
	}

	@Test(expected = IllegalArgumentException.class)
	public void createPreprocessor_class_badtype() {
		Client clientMock = mock(Client.class);

		Map<String, Object> preprocessorConfig = getTestingPreprocessorConfig();
		preprocessorConfig.put(StructuredContentPreprocessorFactory.CFG_CLASS, "java.lang.String");
		StructuredContentPreprocessorFactory.createPreprocessor(preprocessorConfig, clientMock);
	}

	@Test(expected = IllegalArgumentException.class)
	public void createPreprocessor_settings_badtype() {
		Client clientMock = mock(Client.class);
		Map<String, Object> preprocessorConfig = getTestingPreprocessorConfig();
		preprocessorConfig.put(StructuredContentPreprocessorFactory.CFG_SETTINGS, "");
		StructuredContentPreprocessorFactory.createPreprocessor(preprocessorConfig, clientMock);
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> getTestingPreprocessorConfig() {
		List<Map<String, Object>> preprocessorConfigList = (List<Map<String, Object>>) (TestUtils
				.loadJSONFromClasspathFile("/StructuredContentPreprocessorFactory.json")).get("preprocessors");
		Map<String, Object> preprocessorConfig = preprocessorConfigList.get(0);
		return preprocessorConfig;
	}

	@SuppressWarnings("unchecked")
	@Test
	public void createPreprocessors() {
		Client clientMock = mock(Client.class);

		List<Map<String, Object>> preprocessorConfig = (List<Map<String, Object>>) (TestUtils
				.loadJSONFromClasspathFile("/StructuredContentPreprocessorFactory.json")).get("preprocessors");
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
