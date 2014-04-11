/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.SettingsException;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit test for {@link MaxTimestampPreprocessor}
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class MaxTimestampPreprocessorTest {

	@Test
	public void init() {
		MaxTimestampPreprocessor tested = new MaxTimestampPreprocessor();
		Client client = Mockito.mock(Client.class);
		Map<String, Object> settings = null;

		// case - settings mandatory
		try {
			tested.init("Test mapper", client, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals("'settings' section is not defined for preprocessor Test mapper", e.getMessage());
		}

		// case - source_field mandatory
		settings = new HashMap<String, Object>();
		try {
			tested.init("Test mapper", client, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert
					.assertEquals("Missing or empty 'settings/source_field' configuration value for 'Test mapper' preprocessor",
							e.getMessage());
		}

		// case - target_field mandatory
		settings.put(MaxTimestampPreprocessor.CFG_SOURCE_FIELD, "source");
		try {
			tested.init("Test mapper", client, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert
					.assertEquals("Missing or empty 'settings/target_field' configuration value for 'Test mapper' preprocessor",
							e.getMessage());
		}

		// case - no more mandatory setting fields
		settings.put(MaxTimestampPreprocessor.CFG_TARGET_FIELD, "target");
		tested.init("Test mapper", client, settings);
		Assert.assertEquals("source", tested.fieldSource);
		Assert.assertEquals("target", tested.fieldTarget);
	}

	@Test
	public void preprocessData() {
		Client client = Mockito.mock(Client.class);

		MaxTimestampPreprocessor tested = new MaxTimestampPreprocessor();
		Map<String, Object> settings = new HashMap<String, Object>();

		settings.put(MaxTimestampPreprocessor.CFG_SOURCE_FIELD, "source");
		settings.put(MaxTimestampPreprocessor.CFG_TARGET_FIELD, "target");

		tested.init("Test mapper", client, settings);

		// case - not NPE
		tested.preprocessData(null, null);

		// case - let target null if input field is empty
		{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put("target", new Integer(10));
			tested.preprocessData(values, null);
			Assert.assertEquals(null, values.get("target"));
		}

		// case - let target null if input field is bad type, no exception
		{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put("source", new Integer(10));
			PreprocessChainContextImpl chainContext = new PreprocessChainContextImpl();
			tested.preprocessData(values, chainContext);
			Assert.assertEquals(null, values.get("target"));
			Assert.assertTrue(chainContext.isWarning());
		}

		// case - select max from list, ignore bad value formats and types - no chainContext
		{
			Map<String, Object> values = new HashMap<String, Object>();
			List<Object> source = new ArrayList<Object>();
			source.add("2012-01-15T12:24:44Z");
			source.add("2012-01-15T17:40:44Z");
			source.add("badformat");
			source.add("2012-01-15T17:40:45+0100");
			source.add(new Integer(10));
			values.put("source", source);
			tested.preprocessData(values, null);
			Assert.assertEquals("2012-01-15T17:40:44Z", values.get("target"));
		}

		// case - select max from list, ignore bad value formats and types - is chainContext
		{
			Map<String, Object> values = new HashMap<String, Object>();
			List<Object> source = new ArrayList<Object>();
			source.add("2012-01-15T12:24:44Z");
			source.add("badformat");
			source.add("2012-01-15T17:40:45+0100");
			source.add(new Integer(10));
			source.add("2012-01-15T17:40:44Z");
			values.put("source", source);
			PreprocessChainContextImpl chainContext = new PreprocessChainContextImpl();
			tested.preprocessData(values, chainContext);
			Assert.assertEquals("2012-01-15T17:40:44Z", values.get("target"));
			Assert.assertEquals(2, chainContext.getWarnings().size());
		}

	}

}
