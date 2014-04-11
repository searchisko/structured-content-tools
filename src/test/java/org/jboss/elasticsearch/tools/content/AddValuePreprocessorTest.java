/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit test for {@link AddValuePreprocessor}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class AddValuePreprocessorTest {

	@Test
	public void init_settingerrors() {
		AddValuePreprocessor tested = new AddValuePreprocessor();
		Client client = Mockito.mock(Client.class);
		Map<String, Object> settings = null;

		// case - settings mandatory
		try {
			tested.init("Test mapper", client, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals("'settings' section is not defined for preprocessor Test mapper", e.getMessage());
		}

		// case - field mandatory
		settings = new HashMap<String, Object>();
		try {
			tested.init("Test mapper", client, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals("Missing or empty 'settings/field' configuration value for 'Test mapper' preprocessor",
					e.getMessage());
		}

		settings.put(AddValuePreprocessor.CFG_FIELD, " ");
		try {
			tested.init("Test mapper", client, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals("Missing or empty 'settings/field' configuration value for 'Test mapper' preprocessor",
					e.getMessage());
		}

		// case - no more mandatory setting fields
		settings.put(AddValuePreprocessor.CFG_FIELD, "field");
		tested.init("Test mapper", client, settings);
	}

	@Test
	public void init() {
		AddValuePreprocessor tested = new AddValuePreprocessor();
		Client client = Mockito.mock(Client.class);

		// case - null value
		{
			Map<String, Object> settings = new HashMap<String, Object>();
			settings.put(AddValuePreprocessor.CFG_FIELD, "source");
			settings.put(AddValuePreprocessor.CFG_VALUE, null);

			tested.init("Test mapper", client, settings);
			Assert.assertEquals("Test mapper", tested.getName());
			Assert.assertEquals(client, tested.client);
			Assert.assertEquals("source", tested.field);
			Assert.assertNull(tested.value);
		}

		// case - String value
		{
			Map<String, Object> settings = new HashMap<String, Object>();
			settings.put(AddValuePreprocessor.CFG_FIELD, "source");
			settings.put(AddValuePreprocessor.CFG_VALUE, "String Value");

			tested.init("Test mapper", client, settings);
			Assert.assertEquals("Test mapper", tested.getName());
			Assert.assertEquals(client, tested.client);
			Assert.assertEquals("source", tested.field);
			Assert.assertEquals("String Value", tested.value);
		}

		// case - empty String value
		{
			Map<String, Object> settings = new HashMap<String, Object>();
			settings.put(AddValuePreprocessor.CFG_FIELD, "source");
			settings.put(AddValuePreprocessor.CFG_VALUE, "");

			tested.init("Test mapper", client, settings);
			Assert.assertEquals("Test mapper", tested.getName());
			Assert.assertEquals(client, tested.client);
			Assert.assertEquals("source", tested.field);
			Assert.assertEquals("", tested.value);
		}

		// case - Integer value
		{
			Map<String, Object> settings = new HashMap<String, Object>();
			settings.put(AddValuePreprocessor.CFG_FIELD, "source");
			settings.put(AddValuePreprocessor.CFG_VALUE, new Integer(10));

			tested.init("Test mapper", client, settings);
			Assert.assertEquals("Test mapper", tested.getName());
			Assert.assertEquals(client, tested.client);
			Assert.assertEquals("source", tested.field);
			Assert.assertEquals(new Integer(10), tested.value);
		}
	}

	@Test
	public void preprocessData() {

		AddValuePreprocessor tested = new AddValuePreprocessor();
		tested.field = "my_field";

		// case - not NPE
		tested.preprocessData(null, null);

		// case - leave null if no value defined
		{
			Map<String, Object> values = new HashMap<String, Object>();
			tested.preprocessData(values, null);
			Assert.assertNull(values.get(tested.field));
		}

		// case - make null if no value defined
		{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put(tested.field, "value");
			tested.preprocessData(values, null);
			Assert.assertNull(values.get(tested.field));
		}

		// case - fill String value over null
		{
			Map<String, Object> values = new HashMap<String, Object>();
			tested.value = "Value";
			tested.preprocessData(values, null);
			Assert.assertEquals("Value", values.get(tested.field));
		}

		// case - rewrite String value value over null
		{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put(tested.field, "value old");
			tested.value = "Value";
			tested.preprocessData(values, null);
			Assert.assertEquals("Value", values.get(tested.field));
		}

		// case - fill Integer value over null
		{
			Map<String, Object> values = new HashMap<String, Object>();
			tested.value = new Integer(10);
			tested.preprocessData(values, null);
			Assert.assertEquals(new Integer(10), values.get(tested.field));
		}

		// case - rewrite Integer value value over null
		{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put(tested.field, "value old");
			tested.value = new Integer(10);
			tested.preprocessData(values, null);
			Assert.assertEquals(new Integer(10), values.get(tested.field));
		}

		// case - fill String value over null - dot notation
		tested.field = "my_field.level1.level2";
		{
			Map<String, Object> values = new HashMap<String, Object>();
			tested.value = "Value";
			tested.preprocessData(values, null);
			Assert.assertEquals("Value", XContentMapValues.extractValue(tested.field, values));
		}
	}
}
