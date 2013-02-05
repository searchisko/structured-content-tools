/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.junit.Test;

/**
 * Unit test for {@link AddMultipleValuesPreprocessor}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class AddMultipleValuesPreprocessorTest {

	@Test
	public void init_settingerrors() {
		AddMultipleValuesPreprocessor tested = new AddMultipleValuesPreprocessor();
		Map<String, Object> settings = null;

		// case - settings mandatory
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals("'settings' section is not defined for preprocessor Test mapper", e.getMessage());
		}

		// case - no more mandatory setting fields
		settings = new HashMap<String, Object>();
		tested.init("Test mapper", null, settings);
	}

	@Test
	public void init() {
		AddMultipleValuesPreprocessor tested = new AddMultipleValuesPreprocessor();

		{
			Map<String, Object> settings = new HashMap<String, Object>();
			settings.put("field1", "source");
			settings.put("field2", "");

			tested.init("Test mapper", null, settings);
			Assert.assertEquals("Test mapper", tested.getName());
			Assert.assertEquals(2, tested.fields.size());
			Assert.assertEquals(settings, tested.fields);
		}
	}

	@Test
	public void preprocessData() {

		AddMultipleValuesPreprocessor tested = new AddMultipleValuesPreprocessor();
		Map<String, Object> settings = new HashMap<String, Object>();
		settings.put("field_constant", "constant value");
		settings.put("field_empty", "");
		settings.put("field_null", null);
		settings.put("field_null_replace", null);
		settings.put("field_replace_unknown", "{user.unknown}");
		settings.put("field_replace_simple", "{title}");
		settings.put("field_replace_nested", "{user.name}");
		settings.put("field_replace.complex", "I'm {user.name} and like to read '{title}'");
		settings.put("field_replace.complex2", "{title} - {user.name}");
		tested.fields = settings;

		// case - not NPE
		tested.preprocessData(null);

		// case - leave null if no value defined
		{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put("title", "Gratest book ever");
			values.put("field_null_replace", "replace me");
			Map<String, Object> user = new HashMap<String, Object>();
			user.put("name", "Joe");
			values.put("user", user);

			tested.preprocessData(values);
			Assert.assertEquals("constant value", values.get("field_constant"));
			Assert.assertEquals("", values.get("field_empty"));
			Assert.assertNull(values.get("field_null"));
			Assert.assertNull(values.get("field_null_replace"));
			Assert.assertEquals("", values.get("field_replace_unknown"));
			Assert.assertEquals("Gratest book ever", values.get("field_replace_simple"));
			Assert.assertEquals("Joe", values.get("field_replace_nested"));
			Assert.assertEquals("I'm Joe and like to read 'Gratest book ever'",
					XContentMapValues.extractValue("field_replace.complex", values));
			Assert.assertEquals("Gratest book ever - Joe", XContentMapValues.extractValue("field_replace.complex2", values));
		}

	}
}
