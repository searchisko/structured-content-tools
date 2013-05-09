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

/**
 * Unit test for {@link TrimStringValuePreprocessor}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class TrimStringValuePreprocessorTest {

	@Test
	public void init_settingerrors() {
		TrimStringValuePreprocessor tested = new TrimStringValuePreprocessor();
		Map<String, Object> settings = null;

		// case - settings mandatory
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals("'settings' section is not defined for preprocessor Test mapper", e.getMessage());
		}

		// case - source_field mandatory
		settings = new HashMap<String, Object>();
		settings.put(TrimStringValuePreprocessor.CFG_TARGET_FIELD, "tf");
		settings.put(TrimStringValuePreprocessor.CFG_MAX_SIZE, "10");
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert
					.assertEquals("Missing or empty 'settings/source_field' configuration value for 'Test mapper' preprocessor",
							e.getMessage());
		}

		settings.put(TrimStringValuePreprocessor.CFG_SOURCE_FIELD, " ");
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert
					.assertEquals("Missing or empty 'settings/source_field' configuration value for 'Test mapper' preprocessor",
							e.getMessage());
		}

		// case - target_field mandatory
		settings = new HashMap<String, Object>();
		settings.put(TrimStringValuePreprocessor.CFG_SOURCE_FIELD, "tf");
		settings.put(TrimStringValuePreprocessor.CFG_MAX_SIZE, "10");
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert
					.assertEquals("Missing or empty 'settings/target_field' configuration value for 'Test mapper' preprocessor",
							e.getMessage());
		}

		settings.put(TrimStringValuePreprocessor.CFG_TARGET_FIELD, " ");
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert
					.assertEquals("Missing or empty 'settings/target_field' configuration value for 'Test mapper' preprocessor",
							e.getMessage());
		}

		// case - max_size mandatory and must be integer
		settings = new HashMap<String, Object>();
		settings.put(TrimStringValuePreprocessor.CFG_SOURCE_FIELD, "sf");
		settings.put(TrimStringValuePreprocessor.CFG_TARGET_FIELD, "tf");
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals("Missing or empty 'settings/max_size' configuration value for 'Test mapper' preprocessor",
					e.getMessage());
		}

		settings.put(TrimStringValuePreprocessor.CFG_MAX_SIZE, " ");
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals("Non integer 'settings/max_size' configuration value for 'Test mapper' preprocessor",
					e.getMessage());
		}

		settings.put(TrimStringValuePreprocessor.CFG_MAX_SIZE, "aaaa");
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals("Non integer 'settings/max_size' configuration value for 'Test mapper' preprocessor",
					e.getMessage());
		}

		settings.put(TrimStringValuePreprocessor.CFG_MAX_SIZE, "10.2");
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals("Non integer 'settings/max_size' configuration value for 'Test mapper' preprocessor",
					e.getMessage());
		}

		settings.put(TrimStringValuePreprocessor.CFG_MAX_SIZE, new HashMap<Object, Object>());
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals("Non integer 'settings/max_size' configuration value for 'Test mapper' preprocessor",
					e.getMessage());
		}

	}

	@Test
	public void init() {
		TrimStringValuePreprocessor tested = new TrimStringValuePreprocessor();
		Client client = null;

		// case - all ok, max_size is String
		{
			Map<String, Object> settings = new HashMap<String, Object>();
			settings.put(TrimStringValuePreprocessor.CFG_SOURCE_FIELD, "sf");
			settings.put(TrimStringValuePreprocessor.CFG_TARGET_FIELD, "tf");
			settings.put(TrimStringValuePreprocessor.CFG_MAX_SIZE, "10");

			tested.init("Test mapper", client, settings);
			Assert.assertEquals("Test mapper", tested.getName());
			Assert.assertEquals(client, tested.client);
			Assert.assertEquals("sf", tested.getFieldSource());
			Assert.assertEquals("tf", tested.getFieldTarget());
			Assert.assertEquals(10, tested.getMaxSize());
		}

		// case - all ok, max_size is Integer
		{
			Map<String, Object> settings = new HashMap<String, Object>();
			settings.put(TrimStringValuePreprocessor.CFG_SOURCE_FIELD, "sf");
			settings.put(TrimStringValuePreprocessor.CFG_TARGET_FIELD, "tf");
			settings.put(TrimStringValuePreprocessor.CFG_MAX_SIZE, new Integer(20));

			tested.init("Test mapper", client, settings);
			Assert.assertEquals("Test mapper", tested.getName());
			Assert.assertEquals(client, tested.client);
			Assert.assertEquals("sf", tested.fieldSource);
			Assert.assertEquals("tf", tested.fieldTarget);
			Assert.assertEquals(20, tested.maxSize);
		}

	}

	@Test
	public void preprocessData() {

		TrimStringValuePreprocessor tested = new TrimStringValuePreprocessor();
		tested.fieldSource = "source";
		tested.fieldTarget = "target";
		tested.maxSize = 5;

		// case - not NPE
		tested.preprocessData(null);

		// case - leave null if no value defined
		{
			Map<String, Object> values = new HashMap<String, Object>();
			tested.preprocessData(values);
			Assert.assertNull(values.get(tested.fieldTarget));
		}

		// case - leave null if value is not String
		{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put(tested.fieldSource, new Integer(10));
			tested.preprocessData(values);
			Assert.assertNull(values.get(tested.fieldTarget));
		}

		// case - leave size if empty
		{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put(tested.fieldSource, "");
			tested.preprocessData(values);
			Assert.assertEquals("", values.get(tested.fieldTarget));
		}

		// case - trim empty value
		{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put(tested.fieldSource, "   ");
			tested.preprocessData(values);
			Assert.assertEquals("", values.get(tested.fieldTarget));
		}

		// case - do not shorten value
		{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put(tested.fieldSource, "aabbc");
			tested.preprocessData(values);
			Assert.assertEquals("aabbc", values.get(tested.fieldTarget));
		}

		// case - shorten value
		{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put(tested.fieldSource, "abcdef");
			tested.preprocessData(values);
			Assert.assertEquals("abcde", values.get(tested.fieldTarget));
		}

		// case - trim value whitespaces, do not shorten it
		{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put(tested.fieldSource, " aa  ");
			tested.preprocessData(values);
			Assert.assertEquals("aa", values.get(tested.fieldTarget));
		}

		// case - trim value whitespaces and shorten it
		{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put(tested.fieldSource, " too long value  ");
			tested.preprocessData(values);
			Assert.assertEquals("too l", values.get(tested.fieldTarget));
		}

		// case - dot notation
		{
			tested.fieldSource = "my_field.level1.level2";
			tested.fieldTarget = "my_field.level21.level22";
			tested.maxSize = 3;
			Map<String, Object> values = new HashMap<String, Object>();
			StructureUtils.putValueIntoMapOfMaps(values, tested.fieldSource, "   Value   ");
			tested.preprocessData(values);
			Assert.assertEquals("Val", XContentMapValues.extractValue(tested.fieldTarget, values));
		}
	}
}
