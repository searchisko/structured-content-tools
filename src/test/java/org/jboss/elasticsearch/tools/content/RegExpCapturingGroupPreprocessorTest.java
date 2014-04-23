/*
 * JBoss, Home of Professional Open Source
 * Copyright 2014 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import junit.framework.Assert;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.SettingsException;
import org.junit.Test;

/**
 * Unit test for {@link RegExpCapturingGroupPreprocessor}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class RegExpCapturingGroupPreprocessorTest {

	private static final String TARGET_2 = "target2";
	private static final String TARGET_1 = "target1";
	private static final String TARGET_0 = "target";
	private static final Map<Object, String> RESULT_MAPPING_VALID = new HashMap<>();
	static {
		RESULT_MAPPING_VALID.put(new Integer(0), TARGET_0);
		RESULT_MAPPING_VALID.put("1", TARGET_1);
		RESULT_MAPPING_VALID.put(new Long(2), TARGET_2);
	}

	@Test
	public void init_settingerrors() {
		RegExpCapturingGroupPreprocessor tested = new RegExpCapturingGroupPreprocessor();
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
		settings.put(RegExpCapturingGroupPreprocessor.CFG_PATTERN, "(.*)");
		settings.put(RegExpCapturingGroupPreprocessor.CFG_RESULT_MAPPING, RESULT_MAPPING_VALID);
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert
					.assertEquals("Missing or empty 'settings/source_field' configuration value for 'Test mapper' preprocessor",
							e.getMessage());
		}

		settings.put(RegExpCapturingGroupPreprocessor.CFG_SOURCE_FIELD, " ");
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert
					.assertEquals("Missing or empty 'settings/source_field' configuration value for 'Test mapper' preprocessor",
							e.getMessage());
		}

		// case - pattern mandatory
		settings = new HashMap<String, Object>();
		settings.put(RegExpCapturingGroupPreprocessor.CFG_SOURCE_FIELD, "sf");
		settings.put(RegExpCapturingGroupPreprocessor.CFG_RESULT_MAPPING, RESULT_MAPPING_VALID);
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals("Missing or empty 'settings/pattern' configuration value for 'Test mapper' preprocessor",
					e.getMessage());
		}

		settings.put(RegExpCapturingGroupPreprocessor.CFG_PATTERN, " ");
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals("Missing or empty 'settings/pattern' configuration value for 'Test mapper' preprocessor",
					e.getMessage());
		}

		// case - pattern invalid
		settings.put(RegExpCapturingGroupPreprocessor.CFG_PATTERN, "(.");
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals(
					"'settings/pattern' configuration value for 'Test mapper' preprocessor is invalid: Unclosed group near index 2\n"
							+ "(.\n" + "  ^", e.getMessage());
		}

		// case - resultMapping mandatory
		settings = new HashMap<String, Object>();
		settings.put(RegExpCapturingGroupPreprocessor.CFG_SOURCE_FIELD, "sf");
		settings.put(RegExpCapturingGroupPreprocessor.CFG_PATTERN, "(.*)");
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals(
					"Missing or empty 'settings/result_mapping' configuration object for 'Test mapper' preprocessor",
					e.getMessage());
		}

		Map<Object, Object> rm = new HashMap<>();
		settings.put(RegExpCapturingGroupPreprocessor.CFG_RESULT_MAPPING, rm);
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals(
					"Missing or empty 'settings/result_mapping' configuration object for 'Test mapper' preprocessor",
					e.getMessage());
		}

		// case - resultMapping bad type
		settings.put(RegExpCapturingGroupPreprocessor.CFG_RESULT_MAPPING, "");
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals("'settings/result_mapping' configuration value for 'Test mapper' preprocessor is invalid",
					e.getMessage());
		}

		// case - resultMapping bad type of index
		rm.put(new Object(), "tf");
		settings.put(RegExpCapturingGroupPreprocessor.CFG_RESULT_MAPPING, rm);
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals(
					"Index must be a number in 'settings/result_mapping' configuration for 'Test mapper' preprocessor",
					e.getMessage());
		}

		// case - resultMapping empty value for index
		rm.put("", "tf");
		settings.put(RegExpCapturingGroupPreprocessor.CFG_RESULT_MAPPING, rm);
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals(
					"Missing or empty index in 'settings/result_mapping' configuration for 'Test mapper' preprocessor",
					e.getMessage());
		}

		// case - resultMapping bad type of field name
		rm.clear();
		rm.put(new Integer(1), new Object());
		settings.put(RegExpCapturingGroupPreprocessor.CFG_RESULT_MAPPING, rm);
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals(
					"Value for 'settings/result_mapping/1' configuration for 'Test mapper' preprocessor must be String",
					e.getMessage());
		}

		// case - resultMapping null or empty field name
		rm.clear();
		rm.put(new Integer(1), null);
		settings.put(RegExpCapturingGroupPreprocessor.CFG_RESULT_MAPPING, rm);
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals(
					"Missing or empty value in 'settings/result_mapping/1' configuration for 'Test mapper' preprocessor",
					e.getMessage());
		}

		// case - resultMapping null or empty field name
		rm.clear();
		rm.put(new Integer(1), "  ");
		settings.put(RegExpCapturingGroupPreprocessor.CFG_RESULT_MAPPING, rm);
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals(
					"Missing or empty value in 'settings/result_mapping/1' configuration for 'Test mapper' preprocessor",
					e.getMessage());
		}

	}

	@Test
	public void init() {
		RegExpCapturingGroupPreprocessor tested = new RegExpCapturingGroupPreprocessor();
		Client client = null;

		// case - all ok
		{
			Map<String, Object> settings = new HashMap<String, Object>();
			settings.put(RegExpCapturingGroupPreprocessor.CFG_SOURCE_FIELD, "sf");
			settings.put(RegExpCapturingGroupPreprocessor.CFG_PATTERN, "(.*)");
			settings.put(RegExpCapturingGroupPreprocessor.CFG_RESULT_MAPPING, RESULT_MAPPING_VALID);
			List<String> sb = new ArrayList<String>();
			settings.put(LongToTimestampValuePreprocessor.CFG_source_bases, sb);

			tested.init("Test mapper", client, settings);
			Assert.assertEquals("Test mapper", tested.getName());
			Assert.assertEquals(client, tested.client);
			Assert.assertEquals("sf", tested.getFieldSource());
			Assert.assertEquals("(.*)", tested.getPattern());
			Assert.assertEquals(RESULT_MAPPING_VALID, tested.getResultMapping());
			Assert.assertEquals(sb, tested.getSourceBases());
		}
	}

	@Test
	public void preprocessData_nobases() {

		RegExpCapturingGroupPreprocessor tested = new RegExpCapturingGroupPreprocessor();
		tested.name = "mypreproc";
		tested.fieldSource = "source";
		tested.resultMapping = RESULT_MAPPING_VALID;
		tested.patternCompiled = Pattern.compile("num\\s(\\d+)\\sof\\s(.+)");

		// case - not NPE
		tested.preprocessData(null, null);

		// case - leave null and no warning if no source value defined
		{
			Map<String, Object> values = new HashMap<String, Object>();
			PreprocessChainContextImpl context = new PreprocessChainContextImpl();
			tested.preprocessData(values, context);
			Assert.assertNull(values.get(TARGET_0));
			Assert.assertNull(values.get(TARGET_1));
			Assert.assertNull(values.get(TARGET_2));
			Assert.assertFalse(context.isWarning());
		}

		// case - leave null and warning if unsupported type
		{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put(tested.fieldSource, new Object());

			PreprocessChainContextImpl context = new PreprocessChainContextImpl();
			tested.preprocessData(values, context);
			Assert.assertNull(values.get(TARGET_0));
			Assert.assertNull(values.get(TARGET_1));
			Assert.assertNull(values.get(TARGET_2));
			Assert.assertTrue(context.isWarning());
		}

		// case - leave targets null and no warning if source String is null
		{
			Map<String, Object> values = new HashMap<String, Object>();
			PreprocessChainContextImpl context = new PreprocessChainContextImpl();
			values.put(tested.fieldSource, null);
			tested.preprocessData(values, context);
			Assert.assertNull(values.get(TARGET_0));
			Assert.assertNull(values.get(TARGET_1));
			Assert.assertNull(values.get(TARGET_2));
			Assert.assertFalse(context.isWarning());
		}

		// case - pattern matches
		{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put(tested.fieldSource, "num 2 of test");
			PreprocessChainContextImpl context = new PreprocessChainContextImpl();
			tested.preprocessData(values, context);
			Assert.assertEquals("num 2 of test", values.get(TARGET_0));
			Assert.assertEquals("2", values.get(TARGET_1));
			Assert.assertEquals("test", values.get(TARGET_2));
			Assert.assertFalse(context.isWarning());
		}

		// case - pattern not matches
		{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put(tested.fieldSource, "num 2 of");
			PreprocessChainContextImpl context = new PreprocessChainContextImpl();
			tested.preprocessData(values, context);
			Assert.assertEquals(null, values.get(TARGET_0));
			Assert.assertEquals(null, values.get(TARGET_1));
			Assert.assertEquals(null, values.get(TARGET_2));
			Assert.assertTrue(context.isWarning());
		}
		{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put(tested.fieldSource, "");
			PreprocessChainContextImpl context = new PreprocessChainContextImpl();
			tested.preprocessData(values, context);
			Assert.assertNull(values.get(TARGET_0));
			Assert.assertNull(values.get(TARGET_1));
			Assert.assertNull(values.get(TARGET_2));
			Assert.assertTrue(context.isWarning());
		}

	}

}
