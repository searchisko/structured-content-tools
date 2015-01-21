/*
 * JBoss, Home of Professional Open Source
 * Copyright 2014 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.SettingsException;
import org.junit.Test;

/**
 * Unit test for {@link LongToTimestampValuePreprocessor}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class LongToTimestampValuePreprocessorTest {

	@Test
	public void init_settingerrors() {
		LongToTimestampValuePreprocessor tested = new LongToTimestampValuePreprocessor();
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
		settings.put(LongToTimestampValuePreprocessor.CFG_TARGET_FIELD, "tf");
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert
					.assertEquals("Missing or empty 'settings/source_field' configuration value for 'Test mapper' preprocessor",
							e.getMessage());
		}

		settings.put(LongToTimestampValuePreprocessor.CFG_SOURCE_FIELD, " ");
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
		settings.put(LongToTimestampValuePreprocessor.CFG_SOURCE_FIELD, "tf");
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert
					.assertEquals("Missing or empty 'settings/target_field' configuration value for 'Test mapper' preprocessor",
							e.getMessage());
		}

		settings.put(LongToTimestampValuePreprocessor.CFG_TARGET_FIELD, " ");
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert
					.assertEquals("Missing or empty 'settings/target_field' configuration value for 'Test mapper' preprocessor",
							e.getMessage());
		}
	}

	@Test
	public void init() {
		LongToTimestampValuePreprocessor tested = new LongToTimestampValuePreprocessor();
		Client client = null;

		// case - all ok
		{
			Map<String, Object> settings = new HashMap<String, Object>();
			settings.put(LongToTimestampValuePreprocessor.CFG_SOURCE_FIELD, "sf");
			settings.put(LongToTimestampValuePreprocessor.CFG_TARGET_FIELD, "tf");
			List<String> sb = new ArrayList<String>();
			settings.put(LongToTimestampValuePreprocessor.CFG_source_bases, sb);

			tested.init("Test mapper", client, settings);
			Assert.assertEquals("Test mapper", tested.getName());
			Assert.assertEquals(client, tested.client);
			Assert.assertEquals("sf", tested.getFieldSource());
			Assert.assertEquals("tf", tested.getFieldTarget());
			Assert.assertEquals(sb, tested.getSourceBases());
		}
	}

	@Test
	public void preprocessData_nobases() {

		LongToTimestampValuePreprocessor tested = new LongToTimestampValuePreprocessor();
		tested.name = "mypreproc";
		tested.fieldSource = "source";
		tested.fieldTarget = "target";

		// case - not NPE
		tested.preprocessData(null, null);

		// case - leave null if no value defined
		{
			Map<String, Object> values = new HashMap<String, Object>();
			tested.preprocessData(values, null);
			Assert.assertNull(values.get(tested.fieldTarget));
		}

		// case - leave null and warning if unsupported type
		{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put(tested.fieldSource, new Object());

			PreprocessChainContextImpl context = new PreprocessChainContextImpl();
			tested.preprocessData(values, context);
			Assert.assertNull(values.get(tested.fieldTarget));
			Assert.assertTrue(context.isWarning());
		}

		// *** String values handling

		// case - leave null if String is empty
		{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put(tested.fieldSource, null);
			tested.preprocessData(values, null);
			Assert.assertNull(values.get(tested.fieldTarget));
		}
		{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put(tested.fieldSource, "");
			tested.preprocessData(values, null);
			Assert.assertNull(values.get(tested.fieldTarget));
		}

		// case - leave warning if String is not a number
		{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put(tested.fieldSource, "aa");

			PreprocessChainContextImpl context = new PreprocessChainContextImpl();
			tested.preprocessData(values, context);
			Assert.assertNull(values.get(tested.fieldTarget));
			Assert.assertTrue(context.isWarning());
		}

		// case - perform conversion
		{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put(tested.fieldSource, "100");
			tested.preprocessData(values, null);
			Assert.assertEquals("1970-01-01T00:00:00.100Z", values.get(tested.fieldTarget));
		}

		// *** Number values handling

		// case - Integer
		{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put(tested.fieldSource, new Integer(10));
			tested.preprocessData(values, null);
			Assert.assertEquals("1970-01-01T00:00:00.010Z", values.get(tested.fieldTarget));
		}

		// case - Long, rewrite source
		{
			tested.fieldTarget = "source";
			Map<String, Object> values = new HashMap<String, Object>();
			values.put(tested.fieldSource, new Long(510));
			tested.preprocessData(values, null);
			Assert.assertEquals("1970-01-01T00:00:00.510Z", values.get(tested.fieldTarget));
		}
	}

	@Test
	public void preprocessData_bases() {

		LongToTimestampValuePreprocessor tested = new LongToTimestampValuePreprocessor();
		tested.name = "Test";
		tested.fieldSource = "source";
		tested.fieldTarget = "target";
		tested.sourceBases = Arrays.asList(new String[] { "author", "editor", "comments" });

		// case - test it
		{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put("author", createDataStructureMap("10", ""));
			values.put("editor", createDataStructureMap(new Long("150"), null));
			List<Map<String, Object>> comments = new ArrayList<Map<String, Object>>();
			values.put("comments", comments);

			Map<String, Object> comment1 = createDataStructureMap("250", "");
			comments.add(comment1);

			Map<String, Object> comment2 = createDataStructureMap("4521", null);
			comments.add(comment2);

			tested.preprocessData(values, null);

			assertDataStructure(values.get("author"), "10", "1970-01-01T00:00:00.010Z");
			assertDataStructure(values.get("editor"), new Long("150"), "1970-01-01T00:00:00.150Z");

			Assert.assertEquals(2, comments.size());
			assertDataStructure(comment1, "250", "1970-01-01T00:00:00.250Z");
			assertDataStructure(comment2, "4521", "1970-01-01T00:00:04.521Z");
		}
	}

	private Map<String, Object> createDataStructureMap(Object source, String target) {
		Map<String, Object> ret = new HashMap<String, Object>();
		ret.put("source", source);
		ret.put("terget", target);
		return ret;
	}

	@SuppressWarnings("unchecked")
	private void assertDataStructure(Object dataObj, Object source, String target) {
		Map<String, Object> data = (Map<String, Object>) dataObj;
		Assert.assertEquals(source, data.get("source"));
		Assert.assertEquals(target, data.get("target"));
	}

}
