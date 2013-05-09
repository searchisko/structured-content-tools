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
import org.junit.Test;

/**
 * Unit test for {@link StripHtmlPreprocessor}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class StripHtmlPreprocessorTest {

	@Test
	public void init_settingerrors() {
		StripHtmlPreprocessor tested = new StripHtmlPreprocessor();
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
		settings.put(StripHtmlPreprocessor.CFG_TARGET_FIELD, "tf");
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert
					.assertEquals("Missing or empty 'settings/source_field' configuration value for 'Test mapper' preprocessor",
							e.getMessage());
		}

		settings.put(StripHtmlPreprocessor.CFG_SOURCE_FIELD, " ");
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
		settings.put(StripHtmlPreprocessor.CFG_SOURCE_FIELD, "tf");
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert
					.assertEquals("Missing or empty 'settings/target_field' configuration value for 'Test mapper' preprocessor",
							e.getMessage());
		}

		settings.put(StripHtmlPreprocessor.CFG_TARGET_FIELD, " ");
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
		StripHtmlPreprocessor tested = new StripHtmlPreprocessor();
		Client client = null;

		// case - all ok
		{
			Map<String, Object> settings = new HashMap<String, Object>();
			settings.put(StripHtmlPreprocessor.CFG_SOURCE_FIELD, "sf");
			settings.put(StripHtmlPreprocessor.CFG_TARGET_FIELD, "tf");

			tested.init("Test mapper", client, settings);
			Assert.assertEquals("Test mapper", tested.getName());
			Assert.assertEquals(client, tested.client);
			Assert.assertEquals("sf", tested.getFieldSource());
			Assert.assertEquals("tf", tested.getFieldTarget());
		}

	}

	@Test
	public void preprocessData() {

		StripHtmlPreprocessor tested = new StripHtmlPreprocessor();
		tested.name = "Test";
		tested.fieldSource = "source";
		tested.fieldTarget = "target";

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

		// case - do not trim empty value
		{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put(tested.fieldSource, "   ");
			tested.preprocessData(values);
			Assert.assertEquals("   ", values.get(tested.fieldTarget));
		}

		// case - process HTML
		{
			Map<String, Object> values = new HashMap<String, Object>();
			values
					.put(tested.fieldSource,
							"<b>aa<b>bb<br>cdgh &lt;<div>text in div</div>\n &amp; then\n invalid <p> paragraph <pre>test\npre &amp;</pre>");
			tested.preprocessData(values);
			Assert.assertEquals("aabb cdgh < text in div & then invalid paragraph test\npre &",
					(String) values.get(tested.fieldTarget));
		}

		// case - process HTML - dot notation for source and target
		{
			tested.fieldSource = "values2.source";
			tested.fieldTarget = "values2.target";
			Map<String, Object> values = new HashMap<String, Object>();
			Map<String, Object> values2 = new HashMap<String, Object>();
			values.put("values2", values2);
			values2
					.put("source",
							"<b>aa<b>bb<br>cdgh &lt;<div>text in div</div>\n &amp; then\n invalid <p> paragraph <pre>test\npre &amp;</pre>");
			tested.preprocessData(values);
			Assert.assertEquals("aabb cdgh < text in div & then invalid paragraph test\npre &",
					(String) values2.get("target"));
		}

	}
}
