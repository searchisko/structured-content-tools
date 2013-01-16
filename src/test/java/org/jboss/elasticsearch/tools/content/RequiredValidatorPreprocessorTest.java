/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
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
 * Unit test for {@link RequiredValidatorPreprocessor}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class RequiredValidatorPreprocessorTest {

	@Test
	public void init_settingerrors() {
		RequiredValidatorPreprocessor tested = new RequiredValidatorPreprocessor();
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

		settings.put(RequiredValidatorPreprocessor.CFG_FIELD, " ");
		try {
			tested.init("Test mapper", client, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals("Missing or empty 'settings/field' configuration value for 'Test mapper' preprocessor",
					e.getMessage());
		}

		// case - no more mandatory setting fields
		settings.put(RequiredValidatorPreprocessor.CFG_FIELD, "field");
		tested.init("Test mapper", client, settings);
	}

	@Test
	public void init() {
		RequiredValidatorPreprocessor tested = new RequiredValidatorPreprocessor();
		Client client = Mockito.mock(Client.class);

		// case - null value
		{
			Map<String, Object> settings = new HashMap<String, Object>();
			settings.put(RequiredValidatorPreprocessor.CFG_FIELD, "source");

			tested.init("Test mapper", client, settings);
			Assert.assertEquals("Test mapper", tested.getName());
			Assert.assertEquals(client, tested.client);
			Assert.assertEquals("source", tested.field);
		}
	}

	@Test
	public void preprocessData() {
		RequiredValidatorPreprocessor tested = new RequiredValidatorPreprocessor();
		tested.field = "my_field";

		// case - not NPE
		tested.preprocessData(null);

		Map<String, Object> values = new HashMap<String, Object>();
		// case - value is null
		try {
			tested.preprocessData(values);
			Assert.fail("InvalidDataException expected");
		} catch (InvalidDataException e) {
			// OK
		}

		// cases - matched condition
		values.put(tested.field, 10);
		tested.preprocessData(values);

		values.put(tested.field, "string");
		tested.preprocessData(values);

		// case - fail due empty String
		try {
			values.put(tested.field, "");
			tested.preprocessData(values);
			Assert.fail("InvalidDataException expected");
		} catch (InvalidDataException e) {
			// OK
		}

		// case - fail due blank String
		try {
			values.put(tested.field, "    ");
			tested.preprocessData(values);
			Assert.fail("InvalidDataException expected");
		} catch (InvalidDataException e) {
			// OK
		}

		// case - fail due empty collection
		List<Object> l = new ArrayList<Object>();
		values.put(tested.field, l);
		try {
			tested.preprocessData(values);
			Assert.fail("InvalidDataException expected");
		} catch (InvalidDataException e) {
			// OK
		}

		// case - ok for nonempty collection
		l.add("something");
		tested.preprocessData(values);
	}
}
