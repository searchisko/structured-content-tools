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

import junit.framework.Assert;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.SettingsException;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit test for {@link RemoveMultipleFieldsPreprocessor}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class RemoveMultipleFieldsPreprocessorTest {

	@Test(expected = SettingsException.class)
	public void init_settings_null() {
		RemoveMultipleFieldsPreprocessor tested = getTested();
		tested.init("Test mapper", null, null);
	}

	@Test(expected = SettingsException.class)
	public void init_missing_fields() {
		RemoveMultipleFieldsPreprocessor tested = getTested();

		Map<String, Object> settings = new HashMap<String, Object>();
		tested.init("Test mapper", null, settings);
	}

	@Test(expected = SettingsException.class)
	public void init_empty_fields() {
		RemoveMultipleFieldsPreprocessor tested = getTested();

		Map<String, Object> settings = new HashMap<String, Object>();
		List<String> fields = new ArrayList<String>();
		settings.put(RemoveMultipleFieldsPreprocessor.CFG_FIELDS, fields);

		tested.init("Test mapper", null, settings);
	}

	@Test(expected = SettingsException.class)
	public void init_empty_fields_string() {
		RemoveMultipleFieldsPreprocessor tested = getTested();

		Map<String, Object> settings = new HashMap<String, Object>();
		settings.put(RemoveMultipleFieldsPreprocessor.CFG_FIELDS, "  ");

		tested.init("Test mapper", null, settings);
	}

	@Test
	public void init_nobases_simplefields() {
		RemoveMultipleFieldsPreprocessor tested = getTested();
		Map<String, Object> settings = new HashMap<String, Object>();
		settings.put(RemoveMultipleFieldsPreprocessor.CFG_FIELDS, "sourcefield");

		tested.init("Test mapper", null, settings);
		Assert.assertEquals("Test mapper", tested.getName());
		Assert.assertEquals(1, tested.getFields().size());
		Assert.assertEquals("sourcefield", tested.getFields().get(0));
		Assert.assertNull(tested.getSourceBases());
	}

	@Test
	public void init_bases_morefields() {
		RemoveMultipleFieldsPreprocessor tested = getTested();

		Map<String, Object> settings = new HashMap<String, Object>();
		List<String> fields = new ArrayList<String>();
		fields.add("sourcefield");
		fields.add("sourcefield2");
		settings.put(RemoveMultipleFieldsPreprocessor.CFG_FIELDS, fields);

		List<String> sb = new ArrayList<String>();
		sb.add("sb");
		settings.put(RemoveMultipleFieldsPreprocessor.CFG_source_bases, sb);

		tested.init("Test mapper", null, settings);
		Assert.assertEquals("Test mapper", tested.getName());
		Assert.assertEquals(2, tested.getFields().size());
		Assert.assertEquals("sourcefield", tested.getFields().get(0));
		Assert.assertEquals("sourcefield2", tested.getFields().get(1));
		Assert.assertEquals(1, tested.getSourceBases().size());
		Assert.assertEquals("sb", tested.getSourceBases().get(0));
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void preprocessData_nobases() {
		RemoveMultipleFieldsPreprocessor tested = getTested();
		tested.fields = new ArrayList<String>();
		tested.fields.add("removeme_nonexisting");
		tested.fields.add("removeme_simplevalue");
		tested.fields.add("removeme_map");
		tested.fields.add("removeme_list");
		tested.fields.add("substructure.removeme");

		Map<String, Object> data = new HashMap<String, Object>();
		data.put("removeme_simplevalue", "Ahoj");
		data.put("keepme", "Keep this value in data");
		data.put("removeme_map", new HashMap<Object, Object>());
		data.put("removeme_list", new ArrayList<Object>());

		Map<String, Object> substructure = new HashMap<String, Object>();
		data.put("substructure", substructure);
		substructure.put("removeme", "Ahoj2");
		substructure.put("keepme", "keep me");

		tested.preprocessData(data, null);

		Assert.assertEquals(2, data.size());
		Assert.assertEquals("Keep this value in data", data.get("keepme"));
		Assert.assertEquals(1, ((Map) data.get("substructure")).size());
		Assert.assertEquals("keep me", ((Map) data.get("substructure")).get("keepme"));
	}

	@SuppressWarnings({ "unused", "rawtypes" })
	@Test
	public void preprocessData_bases() {
		RemoveMultipleFieldsPreprocessor tested = getTested();
		tested.fields = new ArrayList<String>();
		tested.fields.add("removeme_nonexisting");
		tested.fields.add("removeme_simplevalue");
		tested.fields.add("removeme_map");
		tested.fields.add("removeme_list");
		tested.fields.add("substructure.removeme");

		tested.sourceBases = new ArrayList<String>();
		tested.sourceBases.add("mybase");
		tested.sourceBases.add("base2");

		Map<String, Object> data = new HashMap<String, Object>();

		Map<String, Object> dataInBase = new HashMap<String, Object>();
		data.put("mybase", dataInBase);

		dataInBase.put("removeme_simplevalue", "Ahoj");
		dataInBase.put("keepme", "Keep this value in data");
		dataInBase.put("removeme_map", new HashMap<Object, Object>());
		dataInBase.put("removeme_list", new ArrayList<Object>());

		Map<String, Object> substructure = new HashMap<String, Object>();
		dataInBase.put("substructure", substructure);
		substructure.put("removeme", "Ahoj2");
		substructure.put("keepme", "keep me");

		List<Object> dataInBase2 = new ArrayList<Object>();
		data.put("base2", dataInBase);

		tested.preprocessData(data, null);

		Assert.assertEquals(2, data.size());
		Assert.assertEquals(2, dataInBase.size());
		Assert.assertEquals("Keep this value in data", dataInBase.get("keepme"));
		Assert.assertEquals(1, ((Map) dataInBase.get("substructure")).size());
		Assert.assertEquals("keep me", ((Map) dataInBase.get("substructure")).get("keepme"));

	}

	private RemoveMultipleFieldsPreprocessor getTested() {
		RemoveMultipleFieldsPreprocessor tested = new RemoveMultipleFieldsPreprocessor();
		tested.logger = Mockito.mock(ESLogger.class);
		return tested;
	}

}
