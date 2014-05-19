/*
 * JBoss, Home of Professional Open Source
 * Copyright 2014 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;

import junit.framework.Assert;

import org.elasticsearch.common.settings.SettingsException;
import org.junit.Test;

/**
 * Unit test for {@link ScriptingPreprocessor}
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class ScriptingPreprocessorTest {

	public static final void main(String... args) {
		ScriptEngineManager factory = new ScriptEngineManager();
		List<ScriptEngineFactory> l = factory.getEngineFactories();
		for (ScriptEngineFactory sef : l) {
			System.out.println(sef);
		}
	}

	@Test(expected = SettingsException.class)
	public void init_no_settings() {
		ScriptingPreprocessor tested = new ScriptingPreprocessor();
		tested.init("my preprocc", null, null);
	}

	@Test(expected = SettingsException.class)
	public void init_empty_settings() {
		ScriptingPreprocessor tested = new ScriptingPreprocessor();
		tested.init("my preprocc", null, new HashMap<String, Object>());
	}

	@Test(expected = SettingsException.class)
	public void init_script_engine_bad() {
		ScriptingPreprocessor tested = new ScriptingPreprocessor();
		Map<String, Object> settings = new HashMap<String, Object>();
		settings.put(ScriptingPreprocessor.CFG_SCRIPT_ENGINE_NAME_FIELD, "InvalidEngine");
		settings.put(ScriptingPreprocessor.CFG_SCRIPT_FIELD, "data.put('v2', data.get('v'))");
		tested.init("my preprocc", null, settings);
	}

	@Test(expected = SettingsException.class)
	public void init_script_null() {
		ScriptingPreprocessor tested = new ScriptingPreprocessor();
		Map<String, Object> settings = new HashMap<String, Object>();
		settings.put(ScriptingPreprocessor.CFG_SCRIPT_FIELD, null);
		tested.init("my preprocc", null, settings);
	}

	@Test(expected = SettingsException.class)
	public void init_script_empty() {
		ScriptingPreprocessor tested = new ScriptingPreprocessor();
		Map<String, Object> settings = new HashMap<String, Object>();
		settings.put(ScriptingPreprocessor.CFG_SCRIPT_FIELD, "  ");
		tested.init("my preprocc", null, settings);
	}

	@Test
	public void init_script_engine_default() {
		ScriptingPreprocessor tested = new ScriptingPreprocessor();
		Map<String, Object> settings = new HashMap<String, Object>();
		settings.put(ScriptingPreprocessor.CFG_SCRIPT_FIELD, "data.put('v2', data.get('v'))");
		tested.init("my preprocc", null, settings);
		Assert.assertNotNull(tested.engine);
		Assert.assertEquals("JavaScript", tested.getScriptEngineName());

		Assert.assertEquals("my preprocc", tested.getName());
		Assert.assertEquals("data.put('v2', data.get('v'))", tested.getScript());
	}

	@Test
	public void preprocessData_invalidScript() {
		ScriptingPreprocessor tested = new ScriptingPreprocessor();
		Map<String, Object> settings = new HashMap<String, Object>();
		settings.put(ScriptingPreprocessor.CFG_SCRIPT_ENGINE_NAME_FIELD, "JavaScript");
		settings.put(ScriptingPreprocessor.CFG_SCRIPT_FIELD, "data.v2 :!# data.v");

		tested.init("my preprocc", null, settings);

		PreprocessChainContextImpl context = new PreprocessChainContextImpl();
		Map<String, Object> data = new HashMap<String, Object>();
		tested.preprocessData(data, context);

		Assert.assertTrue(context.isWarning());
	}

	@Test
	public void preprocessData_validScript() {
		ScriptingPreprocessor tested = new ScriptingPreprocessor();
		Map<String, Object> settings = new HashMap<String, Object>();
		settings.put(ScriptingPreprocessor.CFG_SCRIPT_ENGINE_NAME_FIELD, "JavaScript");
		settings.put(ScriptingPreprocessor.CFG_SCRIPT_FIELD, "data.put('v2', data.get('v')); data.put('c','con');");

		tested.init("my preprocc", null, settings);

		PreprocessChainContextImpl context = new PreprocessChainContextImpl();
		Map<String, Object> data = new HashMap<String, Object>();
		data.put("v", "val");
		tested.preprocessData(data, context);

		Assert.assertFalse("no warnings expected but is " + context, context.isWarning());
		Assert.assertEquals("val", data.get("v"));
		Assert.assertEquals("val", data.get("v2"));
		Assert.assertEquals("con", data.get("c"));
	}

}
