/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.Map;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

/**
 * Content preprocessor which runs script by some <a
 * href="http://docs.oracle.com/javase/6/docs/technotes/guides/scripting/">Java Scripting API</a> provided engine to
 * manipulate processed data. Example of configuration for this preprocessor:
 * 
 * <pre>
 * { 
 *     "name"     : "Script runner",
 *     "class"    : "org.jboss.elasticsearch.tools.content.ScriptingPreprocessor",
 *     "settings" : {
 *         "script_engine_name"  : "JavaScript",
 *         "script"  : "data.put('v2', data.get('v')); data.put('c','con');"
 *     } 
 * }
 * </pre>
 * 
 * Options are:
 * <ul>
 * <li><code>script_engine_name</code> - optional name of scripting engine. Defaults to <code>JavaScript</code>.
 * <li><code>script</code> - script code to run, {@link ScriptEngine#eval(String)} method is used. You can use variable
 * called <code>data</code> to manipulate processed data.
 * </ul>
 * <p>
 * <b>Note</b> that performance of this preprocessor depends on performance of scripting engine. It is always better to
 * use other existing specialized preprocessors for simple tasks like constant values setting, simple value copy etc.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 * @see StructuredContentPreprocessorFactory
 */
public class ScriptingPreprocessor extends StructuredContentPreprocessorBase {

	protected static final String CFG_SCRIPT_ENGINE_NAME_FIELD = "script_engine_name";
	protected static final String CFG_SCRIPT_FIELD = "script";

	protected String scriptEngineName;
	protected static ScriptEngineManager factory = new ScriptEngineManager();
	protected String script;
	protected ScriptEngine engine;

	@Override
	public void init(Map<String, Object> settings) throws SettingsException {
		if (settings == null) {
			throw new SettingsException("'settings' section is not defined for preprocessor " + name);
		}
		script = XContentMapValues.nodeStringValue(settings.get(CFG_SCRIPT_FIELD), null);
		validateConfigurationStringNotEmpty(script, CFG_SCRIPT_FIELD);
		scriptEngineName = XContentMapValues.nodeStringValue(settings.get(CFG_SCRIPT_ENGINE_NAME_FIELD), "JavaScript");
		validateConfigurationStringNotEmpty(scriptEngineName, CFG_SCRIPT_ENGINE_NAME_FIELD);
		engine = factory.getEngineByName(scriptEngineName);
		if (engine == null) {
			throw new SettingsException("No scripting engine is available for name " + scriptEngineName
					+ " for preprocessor " + name);
		}
	}

	@Override
	public Map<String, Object> preprocessData(Map<String, Object> data, PreprocessChainContext chainContext) {

		ScriptContext newContext = new SimpleScriptContext();
		Bindings engineScope = newContext.getBindings(ScriptContext.ENGINE_SCOPE);
		engineScope.put("data", data);

		try {
			synchronized (engine) {
				engine.eval(script, newContext);
			}
		} catch (ScriptException e) {
			String warningMessage = "Script execution failed: " + e.getMessage();
			addDataWarning(chainContext, warningMessage);
			logger.debug(warningMessage);
		}
		return data;
	}

	public String getScriptEngineName() {
		return scriptEngineName;
	}

	public String getScript() {
		return script;
	}

}
