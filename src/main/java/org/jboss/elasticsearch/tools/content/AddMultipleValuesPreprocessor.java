/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.Map;

import org.elasticsearch.common.settings.SettingsException;

/**
 * Content preprocessor which allows to add multiple values to some target fields. Value can be constant or contain
 * pattern with keys for replacement with other values from data structure. Example of configuration for this
 * preprocessor:
 * 
 * <pre>
 * { 
 *     "name"     : "Values filler",
 *     "class"    : "org.jboss.elasticsearch.tools.content.AddMultipleValuesPreprocessor",
 *     "settings" : {
 *         "field_1"  : "value_1",
 *         "field_2"  : "My name is {user.name} {user.surname}."
 *     } 
 * }
 * </pre>
 * 
 * Settings contains map with definition of fields to be added:
 * <ul>
 * <li>key in map is target field in data to store value into. Dot notation can be used here for structure nesting.
 * <li>value in map is value to be stored into field. Rewrites value in field if there is any existing. This value can
 * contain pattern for keys replacement from other values in data. Keys are enclosed in curly braces, dot notation for
 * deeper nesting may be used in keys. Example of value with replacement keys '
 * <code>My name is {user.name} and surname is {user.surname}.</code>'. If value for some key is not found in data
 * structure then empty string is used instead. If replacement value in data is not String then <code>toString()</code>
 * is used to convert it.
 * </ul>
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 * @see StructuredContentPreprocessorFactory
 */
public class AddMultipleValuesPreprocessor extends StructuredContentPreprocessorBase {

	protected Map<String, Object> fields;

	@Override
	public void init(Map<String, Object> settings) throws SettingsException {
		if (settings == null) {
			throw new SettingsException("'settings' section is not defined for preprocessor " + name);
		}
		fields = settings;
	}

	@Override
	public Map<String, Object> preprocessData(Map<String, Object> data) {
		if (data == null)
			return null;
		for (String key : fields.keySet()) {
			Object value = fields.get(key);
			if (value != null && (value instanceof String) && ((String) value).contains("{")) {
				value = ValueUtils.processStringValuePatternReplacement((String) value, data, null);
			}
			StructureUtils.putValueIntoMapOfMaps(data, key, value);
		}
		return data;
	}

	public Map<String, Object> getFields() {
		return fields;
	}

}
