/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.Collection;
import java.util.Map;

import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

/**
 * Content preprocessor which allows to validate some field for 'required' condition. <code>InvalidDataException</code>
 * is thrown in case of invalid data. Required condition means:
 * <ul>
 * <li>field value is not <code>null<code>
 * <li> if field value is <code>String</code> then it's trimmed and checked not to be empty
 * <li>if field value is some <code>Collection</code> then it's checked not to be empty
 * </ul>
 * <p>
 * Example of configuration for this preprocessor:
 * 
 * <pre>
 * { 
 *     "name"     : "Required validator for project field",
 *     "class"    : "org.jboss.elasticsearch.tools.content.RequiredValidatorPreprocessor",
 *     "settings" : {
 *         "field"  : "project"
 *     } 
 * }
 * </pre>
 * 
 * Options are:
 * <ul>
 * <li><code>field</code> - field in data to be validated
 * </ul>
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 * @see StructuredContentPreprocessorFactory
 */
public class RequiredValidatorPreprocessor extends StructuredContentPreprocessorBase {

	protected static final String CFG_FIELD = "field";

	protected String field;

	@Override
	public void init(Map<String, Object> settings) throws SettingsException {
		if (settings == null) {
			throw new SettingsException("'settings' section is not defined for preprocessor " + name);
		}
		field = XContentMapValues.nodeStringValue(settings.get(CFG_FIELD), null);
		validateConfigurationStringNotEmpty(field, CFG_FIELD);
	}

	@Override
	public Map<String, Object> preprocessData(Map<String, Object> data) {
		if (data == null)
			return null;
		Object sourceData = XContentMapValues.extractValue(field, data);
		if (sourceData == null) {
			throw new InvalidDataException("Field " + field + " is required");
		} else if (sourceData instanceof String) {
			String s = ((String) sourceData).trim();
			if (s.isEmpty()) {
				throw new InvalidDataException("Field " + field + " is required not to be empty od blank string");
			}
		} else if (sourceData instanceof Collection) {
			Collection<?> s = (Collection<?>) sourceData;
			if (s.isEmpty()) {
				throw new InvalidDataException("Field " + field + " is required not to be empty collection");
			}
		}
		return data;
	}

}
