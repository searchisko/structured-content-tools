/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.Map;

import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

/**
 * Content preprocessor which takes String value from source field, strip html tags from it, unescape html entities (
 * <code>&amp;lt;</code>, <code>&amp;gt;</code>, <code>&amp;amp;</code> atd) and store result to another or same target
 * field. Example of configuration for this preprocessor:
 * 
 * <pre>
 * { 
 *     "name"     : "HTML content to text description convertor",
 *     "class"    : "org.jboss.elasticsearch.tools.content.StripHtmlPreprocessor",
 *     "settings" : {
 *         "source_field"  : "content",
 *         "target_field"  : "description"
 *     } 
 * }
 * </pre>
 * 
 * Options are:
 * <ul>
 * <li><code>source_field</code> - source field in input data. Dot notation for nested values can be used here (see
 * {@link XContentMapValues#extractValue(String, Map)}).
 * <li><code>target_field</code> - target field in data to store mapped value into. Can be same as input field. Dot
 * notation can be used here for structure nesting.
 * </ul>
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 * @see StructuredContentPreprocessorFactory
 */
public class StripHtmlPreprocessor extends StructuredContentPreprocessorBase {

	protected static final String CFG_SOURCE_FIELD = "source_field";
	protected static final String CFG_TARGET_FIELD = "target_field";

	protected String fieldSource;
	protected String fieldTarget;

	@Override
	public void init(Map<String, Object> settings) throws SettingsException {
		if (settings == null) {
			throw new SettingsException("'settings' section is not defined for preprocessor " + name);
		}
		fieldSource = XContentMapValues.nodeStringValue(settings.get(CFG_SOURCE_FIELD), null);
		validateConfigurationStringNotEmpty(fieldSource, CFG_SOURCE_FIELD);
		fieldTarget = XContentMapValues.nodeStringValue(settings.get(CFG_TARGET_FIELD), null);
		validateConfigurationStringNotEmpty(fieldTarget, CFG_TARGET_FIELD);
	}

	@Override
	public Map<String, Object> preprocessData(Map<String, Object> data) {
		if (data == null)
			return null;

		Object v = null;
		if (fieldSource.contains(".")) {
			v = XContentMapValues.extractValue(fieldSource, data);
		} else {
			v = data.get(fieldSource);
		}

		if (v != null) {
			if (!(v instanceof String)) {
				logger.warn("value for field '" + fieldSource + "' is not String, so can't be processed by '" + name
						+ "' preprocessor");
			} else {
				String value = stripHtml(v.toString());
				StructureUtils.putValueIntoMapOfMaps(data, fieldTarget, value);
			}
		}
		return data;
	}

	protected String stripHtml(String value) {
		if (value == null || value.trim().isEmpty())
			return value;
		Document doc = Jsoup.parse(value);
		return doc.text();
	}

	public String getFieldSource() {
		return fieldSource;
	}

	public String getFieldTarget() {
		return fieldTarget;
	}

}
