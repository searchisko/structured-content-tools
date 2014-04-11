/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.Map;

import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

/**
 * Content preprocessor which allows to trim value from source field to the configured maximal length and store it to
 * another or same target field. White spaces at the begining and end are removed too. Example of configuration for this
 * preprocessor:
 * 
 * <pre>
 * { 
 *     "name"     : "Short description creator",
 *     "class"    : "org.jboss.elasticsearch.tools.content.TrimStringValuePreprocessor",
 *     "settings" : {
 *         "source_field"  : "fields.summary",
 *         "target_field"  : "dcp_description",
 *         "max_size" : 300
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
 * <li><code>max_size</code> - maximal size of string. Strings longer than this value are trimmed.
 * <li><code>source_bases</code> - list of fields in source data which are used as bases for trimming. If defined then
 * trimming is performed for each of this fields, <code>source_field</code> and <code>target_field</code> are resolved
 * relatively against this base. Base must provide object or list of objects.
 * </ul>
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 * @see StructuredContentPreprocessorFactory
 */
public class TrimStringValuePreprocessor extends StructuredContentPreprocessorWithSourceBasesBase<Object> {

	protected static final String CFG_SOURCE_FIELD = "source_field";
	protected static final String CFG_TARGET_FIELD = "target_field";
	protected static final String CFG_MAX_SIZE = "max_size";

	protected String fieldSource;
	protected String fieldTarget;
	protected int maxSize;

	@Override
	public void init(Map<String, Object> settings) throws SettingsException {
		super.init(settings);
		fieldSource = XContentMapValues.nodeStringValue(settings.get(CFG_SOURCE_FIELD), null);
		validateConfigurationStringNotEmpty(fieldSource, CFG_SOURCE_FIELD);
		fieldTarget = XContentMapValues.nodeStringValue(settings.get(CFG_TARGET_FIELD), null);
		validateConfigurationStringNotEmpty(fieldTarget, CFG_TARGET_FIELD);
		maxSize = readMandatoryIntegerConfigValue(settings, CFG_MAX_SIZE);
	}

	@Override
	protected void processOneSourceValue(Map<String, Object> data, Object context, String base) {
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
				String origValue = v.toString().trim();
				if (origValue.length() > maxSize) {
					if (maxSize > 4) {
						origValue = origValue.substring(0, maxSize - 3) + "...";
					} else {
						origValue = origValue.substring(0, maxSize);
					}
				}
				putTargetValue(data, origValue);
			}
		}
	}

	protected void putTargetValue(Map<String, Object> data, String value) {
		StructureUtils.putValueIntoMapOfMaps(data, fieldTarget, value);
	}

	@Override
	protected Object createContext() {
		return null;
	}

	public String getFieldSource() {
		return fieldSource;
	}

	public String getFieldTarget() {
		return fieldTarget;
	}

	public int getMaxSize() {
		return maxSize;
	}

}
