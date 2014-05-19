/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

/**
 * Content preprocessor which apply Regular Expression (see {@link Pattern}) to the source String value, and copy
 * content of defined <a href="http://docs.oracle.com/javase/tutorial/essential/regex/groups.html">Capturing Groups</a>
 * to defined target fields. Example of configuration for this preprocessor:
 * 
 * <pre>
 * { 
 *     "name"     : "Name extractor",
 *     "class"    : "org.jboss.elasticsearch.tools.content.RegExpCapturingGroupPreprocessor",
 *     "settings" : {
 *         "source_field"  : "fields.updated",
 *         "pattern"       : "my name is (.*)",
 *         "result_mapping" : {
 *           0 : "target_field_group_0",
 *           1 : "target_field_group_1"
 *         }
 *     } 
 * }
 * </pre>
 * 
 * Options are:
 * <ul>
 * <li><code>source_field</code> - source field in input data. Dot notation for nested values can be used here (see
 * {@link XContentMapValues#extractValue(String, Map)}).
 * <li><code>pattern</code> - regular expression pattern to be used. Should contain some Capturing Groups!
 * <li><code>result_mapping</code> - mapping of values of Capturing Groups found in input value into target fields.
 * Target field can be same as input field. Dot notation can be used here for structure nesting. Target fields are not
 * rewritten if pattern doesn't match (but warning is generated in this case).
 * <li><code>source_bases</code> - list of fields in source data which are used as bases for extraction. If defined then
 * extraction is performed for each of this fields, <code>source_field</code> and <code>target_field_xx</code> are
 * resolved relatively against this base. Base must provide object or list of objects.
 * </ul>
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 * @see StructuredContentPreprocessorFactory
 * @see Pattern
 * @see Matcher#matches()
 * @see Matcher#group(int)
 */
public class RegExpCapturingGroupPreprocessor extends StructuredContentPreprocessorWithSourceBasesBase<Object> {

	protected static final String CFG_SOURCE_FIELD = "source_field";
	protected static final String CFG_PATTERN = "pattern";
	protected static final String CFG_RESULT_MAPPING = "result_mapping";

	protected String fieldSource;
	protected Pattern patternCompiled;
	protected Map<Object, String> resultMapping;

	@SuppressWarnings("unchecked")
	@Override
	public void init(Map<String, Object> settings) throws SettingsException {
		super.init(settings);
		fieldSource = XContentMapValues.nodeStringValue(settings.get(CFG_SOURCE_FIELD), null);
		validateConfigurationStringNotEmpty(fieldSource, CFG_SOURCE_FIELD);
		String pattern = XContentMapValues.nodeStringValue(settings.get(CFG_PATTERN), null);
		validateConfigurationStringNotEmpty(pattern, CFG_PATTERN);
		try {
			patternCompiled = Pattern.compile(pattern);
		} catch (PatternSyntaxException e) {
			throw new SettingsException("'settings/" + CFG_PATTERN + "' configuration value for '" + name
					+ "' preprocessor is invalid: " + e.getMessage());
		}
		try {
			resultMapping = (Map<Object, String>) settings.get(CFG_RESULT_MAPPING);
			validateResultMappingConfiguration(resultMapping, CFG_RESULT_MAPPING);
		} catch (ClassCastException e) {
			throw new SettingsException("'settings/" + CFG_RESULT_MAPPING + "' configuration value for '" + name
					+ "' preprocessor is invalid");
		}
	}

	/**
	 * Validate result mapping configuration part.
	 * 
	 * @param value to check
	 * @param configFieldName name of field in preprocessor settings structure. Used for error message.
	 * @throws SettingsException thrown if value is not valid
	 */
	protected void validateResultMappingConfiguration(Map<Object, String> value, String configFieldName)
			throws SettingsException {
		if (value == null || value.isEmpty()) {
			throw new SettingsException("Missing or empty 'settings/" + configFieldName + "' configuration object for '"
					+ name + "' preprocessor");
		}
		for (Object index : value.keySet()) {
			if (ValueUtils.isEmpty(index)) {
				throw new SettingsException("Missing or empty index in 'settings/" + configFieldName + "' configuration for '"
						+ name + "' preprocessor");
			}
			boolean isnumber = false;
			if (index instanceof Number) {
				isnumber = true;
			} else if (index instanceof String) {
				try {
					new Integer((String) index);
					isnumber = true;
				} catch (NumberFormatException e) {

				}
			}
			if (!isnumber) {
				throw new SettingsException("Index must be a number in 'settings/" + configFieldName + "' configuration for '"
						+ name + "' preprocessor");
			}

			try {
				if (ValueUtils.isEmpty((String) value.get(index))) {
					throw new SettingsException("Missing or empty value in 'settings/" + configFieldName + "/" + index
							+ "' configuration for '" + name + "' preprocessor");
				}
			} catch (ClassCastException e) {
				throw new SettingsException("Value for 'settings/" + configFieldName + "/" + index + "' configuration for '"
						+ name + "' preprocessor must be String");
			}
		}
	}

	@Override
	protected void processOneSourceValue(Map<String, Object> data, Object context, String base,
			PreprocessChainContext chainContext) {
		Object v = null;
		if (fieldSource.contains(".")) {
			v = XContentMapValues.extractValue(fieldSource, data);
		} else {
			v = data.get(fieldSource);
		}

		if (v != null) {
			if (v instanceof String) {
				String vs = (String) v;
				Matcher m = patternCompiled.matcher(vs);
				if (m.matches()) {
					for (Object index : resultMapping.keySet()) {
						int i = -1;
						if (index instanceof Number) {
							i = ((Number) index).intValue();
						} else {
							i = Integer.parseInt(index.toString());
						}
						if (i >= 0 && i <= m.groupCount()) {
							try {
								StructureUtils.putValueIntoMapOfMaps(data, resultMapping.get(index), m.group(i));
							} catch (IllegalStateException e) {
								String warningMessage = "No match found for Capturing group " + i + " in value '" + vs
										+ "' from field '" + fieldSource + "'";
								addDataWarning(chainContext, warningMessage);
								logger.debug(warningMessage);
							}
						}
					}
				} else {
					String warningMessage = "value '" + vs + "' for field '" + fieldSource
							+ "' do not match pattern, so can't be processed";
					addDataWarning(chainContext, warningMessage);
					logger.debug(warningMessage);
				}
			} else {
				String warningMessage = "value for field '" + fieldSource + "' is not String but is " + v.getClass().getName()
						+ ", so can't be processed";
				addDataWarning(chainContext, warningMessage);
				logger.debug(warningMessage);
			}
		}
	}

	@Override
	protected Object createContext(Map<String, Object> data) {
		return null;
	}

	public String getFieldSource() {
		return fieldSource;
	}

	public Map<Object, String> getResultMapping() {
		return resultMapping;
	}

	public String getPattern() {
		return patternCompiled != null ? patternCompiled.pattern() : null;
	}

}
