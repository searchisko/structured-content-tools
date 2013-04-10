/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

/**
 * Content preprocessor which collects values from multiple source fields and store them as List in target field.
 * Duplicities are removed during collecting. Example of configuration for this preprocessor:
 * 
 * <pre>
 * { 
 *     "name"     : "Contributors collector",
 *     "class"    : "org.jboss.elasticsearch.tools.content.ValuesCollectingPreprocessor",
 *     "settings" : {
 *         "target_field"  : "contributors",
 *         "source_fields" : ["fields.author","fields.reporter","fields.updater"]
 *     } 
 * }
 * </pre>
 * 
 * Options are:
 * <ul>
 * <li><code>source_fields</code> - array with source fields in input data. Dot notation for nested values can be used
 * here, lists can be in path - see {@link XContentMapValues#extractValue(String, Map)}.
 * <li><code>target_field</code> - target field in data to store final list. Dot notation can be used here for structure
 * nesting. If collected list is empty nothing is stored here.
 * </ul>
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 * @see StructuredContentPreprocessorFactory
 */
public class ValuesCollectingPreprocessor extends StructuredContentPreprocessorBase {

	protected static final String CFG_SOURCE_FIELDS = "source_fields";
	protected static final String CFG_TARGET_FIELD = "target_field";

	protected String fieldTarget;
	protected List<String> fieldsSource;

	@SuppressWarnings("unchecked")
	@Override
	public void init(Map<String, Object> settings) throws SettingsException {
		if (settings == null) {
			throw new SettingsException("'settings' section is not defined for preprocessor " + name);
		}
		fieldsSource = ((List<String>) settings.get(CFG_SOURCE_FIELDS));
		validateConfigurationObjectNotEmpty(fieldsSource, CFG_SOURCE_FIELDS);
		fieldTarget = XContentMapValues.nodeStringValue(settings.get(CFG_TARGET_FIELD), null);
		validateConfigurationStringNotEmpty(fieldTarget, CFG_TARGET_FIELD);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> preprocessData(Map<String, Object> data) {
		if (data == null)
			return null;
		Set<Object> vals = new HashSet<Object>();

		for (String sourceField : fieldsSource) {
			if (ValueUtils.isEmpty(sourceField))
				continue;
			Object v = XContentMapValues.extractValue(sourceField, data);
			if (v != null) {
				if (v instanceof Collection) {
					vals.addAll((Collection<Object>) v);
				} else {
					vals.add(v);
				}
			}
		}
		if (vals != null && !vals.isEmpty()) {
			StructureUtils.putValueIntoMapOfMaps(data, fieldTarget, new ArrayList<Object>(vals));
		} else {
			StructureUtils.putValueIntoMapOfMaps(data, fieldTarget, null);
		}
		return data;
	}

	public String getFieldTarget() {
		return fieldTarget;
	}

	public List<String> getFieldsSource() {
		return fieldsSource;
	}

}
