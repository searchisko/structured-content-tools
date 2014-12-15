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
 *         "source_fields" : ["fields.author","fields.reporter","fields.updater"],
 *         "deep_copy" : "false"
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
 * <li><code>deep_copy</code> - default value "false". This parameter specifies whether a complete copy of the whole
 * source_fields structure should be done. In default case the copy of data will be done only by reference. Switching
 * this parameter to true is especially useful when a person is collecting more complicated values like Lists and Maps
 * with a plan to modify those without modifying the source instances.
 * </ul>
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 * @author Ryszard Kozmik (rkozmik at redhat dot com)
 * @see StructuredContentPreprocessorFactory
 */
public class ValuesCollectingPreprocessor extends StructuredContentPreprocessorBase {

	protected static final String CFG_SOURCE_FIELDS = "source_fields";
	protected static final String CFG_TARGET_FIELD = "target_field";
	protected static final String CFG_DEEP_COPY = "deep_copy";

	protected String fieldTarget;
	protected List<String> fieldsSource;
	protected boolean fieldDeepCopy;

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
		String fieldDeepCopyStr = XContentMapValues.nodeStringValue(settings.get(CFG_DEEP_COPY), "false" );
		fieldDeepCopy = fieldDeepCopyStr.compareTo("true")==0 ? true : false;
	}

	@Override
	public Map<String, Object> preprocessData(Map<String, Object> data, PreprocessChainContext chainContext) {
		if (data == null)
			return null;
		Set<Object> vals = new HashSet<Object>();

		for (String sourceField : fieldsSource) {
			if (ValueUtils.isEmpty(sourceField))
				continue;
			Object v = XContentMapValues.extractValue(sourceField, data);
			collectValue(vals, v);
		}
		if (vals != null && !vals.isEmpty()) {
			StructureUtils.putValueIntoMapOfMaps(data, fieldTarget, new ArrayList<Object>(vals));
		} else {
			StructureUtils.putValueIntoMapOfMaps(data, fieldTarget, null);
		}
		return data;
	}

	@SuppressWarnings("unchecked")
	private void collectValue(Set<Object> values, Object value) {
		if (value != null) {
			if (value instanceof Collection) {
				for (Object o : ((Collection<Object>) value))
					if ( fieldDeepCopy ) {
						collectValue(values, StructureUtils.getADeepStructureCopy(o));
					} else {
						collectValue(values, o);
					}
			} else {
				if ( fieldDeepCopy ) {
					values.add(StructureUtils.getADeepStructureCopy(value));
				} else {
					values.add(value);
				}
			}
		}
	}

	public String getFieldTarget() {
		return fieldTarget;
	}

	public List<String> getFieldsSource() {
		return fieldsSource;
	}

}
