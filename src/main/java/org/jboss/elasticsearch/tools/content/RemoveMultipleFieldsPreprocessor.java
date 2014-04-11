/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.settings.SettingsException;

/**
 * Content preprocessor which allows to remove multiple fields from data structure. Removed field may be simple value,
 * list or object. No problem if field is not present in data. Example of configuration for this preprocessor:
 * 
 * <pre>
 * { 
 *     "name"     : "Values remover",
 *     "class"    : "org.jboss.elasticsearch.tools.content.RemoveMultipleFieldsPreprocessor",
 *     "settings" : {
 *         "fields"  : ["private","user.password"]
 *     } 
 * }
 * </pre>
 * 
 * Settings contains map with definition of fields to be added:
 * <ul>
 * <li><code>fields</code> - Array with names of fields to remove. Dot notation can be used here for structure nesting.
 * <li><code>source_bases</code> - list of fields in source data which are used as bases for removing. If defined then
 * removing is performed for each of this fields, <code>fields</code> are resolved relatively against this base. Base
 * must provide object or list of objects.
 * </ul>
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 * @see StructuredContentPreprocessorFactory
 */
public class RemoveMultipleFieldsPreprocessor extends StructuredContentPreprocessorWithSourceBasesBase<Object> {

	protected static final String CFG_FIELDS = "fields";

	protected List<String> fields;

	@SuppressWarnings("unchecked")
	@Override
	public void init(Map<String, Object> settings) throws SettingsException {
		super.init(settings);
		Object o = settings.get(CFG_FIELDS);
		if (o instanceof List) {
			fields = (List<String>) o;
		} else if (o instanceof String) {
			String s = ValueUtils.trimToNull((String) o);
			if (s != null) {
				fields = new ArrayList<String>();
				fields.add(s);
			}
		}
		if (fields == null || fields.isEmpty()) {
			throw new SettingsException("Missing, empty or bad 'settings/" + CFG_FIELDS + "' configuration value for '"
					+ name + "' preprocessor");
		}
	}

	@Override
	protected void processOneSourceValue(Map<String, Object> data, Object context, String base,
			PreprocessChainContext chainContext) {
		for (String key : fields) {
			StructureUtils.removeValueFromMapOfMaps(data, key);
		}
	}

	@Override
	protected Object createContext() {
		return null;
	}

	/**
	 * @return configured fields for remove
	 */
	public List<String> getFields() {
		return fields;
	}

}
