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
 * Content preprocessor which allows to add value to some target field. Value can be constant or contain pattern with
 * keys for replacement with other values from data structure. Example of configuration for this preprocessor:
 * 
 * <pre>
 * { 
 *     "name"     : "Value filler",
 *     "class"    : "org.jboss.elasticsearch.tools.content.AddValuePreprocessor",
 *     "settings" : {
 *         "field"  : "full_name",
 *         "value"  : "My name is {user.name} {user.surname}."
 *     } 
 * }
 * </pre>
 * 
 * Options are:
 * <ul>
 * <li><code>field</code> - target field in data to store value into. Dot notation can be used here for structure
 * nesting.
 * <li><code>value</code> - value to be stored into field. Rewrites value in field if there is any existing.
 * <code>null</code> is added into field if value is not defined! This value can contain pattern for keys replacement
 * from other values in data. Keys are enclosed in curly braces, dot notation for deeper nesting may be used in keys.
 * Example of value with replacement keys '<code>My name is {user.name} and surname is {user.surname}.</code>'. If value
 * for some key is not found in data structure then empty string is used instead. If replacement value in data is not
 * String then <code>toString()</code> is used to convert it.
 * </ul>
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 * @see StructuredContentPreprocessorFactory
 * @see ValueUtils#processStringValuePatternReplacement(String, Map, Object)
 */
public class AddValuePreprocessor extends StructuredContentPreprocessorBase {

  protected static final String CFG_FIELD = "field";
  protected static final String CFG_VALUE = "value";

  protected String field;
  protected Object value = null;

  @Override
  public void init(Map<String, Object> settings) throws SettingsException {
    if (settings == null) {
      throw new SettingsException("'settings' section is not defined for preprocessor " + name);
    }
    field = XContentMapValues.nodeStringValue(settings.get(CFG_FIELD), null);
    validateConfigurationStringNotEmpty(field, CFG_FIELD);
    value = settings.get(CFG_VALUE);
  }

  @Override
  public Map<String, Object> preprocessData(Map<String, Object> data) {
    if (data == null)
      return null;
    if (value != null && (value instanceof String) && ((String) value).contains("{")) {
      value = ValueUtils.processStringValuePatternReplacement((String) value, data, null);
    }
    StructureUtils.putValueIntoMapOfMaps(data, field, value);
    return data;
  }

}
