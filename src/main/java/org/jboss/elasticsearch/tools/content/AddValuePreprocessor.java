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
 * Content preprocessor which allows to add value to some target field. Example of configuration for this preprocessor:
 * 
 * <pre>
 * { 
 *     "name"     : "Status Normalizer",
 *     "class"    : "org.jboss.elasticsearch.tools.content.AddValuePreprocessor",
 *     "settings" : {
 *         "field"  : "project",
 *         "value"  : "ORG"
 *     } 
 * }
 * </pre>
 * 
 * Options are:
 * <ul>
 * <li><code>field</code> - target field in data to store value into. Dot notation can be used here for structure
 * nesting.
 * <li><code>value</code> - value to be stored into field. Rewrites value in field if there is any existing.
 * <code>null</code> is added into field if not defined!
 * </ul>
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 * @see StructuredContentPreprocessorFactory
 */
public class AddValuePreprocessor extends StructuredContentPreprocessorBase {

  protected static final String CFG_FIELD = "field";
  protected static final String CFG_VALUE = "value";

  String field;
  Object value = null;

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
    StructureUtils.putValueIntoMapOfMaps(data, field, value);
    return data;
  }

}
