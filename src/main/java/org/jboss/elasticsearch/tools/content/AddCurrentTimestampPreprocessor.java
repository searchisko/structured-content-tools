/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.Map;

import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

/**
 * Content preprocessor which allows to add current timestamp to some target field. Example of configuration for this
 * preprocessor:
 * 
 * <pre>
 * { 
 *     "name"     : "Updated field setter",
 *     "class"    : "org.jboss.elasticsearch.tools.content.AddCurrentTimestampPreprocessor",
 *     "settings" : {
 *         "field"  : "updated"
 *     } 
 * }
 * </pre>
 * 
 * Options are:
 * <ul>
 * <li><code>field</code> - target field in data to store current timestamp into. Value is String with ISO formated
 * current date time value, eg. <code>2012-09-17T15:56:52.383+02:00</code>
 * </ul>
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 * @see StructuredContentPreprocessorFactory
 */
public class AddCurrentTimestampPreprocessor extends StructuredContentPreprocessorBase {

  protected static final String CFG_FIELD = "field";

  String field;

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
    StructureUtils.putValueIntoMapOfMaps(data, field, ISODateTimeFormat.dateTime().print(System.currentTimeMillis()));
    return data;
  }

}
