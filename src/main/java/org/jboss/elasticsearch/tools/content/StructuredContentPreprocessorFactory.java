/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.client.Client;

/**
 * Factory class used to construct {@link StructuredContentPreprocessor} instances from configuration.
 * <p>
 * Preprocessors may be created from configuration with next structure:
 * 
 * <pre>
 * { 
 *     "name"     : "Status Normalizer",
 *     "class"    : "org.jboss.elasticsearch.river.jira.preproc.StatusNormalizer",
 *     "settings" : {
 *         "some_setting_1" : "value1",
 *         "some_setting_2" : "value2"
 *     }
 * }
 * </pre>
 * 
 * Class defined in <code>class</code> element must implement {@link StructuredContentPreprocessor} interface. Name of
 * preprocessor from <code>name</code> element and configuration structure stored in <code>settings</code> element (must
 * be <code>Map<String, Object></code>) is then passed to the
 * {@link StructuredContentPreprocessor#init(String, Client, Map)} method.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class StructuredContentPreprocessorFactory {

  public static final String CFG_SETTINGS = "settings";
	public static final String CFG_CLASS = "class";
	public static final String CFG_NAME = "name";

	/**
   * Create preprocessor from configuration described in this class's javadoc.
   * 
   * @param preprocessorConfig configuration structure in Map of Maps
   * @param client ES client to be passed to the preprocessor.
   * @return instance
   * @throws IllegalArgumentException if something is wrong and preprocessor can't be instantiated.
   */
  @SuppressWarnings("unchecked")
  public static StructuredContentPreprocessor createPreprocessor(Map<String, Object> preprocessorConfig, Client client)
      throws IllegalArgumentException {
    String name = StructureUtils.getStringValue(preprocessorConfig, CFG_NAME);
    if (ValueUtils.isEmpty(name)) {
      throw new IllegalArgumentException("'name' element not defined");
    }
    String className = StructureUtils.getStringValue(preprocessorConfig, CFG_CLASS);
    if (ValueUtils.isEmpty(className)) {
      throw new IllegalArgumentException("'class' element not defined for preprocessor " + name);
    }
    Object settings = preprocessorConfig.get(CFG_SETTINGS);
    if (settings != null && !(settings instanceof Map)) {
      throw new IllegalArgumentException("'settings' element must be Map for preprocessor " + name);
    }
    try {
      StructuredContentPreprocessor preproc = (StructuredContentPreprocessor) Class.forName(className).newInstance();
      preproc.init(name, client, (Map<String, Object>) settings);
      return preproc;
    } catch (InstantiationException e) {
      throw new IllegalArgumentException("Preprocessor class " + className + " creation exception " + e.getMessage(), e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Preprocessor class " + className + " creation exception " + e.getMessage(), e);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Preprocessor class " + className + " not found", e);
    } catch (ClassCastException e) {
      throw new IllegalArgumentException("Preprocessor class " + className + " must implement interface "
          + StructuredContentPreprocessor.class.getName());
    }
  }

  /**
   * Create more preprocessor from array of configurations described in this class's javadoc.
   * 
   * @param preprocessorConfig List of configuration structure in Map of Maps
   * @param client ES client to be passed to the preprocessor.
   * @return list of created instances
   * @throws IllegalArgumentException if something is wrong and preprocessor can't be instantiated.
   */
  public static List<StructuredContentPreprocessor> createPreprocessors(List<Map<String, Object>> preprocessorConfig,
      Client client) throws IllegalArgumentException {
    List<StructuredContentPreprocessor> ret = new ArrayList<StructuredContentPreprocessor>();
    if (preprocessorConfig != null) {
      for (Map<String, Object> cfgMap : preprocessorConfig) {
        ret.add(createPreprocessor(cfgMap, client));
      }
    }
    return ret;
  }

}
