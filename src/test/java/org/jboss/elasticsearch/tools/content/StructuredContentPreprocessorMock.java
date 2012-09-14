/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.Map;

import org.elasticsearch.common.settings.SettingsException;

/**
 * Implementation of StructuredContentPreprocessorBase for configuration loading tests.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class StructuredContentPreprocessorMock extends StructuredContentPreprocessorBase {

  public Map<String, Object> settings = null;

  @Override
  public void init(Map<String, Object> settings) throws SettingsException {
    this.settings = settings;
  }

  @Override
  public Map<String, Object> preprocessData(Map<String, Object> issueData) {
    return issueData;
  }

}
