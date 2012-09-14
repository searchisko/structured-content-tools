/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * Utility classes for tests.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class TestUtils {

  /**
   * Read JSON file from classpath into Map of Map structure.
   * 
   * @param filePath path inside jar/classpath pointing to JSON file to read
   * @return parsed JSON file
   * @throws SettingsException
   */
  public static Map<String, Object> loadJSONFromJarPackagedFile(String filePath) throws SettingsException {
    XContentParser parser = null;
    try {
      parser = XContentFactory.xContent(XContentType.JSON).createParser(
          StructureUtils.class.getResourceAsStream(filePath));
      return parser.mapAndClose();
    } catch (IOException e) {
      throw new SettingsException(e.getMessage(), e);
    } finally {
      if (parser != null)
        parser.close();
    }
  }

}
