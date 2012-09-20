/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content.testtools;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Assert;

/**
 * Helper methods for Unit tests.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public abstract class TestUtils {

  /**
   * Assert passed string is same as contnt of given file loaded from classpath.
   * 
   * @param expectedFilePath path to file inside classpath
   * @param actual content to assert
   * @throws IOException
   */
  public static void assertStringFromClasspathFile(String expectedFilePath, String actual) throws IOException {
    Assert.assertEquals(readStringFromClasspathFile(expectedFilePath), actual);
  }

  /**
   * Read file from classpath into String. UTF-8 encoding expected.
   * 
   * @param filePath in classpath to read data from.
   * @return file content.
   * @throws IOException
   */
  public static String readStringFromClasspathFile(String filePath) throws IOException {
    StringWriter stringWriter = new StringWriter();
    IOUtils.copy(TestUtils.class.getResourceAsStream(filePath), stringWriter, "UTF-8");
    return stringWriter.toString();
  }

  /**
   * Read JSON file from classpath into Map of Map structure.
   * 
   * @param filePath path in classpath pointing to JSON file to read
   * @return parsed JSON file
   * @throws SettingsException
   */
  public static Map<String, Object> loadJSONFromClasspathFile(String filePath) throws SettingsException {
    XContentParser parser = null;
    try {
      parser = XContentFactory.xContent(XContentType.JSON).createParser(TestUtils.class.getResourceAsStream(filePath));
      return parser.mapAndClose();
    } catch (IOException e) {
      throw new SettingsException(e.getMessage(), e);
    } finally {
      if (parser != null)
        parser.close();
    }
  }

}
