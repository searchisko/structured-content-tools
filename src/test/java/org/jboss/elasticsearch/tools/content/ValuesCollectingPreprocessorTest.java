/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.jboss.elasticsearch.tools.content.testtools.TestUtils;
import org.junit.Test;

/**
 * Unit tests for {@link ValuesCollectingPreprocessor}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class ValuesCollectingPreprocessorTest {

  @Test
  public void init_settingerrors() {
    ValuesCollectingPreprocessor tested = new ValuesCollectingPreprocessor();
    Map<String, Object> settings = null;

    // case - settings mandatory
    try {
      tested.init("Test mapper", null, settings);
      Assert.fail("SettingsException must be thrown");
    } catch (SettingsException e) {
      Assert.assertEquals("'settings' section is not defined for preprocessor Test mapper", e.getMessage());
    }

    // case - source_field mandatory
    settings = new HashMap<String, Object>();
    try {
      tested.init("Test mapper", null, settings);
      Assert.fail("SettingsException must be thrown");
    } catch (SettingsException e) {
      Assert.assertEquals(
          "Missing or empty 'settings/source_fields' configuration value for 'Test mapper' preprocessor",
          e.getMessage());
    }

    // case - source_field cant be empty
    List<Object> sources = new ArrayList<Object>();
    settings.put(ValuesCollectingPreprocessor.CFG_SOURCE_FIELDS, sources);
    try {
      tested.init("Test mapper", null, settings);
      Assert.fail("SettingsException must be thrown");
    } catch (SettingsException e) {
      Assert.assertEquals(
          "Missing or empty 'settings/source_fields' configuration value for 'Test mapper' preprocessor",
          e.getMessage());
    }

    // case - target_field mandatory
    sources.add("source1");
    try {
      tested.init("Test mapper", null, settings);
      Assert.fail("SettingsException must be thrown");
    } catch (SettingsException e) {
      Assert
          .assertEquals("Missing or empty 'settings/target_field' configuration value for 'Test mapper' preprocessor",
              e.getMessage());
    }

    // case - no more mandatory setting fields
    settings.put(ValuesCollectingPreprocessor.CFG_TARGET_FIELD, "target");
    tested.init("Test mapper", null, settings);
  }

  @Test
  public void init() {
    ValuesCollectingPreprocessor tested = new ValuesCollectingPreprocessor();

    // case - mandatory fields only
    Map<String, Object> settings = new HashMap<String, Object>();
    List<Object> sources = new ArrayList<Object>();
    sources.add("source1");
    sources.add("source2");
    settings.put(ValuesCollectingPreprocessor.CFG_SOURCE_FIELDS, sources);
    settings.put(ValuesCollectingPreprocessor.CFG_TARGET_FIELD, "target");

    tested.init("Test mapper", null, settings);
    Assert.assertEquals("Test mapper", tested.getName());
    Assert.assertEquals(2, tested.fieldsSource.size());
    Assert.assertTrue(tested.fieldsSource.contains("source1"));
    Assert.assertTrue(tested.fieldsSource.contains("source2"));
    Assert.assertEquals("target", tested.fieldTarget);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void preprocessData() {
    ValuesCollectingPreprocessor tested = new ValuesCollectingPreprocessor();
    tested.init("Test mapper", null, TestUtils.loadJSONFromClasspathFile("/ValuesCollecting_preprocessData.json"));

    // case - not NPE
    tested.preprocessData(null);

    // case - nothing in input, so no output value added
    {
      Map<String, Object> values = new HashMap<String, Object>();
      tested.preprocessData(values);
      Assert.assertNull(values.get("target"));
    }

    // case - nothing in input, so no output value added, but there was something in field so it is deleted
    {
      Map<String, Object> values = new HashMap<String, Object>();
      values.put("target", "something to delete");
      tested.preprocessData(values);
      Assert.assertNull(values.get("target"));
    }

    // case - normal processing with duplicity removes and simple target field
    {
      Map<String, Object> values = new HashMap<String, Object>();
      values.put("source_simple", "source value");
      values.put("source_duplicit", "source value");
      Map<String, Object> nested = new HashMap<String, Object>();
      values.put("source_nested", nested);
      nested.put("value1", "nested value 1");

      tested.preprocessData(values);
      List<Object> vals = (List<Object>) values.get("target");
      Assert.assertNotNull(vals);
      Assert.assertEquals(2, vals.size());
      Assert.assertTrue(vals.contains("source value"));
      Assert.assertTrue(vals.contains("nested value 1"));
    }

    // case - normal processing with duplicity removes and nested target field. Some lists in source data used.
    {
      Map<String, Object> values = new HashMap<String, Object>();
      values.put("source_simple", "source value");
      values.put("source_duplicit", "source value");
      Map<String, Object> nested = new HashMap<String, Object>();
      values.put("source_nested", nested);
      nested.put("value1", "nested value 1");
      List<Object> list1 = new ArrayList<Object>();
      list1.add("list1 value1");
      list1.add("list1 value2");
      nested.put("list1", list1);

      List<Object> list2 = new ArrayList<Object>();
      list2.add(newMapWithFiled("value", "list2 value1"));
      list2.add(newMapWithFiled("value", "list2 value2"));
      nested.put("list2", list2);

      tested.fieldTarget = "target.level1";

      tested.preprocessData(values);
      List<Object> vals = (List<Object>) XContentMapValues.extractValue(tested.fieldTarget, values);
      Assert.assertNotNull(vals);
      Assert.assertEquals(6, vals.size());
      Assert.assertTrue(vals.contains("source value"));
      Assert.assertTrue(vals.contains("nested value 1"));
      Assert.assertTrue(vals.contains("list1 value1"));
      Assert.assertTrue(vals.contains("list1 value2"));
      Assert.assertTrue(vals.contains("list2 value1"));
      Assert.assertTrue(vals.contains("list2 value2"));
    }

  }

  private Map<String, Object> newMapWithFiled(String key, String value) {
    Map<String, Object> ret = new HashMap<String, Object>();
    ret.put(key, value);
    return ret;
  }
}
