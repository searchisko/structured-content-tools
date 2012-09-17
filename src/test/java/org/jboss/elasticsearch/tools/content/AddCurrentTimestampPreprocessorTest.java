/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.common.settings.SettingsException;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit test for {@link AddCurrentTimestampPreprocessor}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class AddCurrentTimestampPreprocessorTest {

  @Test
  public void init_settingerrors() {
    AddCurrentTimestampPreprocessor tested = new AddCurrentTimestampPreprocessor();
    Client client = Mockito.mock(Client.class);
    Map<String, Object> settings = null;

    // case - settings mandatory
    try {
      tested.init("Test mapper", client, settings);
      Assert.fail("SettingsException must be thrown");
    } catch (SettingsException e) {
      Assert.assertEquals("'settings' section is not defined for preprocessor Test mapper", e.getMessage());
    }

    // case - field mandatory
    settings = new HashMap<String, Object>();
    try {
      tested.init("Test mapper", client, settings);
      Assert.fail("SettingsException must be thrown");
    } catch (SettingsException e) {
      Assert.assertEquals("Missing or empty 'settings/field' configuration value for 'Test mapper' preprocessor",
          e.getMessage());
    }

    settings.put(AddCurrentTimestampPreprocessor.CFG_FIELD, " ");
    try {
      tested.init("Test mapper", client, settings);
      Assert.fail("SettingsException must be thrown");
    } catch (SettingsException e) {
      Assert.assertEquals("Missing or empty 'settings/field' configuration value for 'Test mapper' preprocessor",
          e.getMessage());
    }

    // case - no more mandatory setting fields
    settings.put(AddCurrentTimestampPreprocessor.CFG_FIELD, "field");
    tested.init("Test mapper", client, settings);
  }

  @Test
  public void init() {
    AddCurrentTimestampPreprocessor tested = new AddCurrentTimestampPreprocessor();
    Client client = Mockito.mock(Client.class);

    // case - null value
    {
      Map<String, Object> settings = new HashMap<String, Object>();
      settings.put(AddCurrentTimestampPreprocessor.CFG_FIELD, "source");

      tested.init("Test mapper", client, settings);
      Assert.assertEquals("Test mapper", tested.getName());
      Assert.assertEquals(client, tested.client);
      Assert.assertEquals("source", tested.field);
    }
  }

  @Test
  public void preprocessData() {
    AddCurrentTimestampPreprocessor tested = new AddCurrentTimestampPreprocessor();
    tested.field = "my_field";

    // case - not NPE
    tested.preprocessData(null);

    // case - fill date value over null
    {
      Map<String, Object> values = new HashMap<String, Object>();
      long now = System.currentTimeMillis();
      tested.preprocessData(values);
      long val = ISODateTimeFormat.dateTimeParser().parseMillis((String) values.get(tested.field));
      Assert.assertTrue(now <= val && val <= now + 100);
    }

    // case - rewrite date value over null
    {
      Map<String, Object> values = new HashMap<String, Object>();
      values.put(tested.field, "value old");
      long now = System.currentTimeMillis();
      tested.preprocessData(values);
      long val = ISODateTimeFormat.dateTimeParser().parseMillis((String) values.get(tested.field));
      Assert.assertTrue(now <= val && val <= now + 100);
    }
  }
}
