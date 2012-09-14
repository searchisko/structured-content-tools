/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link StructureUtils}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class StructureUtilsTest {

  @Test
  public void nodeIntegerValue() {
    Assert.assertNull(StructureUtils.nodeIntegerValue(null));
    Assert.assertEquals(new Integer(10), StructureUtils.nodeIntegerValue(new Integer(10)));
    Assert.assertEquals(new Integer(10), StructureUtils.nodeIntegerValue(new Short("10")));
    Assert.assertEquals(new Integer(10), StructureUtils.nodeIntegerValue(new Long("10")));
    Assert.assertEquals(new Integer(10), StructureUtils.nodeIntegerValue("10"));
    try {
      StructureUtils.nodeIntegerValue("ahoj");
      Assert.fail("No NumberFormatException thrown.");
    } catch (NumberFormatException e) {
      // OK
    }
  }

  @Test
  public void filterDataInMap() {
    // case - no exceptions on distinct null and empty inputs
    StructureUtils.filterDataInMap(null, null);
    Set<String> keysToLeave = new HashSet<String>();
    StructureUtils.filterDataInMap(null, keysToLeave);
    Map<String, Object> map = new HashMap<String, Object>();
    StructureUtils.filterDataInMap(map, null);
    StructureUtils.filterDataInMap(map, keysToLeave);
    keysToLeave.add("key1");
    StructureUtils.filterDataInMap(null, keysToLeave);
    StructureUtils.filterDataInMap(map, keysToLeave);

    // case - no filtering on null or empty keysToLeave
    keysToLeave.clear();
    map.clear();
    map.put("key1", "val1");
    map.put("key2", "val2");
    StructureUtils.filterDataInMap(map, null);
    Assert.assertEquals(2, map.size());
    StructureUtils.filterDataInMap(map, keysToLeave);
    Assert.assertEquals(2, map.size());

    // case - filtering works
    map.clear();
    keysToLeave.clear();
    map.put("key2", "val2");
    keysToLeave.add("key2");
    StructureUtils.filterDataInMap(map, keysToLeave);
    Assert.assertEquals(1, map.size());
    Assert.assertTrue(map.containsKey("key2"));

    map.clear();
    keysToLeave.clear();
    map.put("key1", "val1");
    map.put("key2", "val2");
    map.put("key3", "val3");
    map.put("key4", "val4");
    keysToLeave.add("key2");
    keysToLeave.add("key3");
    StructureUtils.filterDataInMap(map, keysToLeave);
    Assert.assertEquals(2, map.size());
    Assert.assertTrue(map.containsKey("key2"));
    Assert.assertTrue(map.containsKey("key3"));
  }

  @Test
  public void remapDataInMap() {
    // case - no exceptions on distinct null and empty inputs
    StructureUtils.remapDataInMap(null, null);
    Map<String, String> remapInstructions = new LinkedHashMap<String, String>();
    StructureUtils.remapDataInMap(null, remapInstructions);
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    StructureUtils.remapDataInMap(map, null);
    StructureUtils.remapDataInMap(map, remapInstructions);
    remapInstructions.put("key1", "key1new");
    StructureUtils.remapDataInMap(null, remapInstructions);
    StructureUtils.remapDataInMap(map, remapInstructions);

    // case - no change in map if remap instruction is null or empty
    remapInstructions.clear();
    map.put("key1", "value1");
    map.put("key2", "value2");
    StructureUtils.remapDataInMap(map, null);
    Assert.assertEquals(2, map.size());
    Assert.assertEquals("value1", map.get("key1"));
    Assert.assertEquals("value2", map.get("key2"));
    StructureUtils.remapDataInMap(map, remapInstructions);
    Assert.assertEquals(2, map.size());
    Assert.assertEquals("value1", map.get("key1"));
    Assert.assertEquals("value2", map.get("key2"));

    // case remap some values and filter out some other and leave some untouched
    map.clear();
    remapInstructions.clear();
    map.put("key1", "value1");
    map.put("key2", "value2");
    map.put("key3", "value3");
    map.put("key4", "value4");
    map.put("key5", "value5");
    map.put("key6", "value6");
    map.put("key7", "value7");
    map.put("key8", "value8");
    map.put("key9", "value9");
    map.put("key10", "value10");

    remapInstructions.put("key1", "key1new");
    remapInstructions.put("key3", "key3");
    remapInstructions.put("key4", "key5");
    remapInstructions.put("key6", "key4");
    remapInstructions.put("key7", "key8");
    remapInstructions.put("key8", "key7");
    remapInstructions.put("key10", "key10new");

    StructureUtils.remapDataInMap(map, remapInstructions);
    Assert.assertEquals(7, map.size());
    Assert.assertEquals("value1", map.get("key1new"));
    Assert.assertFalse(map.containsKey("key2"));
    Assert.assertEquals("value3", map.get("key3"));
    Assert.assertEquals("value4", map.get("key5"));
    Assert.assertEquals("value6", map.get("key4"));
    Assert.assertEquals("value7", map.get("key8"));
    Assert.assertEquals("value8", map.get("key7"));
    Assert.assertFalse(map.containsKey("key9"));
    Assert.assertEquals("value10", map.get("key10new"));
  }

  @Test
  public void putValueIntoMapOfMaps() {

    // case - not NPE on empty data map
    StructureUtils.putValueIntoMapOfMaps(null, "field", null);
    Map<String, Object> map = new HashMap<String, Object>();

    // case - exception on invalid field definition
    try {
      StructureUtils.putValueIntoMapOfMaps(map, null, null);
      Assert.fail("IllegalArgumentException must be thrown");
    } catch (IllegalArgumentException e) {
      // OK
    }
    try {
      StructureUtils.putValueIntoMapOfMaps(map, "  ", null);
      Assert.fail("IllegalArgumentException must be thrown");
    } catch (IllegalArgumentException e) {
      // OK
    }

    // case - simplefield - insert null can produce null even if it was set previously
    StructureUtils.putValueIntoMapOfMaps(map, "field", null);
    Assert.assertNull(map.get("field"));
    map.put("field", "value");
    StructureUtils.putValueIntoMapOfMaps(map, "field", null);
    Assert.assertNull(map.get("field"));

    // case - simplefield - insert and replace value
    map.clear();
    StructureUtils.putValueIntoMapOfMaps(map, "field", "value");
    Assert.assertEquals("value", map.get("field"));
    StructureUtils.putValueIntoMapOfMaps(map, "field", "value2");
    Assert.assertEquals("value2", map.get("field"));

    // case- dot notation
    map.clear();
    StructureUtils.putValueIntoMapOfMaps(map, "field.level1.level11", "value");
    Assert.assertEquals("value", XContentMapValues.extractValue("field.level1.level11", map));

    StructureUtils.putValueIntoMapOfMaps(map, "field.level1.level12", "value2");
    Assert.assertEquals("value2", XContentMapValues.extractValue("field.level1.level12", map));

    // case - dot notation structure error leads to exception
    try {
      map.clear();
      map.put("field", "dsd");
      StructureUtils.putValueIntoMapOfMaps(map, "field.level1.level11", "value");
      Assert.fail("IllegalArgumentException must be thrown");
    } catch (IllegalArgumentException e) {
      // OK
    }
    try {
      map.clear();
      map.put("field", new ArrayList<Object>());
      StructureUtils.putValueIntoMapOfMaps(map, "field.level1.level11", "value");
      Assert.fail("IllegalArgumentException must be thrown");
    } catch (IllegalArgumentException e) {
      // OK
    }
  }

}
