/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link ValueUtils}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class ValueUtilsTest {

  @Test
  public void isEmpty() {
    Assert.assertTrue(ValueUtils.isEmpty(null));
    Assert.assertTrue(ValueUtils.isEmpty(""));
    Assert.assertTrue(ValueUtils.isEmpty("     "));
    Assert.assertTrue(ValueUtils.isEmpty(" "));
    Assert.assertFalse(ValueUtils.isEmpty("a"));
    Assert.assertFalse(ValueUtils.isEmpty(" a"));
    Assert.assertFalse(ValueUtils.isEmpty("a "));
    Assert.assertFalse(ValueUtils.isEmpty(" a "));
  }

  @Test
  public void trimToNull() {
    Assert.assertNull(ValueUtils.trimToNull(null));
    Assert.assertNull(ValueUtils.trimToNull(""));
    Assert.assertNull(ValueUtils.trimToNull("     "));
    Assert.assertNull(ValueUtils.trimToNull(" "));
    Assert.assertEquals("a", ValueUtils.trimToNull("a"));
    Assert.assertEquals("a", ValueUtils.trimToNull(" a"));
    Assert.assertEquals("a", ValueUtils.trimToNull("a "));
    Assert.assertEquals("a", ValueUtils.trimToNull(" a "));
  }

  @Test
  public void parseCsvString() {
    Assert.assertNull(ValueUtils.parseCsvString(null));
    Assert.assertNull(ValueUtils.parseCsvString(""));
    Assert.assertNull(ValueUtils.parseCsvString("    "));
    Assert.assertNull(ValueUtils.parseCsvString("  ,, ,   ,   "));
    List<String> r = ValueUtils.parseCsvString(" ORG ,UUUU, , PEM  , ,SU07  ");
    Assert.assertEquals(4, r.size());
    Assert.assertEquals("ORG", r.get(0));
    Assert.assertEquals("UUUU", r.get(1));
    Assert.assertEquals("PEM", r.get(2));
    Assert.assertEquals("SU07", r.get(3));
  }

  @Test
  public void createCsvString() {
    Assert.assertNull(ValueUtils.createCsvString(null));
    List<String> c = new ArrayList<String>();
    Assert.assertEquals("", ValueUtils.createCsvString(c));
    c.add("ahoj");
    Assert.assertEquals("ahoj", ValueUtils.createCsvString(c));
    c.add("b");
    c.add("task");
    Assert.assertEquals("ahoj,b,task", ValueUtils.createCsvString(c));
  }

}
