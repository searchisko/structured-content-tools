/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.xcontent.support.XContentMapValues;

/**
 * Utility functions for values manipulation.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class ValueUtils {

  /**
   * Trim String value, return null if empty after trim.
   * 
   * @param src value
   * @return trimmed value or null
   */
  public static String trimToNull(String src) {
    if (src == null || src.length() == 0) {
      return null;
    }
    src = src.trim();
    if (src.length() == 0) {
      return null;
    }
    return src;
  }

  /**
   * Check if String value is null or empty.
   * 
   * @param src value
   * @return <code>true</code> if value is null or empty
   */
  public static boolean isEmpty(String src) {
    return (src == null || src.length() == 0 || src.trim().length() == 0);
  }

  /**
   * Parse comma separated string into list of tokens. Tokens are trimmed, empty tokens are not in result.
   * 
   * @param toParse String to parse
   * @return List of tokens if at least one token exists, null otherwise.
   */
  public static List<String> parseCsvString(String toParse) {
    if (toParse == null || toParse.length() == 0) {
      return null;
    }
    String[] t = toParse.split(",");
    if (t.length == 0) {
      return null;
    }
    List<String> ret = new ArrayList<String>();
    for (String s : t) {
      if (s != null) {
        s = s.trim();
        if (s.length() > 0) {
          ret.add(s);
        }
      }
    }
    if (ret.isEmpty())
      return null;
    else
      return ret;
  }

  /**
   * Create string with comma separated list of values from input collection. Ordering by used Collection implementation
   * iteration order is used.
   * 
   * @param in collection to format
   * @return <code>null</code> if <code>in</code> is <code>null</code>, CSV string in other cases (empty id in is empty)
   */
  public static String createCsvString(Collection<String> in) {
    if (in == null)
      return null;
    if (in.isEmpty()) {
      return "";
    }
    boolean first = true;
    StringBuilder sb = new StringBuilder();
    for (String s : in) {
      if (first)
        first = false;
      else
        sb.append(",");
      sb.append(s);
    }
    return sb.toString();
  }

  /**
   * Key used in {@link #processStringValuePatternReplacement(String, Map, Object)} to indicate original value.
   */
  public static final String PATTERN_KEY_ORIGINAL_VALUE = "__original";

  /**
   * This method replaces keys in input string with values from passed data Map structure. Keys are enclosed in curly
   * braces, dot notation for deeper nesting may be used in keys. Special key
   * <code>{@value #PATTERN_KEY_ORIGINAL_VALUE}</code> is used to be replaced by original value passed to this method as
   * separate parameter. Example of value with replacement keys:
   * <code>My name is {user.name} and surname is {user.surname}.</code>. If value is not found in data structure then
   * empty string is used. If value in data is not String then <code>toString()</code> is used to convert it.
   * 
   * @param patternValue to process
   * @param data to get replacement values from
   * @param originalValue used in pattern if {@value #PATTERN_KEY_ORIGINAL_VALUE} is used as key
   * @return value with replaced keys
   */
  public static String processStringValuePatternReplacement(String patternValue, Map<String, Object> data,
      Object originalValue) {
    if (patternValue == null || patternValue.length() == 0)
      return patternValue;
    StringBuilder finalContent = new StringBuilder();

    boolean inBraces = false;
    StringBuilder bracesContent = null;
    for (int idx = 0; idx < patternValue.length(); idx++) {
      char ch = patternValue.charAt(idx);
      if (!inBraces && ch == '{') {
        inBraces = true;
        bracesContent = new StringBuilder();
      } else if (inBraces && ch == '}') {
        inBraces = false;
        String key = bracesContent.toString();
        if (key.length() > 0) {
          Object v = null;
          if (PATTERN_KEY_ORIGINAL_VALUE.equals(key)) {
            v = originalValue;
          } else if (data != null) {
            if (key.contains(".")) {
              v = XContentMapValues.extractValue(key, data);
            } else {
              v = data.get(key);
            }
          }
          if (v != null) {
            finalContent.append(v.toString());
          }
        }
      } else if (inBraces) {
        bracesContent.append(ch);
      } else {
        finalContent.append(ch);
      }
    }
    // handle not closed brace
    if (inBraces) {
      finalContent.append("{").append(bracesContent);
    }
    return finalContent.toString();
  }
}
