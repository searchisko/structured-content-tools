/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
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
     * Check if value is null or empty String.
     * 
     * @param src value to check
     * @return <code>true</code> if value is null or empty String
     */
    public static boolean isEmpty(Object src) {
        if (src instanceof String)
            return isEmpty((String) src);
        else
            return src == null;
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
     * Create string with comma separated list of values from input collection. Ordering by used Collection
     * implementation iteration order is used.
     * 
     * @param in collection to format
     * @return <code>null</code> if <code>in</code> is <code>null</code>, CSV string in other cases (empty id in is
     *         empty)
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
     * <code>{@value #PATTERN_KEY_ORIGINAL_VALUE}</code> is used to be replaced by original value passed to this method
     * as separate parameter. Example of value with replacement keys:
     * <code>My name is {user.name} and surname is {user.surname}.</code>. If value is not found in data structure then
     * empty string is used. If value in data is not String then <code>toString()</code> is used to convert it.
     * 
     * @param patternValue to process
     * @param data to get replacement values from
     * @param originalValue used in pattern if {@value #PATTERN_KEY_ORIGINAL_VALUE} is used as key
     * @return value with replaced keys
     */
    public static String processStringValuePatternReplacement(String patternValue, Map<String, Object> data, Object originalValue) {
        return processStringValuePatternReplacement(patternValue, data, originalValue, '{', '}', null);
    }
    
    /**
     * @see #processStringValuePatternReplacement(String, Map, Object)
     */
    public static String processStringValuePatternReplacement(String patternValue, Map<String, Object> data, Object originalValue, IValueEncoder encoder) {
        return processStringValuePatternReplacement(patternValue, data, originalValue, '{', '}', encoder);
    }

    /**
     * Interface for value encoder used by <code>processStringValuePatternReplacement()</code> method.
     */
    public static interface IValueEncoder {
        public String encode(Object value);
    }

    /**
     * This method replaces keys in input string with values from passed data Map structure. Keys are enclosed by
     * characters defined ias input params. Dot notation for deeper nesting may be used in keys. Special key
     * <code>{@value #PATTERN_KEY_ORIGINAL_VALUE}</code> is used to be replaced by original value passed to this method
     * as separate parameter. Example of value with replacement keys:
     * <code>My name is {user.name} and surname is {user.surname}.</code>. If value is not found in data structure then
     * empty string is used. If value in data is not String then <code>toString()</code> is used to convert it.
     * 
     * @param patternValue to process
     * @param data to get replacement values from
     * @param originalValue used in pattern if {@value #PATTERN_KEY_ORIGINAL_VALUE} is used as key
     * @param startKeyChar start character to look for keys
     * @param endKeyChar end character to look for keys
     * @param encoder which may be used to encode values before appending to the pattern. Common <code>toString()</code> is used if not provided.
     * @return value with replaced keys
     */
    public static String processStringValuePatternReplacement(String patternValue, Map<String, Object> data, Object originalValue, char startKeyChar, char endKeyChar, IValueEncoder encoder) {
        if (patternValue == null || patternValue.length() == 0)
            return patternValue;
        StringBuilder finalContent = new StringBuilder();

        boolean inBraces = false;
        StringBuilder bracesContent = null;
        for (int idx = 0; idx < patternValue.length(); idx++) {
            char ch = patternValue.charAt(idx);
            if (!inBraces && ch == startKeyChar) {
                inBraces = true;
                bracesContent = new StringBuilder();
            } else if (inBraces && ch == endKeyChar) {
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
                        if (encoder != null) {
                            finalContent.append(encoder.encode(v));
                        } else {
                            finalContent.append(v.toString());
                        }
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
            finalContent.append(startKeyChar).append(bracesContent);
        }
        return finalContent.toString();
    }

    protected static final SimpleDateFormat ISO_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXX");
    static {
        ISO_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    /**
     * Format Date into ISO 8601 full datetime string.
     * 
     * @param date to format
     * @return formatted string
     */
    public static final String formatISODateTime(Date date) {
        if (date == null)
            return null;
        synchronized (ISO_DATE_FORMAT) {
            return ISO_DATE_FORMAT.format(date);
        }
    }

    /**
     * Parse JSON data into Object Structure.
     * 
     * @param jsonData to parse
     * @return parsed data
     * @throws UnsupportedEncodingException
     * @throws IOException
     */
    public static Map<String, Object> parseJSON(byte[] jsonData) throws  IOException {
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(XContentType.JSON).createParser(jsonData);
            return parser.mapAndClose();
        } finally {
            if (parser != null)
                parser.close();
        }
    }
}
