package org.jboss.elasticsearch.tools.content;

/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Utility functions for structured content manipulation. Structured content is commonly represented as Map of Maps
 * structure.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class StructureUtils {

	/**
	 * Typesafe get value from map as {@link Integer} object instance if possible.
	 * 
	 * @param values to get value from. Can be null.
	 * @param key to get value from Map. Must be defined. Dot notation not supported here for nesting!
	 * @return Integer value or null.
	 * @throws NumberFormatException if value can't be converted to the int value
	 * 
	 */
	public static Integer getIntegerValue(Map<String, Object> values, String key) throws NumberFormatException {
		if (ValueUtils.isEmpty(key))
			throw new IllegalArgumentException("key must be defined");
		if (values == null)
			return null;

		Object node = values.get(key);
		if (node == null) {
			return null;
		}
		if (node instanceof Integer) {
			return (Integer) node;
		} else if (node instanceof Number) {
			return new Integer(((Number) node).intValue());
		}

		return Integer.parseInt(node.toString());
	}

	/**
	 * Typesafe get value from map as {@link String}. An {@link Object#toString()} is used for nonstring objects.
	 * 
	 * @param values to get value from. Can be null.
	 * @param key to get value from Map. Must be defined. Dot notation not supported here for nesting!
	 * @return value for given key as String.
	 */
	public static String getStringValue(Map<String, Object> values, String key) {
		if (ValueUtils.isEmpty(key))
			throw new IllegalArgumentException("key must be defined");
		if (values == null)
			return null;
		Object node = values.get(key);
		if (node == null) {
			return null;
		} else {
			return node.toString();
		}
	}

	/**
	 * Filter data in Map. Leave here only data with keys passed in second parameter.
	 * 
	 * @param map to filter data inside
	 * @param keysToLeave keys leaved in map. If <code>null</code> or empty then no filtering is performed!
	 */
	public static <T> void filterDataInMap(Map<T, Object> map, Set<T> keysToLeave) {
		if (map == null || map.isEmpty())
			return;
		if (keysToLeave == null || keysToLeave.isEmpty())
			return;

		Set<T> keysToRemove = new HashSet<T>(map.keySet());
		keysToRemove.removeAll(keysToLeave);
		if (!keysToRemove.isEmpty()) {
			for (T rk : keysToRemove) {
				map.remove(rk);
			}
		}
	}

	/**
	 * Remap data in input Map. Leave here only data with defined keys, but change these keys to new ones if necessary.
	 * Some new key can be same as some other old key, but if two new keys are same, then only latest value is preserved
	 * (given by <code>mapToChange</code> key iteration order).
	 * 
	 * @param mapToChange Map to remap data inside. Must be mutable!
	 * @param remapInstructions instructions how to remap. If <code>null</code> or empty then remap is not performed and
	 *          <code>mapToChange</code> is not changed! Key in this Map must be same as key in <code>mapToChange</code>
	 *          which may leave there. Value in this map means new key of value in <code>mapToChange</code> after
	 *          remapping.
	 */
	public static <T> void remapDataInMap(Map<T, Object> mapToChange, Map<T, T> remapInstructions) {
		if (mapToChange == null || mapToChange.isEmpty())
			return;
		if (remapInstructions == null || remapInstructions.isEmpty())
			return;

		Map<T, Object> newMap = new HashMap<T, Object>();
		for (T keyOrig : mapToChange.keySet()) {
			if (remapInstructions.containsKey(keyOrig)) {
				T keyNew = remapInstructions.get(keyOrig);
				newMap.put(keyNew, mapToChange.get(keyOrig));
			}
		}

		mapToChange.clear();
		mapToChange.putAll(newMap);
	}

	/**
	 * Put value into Map of Maps structure. Dot notation supported for deeper level of nesting.
	 * 
	 * @param map Map to put value into
	 * @param field to put value into. Dot notation can be used.
	 * @param value to be added into Map
	 * @throws IllegalArgumentException if value can't be added due something wrong in data structure
	 */
	@SuppressWarnings("unchecked")
	public static void putValueIntoMapOfMaps(Map<String, Object> map, String field, Object value)
			throws IllegalArgumentException {
		if (map == null)
			return;
		if (ValueUtils.isEmpty(field)) {
			throw new IllegalArgumentException("field argument must be defined");
		}
		if (field.contains(".")) {
			String[] tokens = field.split("\\.");
			int tokensCount = tokens.length;
			Map<String, Object> levelData = map;
			for (String tok : tokens) {
				if (tokensCount == 1) {
					levelData.put(tok, value);
				} else {
					Object o = levelData.get(tok);
					if (o == null) {
						Map<String, Object> lv = new LinkedHashMap<String, Object>();
						levelData.put(tok, lv);
						levelData = lv;
					} else if (o instanceof Map) {
						levelData = (Map<String, Object>) o;
					} else {
						throw new IllegalArgumentException("Cant put value for field '" + field
								+ "' because some element in the path is not Map");
					}
				}
				tokensCount--;
			}
		} else {
			map.put(field, value);
		}
	}

	/**
	 * Remove value from Map of Maps structure. Dot notation supported for deeper level of nesting.
	 * 
	 * @param map Map to remove value from
	 * @param field to remove. Dot notation can be used.
	 * @return object removed from structure if any
	 * @throws IllegalArgumentException if value can't be removed due something wrong in data structure
	 */
	@SuppressWarnings("unchecked")
	public static Object removeValueFromMapOfMaps(Map<String, Object> map, String field) throws IllegalArgumentException {
		if (map == null)
			return null;
		if (ValueUtils.isEmpty(field)) {
			throw new IllegalArgumentException("field argument must be defined");
		}
		if (field.contains(".")) {
			String[] tokens = field.split("\\.");
			int tokensCount = tokens.length;
			Map<String, Object> levelData = map;
			for (String tok : tokens) {
				if (tokensCount == 1) {
					return levelData.remove(tok);
				} else {
					Object o = levelData.get(tok);
					if (o == null) {
						return null;
					} else if (o instanceof Map) {
						levelData = (Map<String, Object>) o;
					} else {
						throw new IllegalArgumentException("Cant remove value for field '" + field
								+ "' because some element in the path is not Map");
					}
				}
				tokensCount--;
			}
		} else {
			return map.remove(field);
		}
		return null;
	}

}
