/*
 * JBoss, Home of Professional Open Source
 * Copyright 2014 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

/**
 * Abstract base class for preprocessors supporting concept of "source_bases". Do not forgot to call parent
 * {@link #init(Map)} from your subclass init method if you override it!
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public abstract class StructuredContentPreprocessorWithSourceBasesBase<T> extends StructuredContentPreprocessorBase {

	protected static final String CFG_source_bases = "source_bases";

	protected List<String> sourceBases;

	@SuppressWarnings("unchecked")
	@Override
	public void init(Map<String, Object> settings) throws SettingsException {
		if (settings == null) {
			throw new SettingsException("'settings' section is not defined for preprocessor " + name);
		}
		sourceBases = (List<String>) settings.get(CFG_source_bases);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> preprocessData(Map<String, Object> data, PreprocessChainContext chainContext) {
		if (data == null)
			return null;

		if (sourceBases == null) {
			processOneSourceValue(data, null, null, chainContext);
		} else {
			T context = createContext();
			for (String base : sourceBases) {
				Object obj = XContentMapValues.extractValue(base, data);
				if (obj != null) {
					if (obj instanceof Map) {
						processOneSourceValue((Map<String, Object>) obj, context, base, chainContext);
					} else if (obj instanceof Collection) {
						for (Object o : (Collection<Object>) obj) {
							if (o instanceof Map) {
								processOneSourceValue((Map<String, Object>) o, context, base, chainContext);
							} else {
								String msg = "Collection in field '" + base
										+ "' contains value which is not Map, which can't be processed as source_base, so is skipped";
								addDataWarning(chainContext, msg);
								logger.debug(msg);
							}
						}
					} else {
						String msg = "Field '" + base
								+ "' contains invalid value which can't be processed as source_base, so is skipped";
						addDataWarning(chainContext, msg);
						logger.debug(msg);
					}
				}
			}
		}
		return data;
	}

	/**
	 * Do preprocessing of data. If "source_bases" concept is used then called multiple times for each base,
	 * <code>data<code> are relative for this base now.
	 * 
	 * @param data to run preprocessing on.
	 * @param context from {@link #createContext()}
	 * @param base processing is called for. Can be null if not called for base. It is just for use in warning messages
	 *          etc (for example {@link #getFullFieldName(String, String)}).
	 * @param chainContext preprocessor chain context
	 */
	protected abstract void processOneSourceValue(Map<String, Object> data, T context, String base,
			PreprocessChainContext chainContext);

	/**
	 * Create shared context object passed to each call of {@link #preprocessData(Map)} if "source_bases" concept is used.
	 * Not called when "source_bases" concept is not used.
	 * 
	 * @return context object or null
	 */
	protected abstract T createContext();

	/**
	 * Get full name of field in respect to base with dot notation.
	 * 
	 * @param base field is for, can be null
	 * @param field we want full name for
	 * @return full name for field
	 */
	protected static String getFullFieldName(String base, String field) {
		if (base != null) {
			return base + "." + field;
		} else {
			return field;
		}
	}

	/**
	 * Get configured source bases
	 * 
	 * @return source bases or null if not configured
	 */
	public List<String> getSourceBases() {
		return sourceBases;
	}

}
