/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.Collection;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.SettingsException;

/**
 * Abstract base class for {@link StructuredContentPreprocessor} implementations.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public abstract class StructuredContentPreprocessorBase implements StructuredContentPreprocessor {

	protected ESLogger logger = null;

	protected String name;
	protected Client client;

	protected StructuredContentPreprocessorBase() {
		logger = Loggers.getLogger(getClass(), name);
	}

	@Override
	public void init(String name, Client client, Map<String, Object> settings) throws SettingsException {
		this.name = name;
		this.client = client;
		init(settings);
	}

	/**
	 * Init your instance settings. You can use instance fields to access other services.
	 * 
	 * @param settings to init from
	 * @throws SettingsException use this exception in case on bad configuration for your implementation
	 */
	public abstract void init(Map<String, Object> settings) throws SettingsException;

	@Override
	public Map<String, Object> preprocessData(Map<String, Object> data) {
		return preprocessData(data, null);
	}

	/**
	 * Write warning message into processing chain context if available.
	 * 
	 * @param chainContext to write warning into. Can be <code>null</code>.
	 * @param warningMessage message with warning description. It is a good idea to write name of data field with problem
	 *          in this message to be clear where problem is.
	 * @see PreprocessChainContext#addDataWarning(String, String)
	 */
	protected void addDataWarning(PreprocessChainContext chainContext, String warningMessage) {
		if (warningMessage == null) {
			throw new IllegalArgumentException("warningMessage must be provided");
		}
		if (chainContext != null) {
			chainContext.addDataWarning(name, warningMessage);
		}
	}

	/**
	 * Validate configuration string is not null or empty. Useful for your {@link #init(Map)} implementation.
	 * 
	 * @param value to check
	 * @param configFieldName name of field in preprocessor settings structure. Used for error message.
	 * @throws SettingsException thrown if value is null or empty
	 */
	protected void validateConfigurationStringNotEmpty(String value, String configFieldName) throws SettingsException {
		if (ValueUtils.isEmpty(value)) {
			throw new SettingsException("Missing or empty 'settings/" + configFieldName + "' configuration value for '"
					+ name + "' preprocessor");
		}
	}

	/**
	 * Validate configuration object is not null or empty in case of String or Collection. Useful for your
	 * {@link #init(Map)} implementation.
	 * 
	 * @param value to check
	 * @param configFieldName name of field in preprocessor settings structure. Used for error message.
	 * @throws SettingsException thrown if value is null or empty
	 */
	@SuppressWarnings("unchecked")
	protected void validateConfigurationObjectNotEmpty(Object value, String configFieldName) throws SettingsException {
		if (value == null || (value instanceof String && ValueUtils.isEmpty((String) value))
				|| ((value instanceof Collection) && ((Collection<Object>) value).isEmpty())) {
			throw new SettingsException("Missing or empty 'settings/" + configFieldName + "' configuration value for '"
					+ name + "' preprocessor");
		}
	}

	/**
	 * Read configuration value which is mandatory int.
	 * 
	 * @param settings to read value from
	 * @param configFieldName name of field in preprocessor settings structure to read.
	 */
	protected int readMandatoryIntegerConfigValue(Map<String, Object> settings, String configFieldName) {
		try {
			Integer mv = StructureUtils.getIntegerValue(settings, configFieldName);
			if (mv == null) {
				throw new SettingsException("Missing or empty 'settings/" + configFieldName + "' configuration value for '"
						+ name + "' preprocessor");
			} else {
				return mv;
			}
		} catch (NumberFormatException e) {
			throw new SettingsException("Non integer 'settings/" + configFieldName + "' configuration value for '" + name
					+ "' preprocessor");
		}
	}

	@Override
	public String getName() {
		return name;
	}

}
