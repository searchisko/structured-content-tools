/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.SettingsException;

/**
 * Interface for components used to preprocess structured data before other action, eg. indexed document is created from
 * them. Instances may be created from configuration using {@link StructuredContentPreprocessorFactory}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public interface StructuredContentPreprocessor {

	/**
	 * Initialize preprocessor after created.
	 * 
	 * @param name name of preprocessor
	 * @param client ElasticSearch client which can be used in this preprocessor to access data in ES cluster.
	 * @param settings structure obtained from river configuration
	 * @throws SettingsException use this exception in case on bad configuration for your implementation
	 */
	void init(String name, Client client, Map<String, Object> settings) throws SettingsException;

	/**
	 * Get name of this preprocessor.
	 * 
	 * @return name of this preprocessor.
	 */
	String getName();

	/**
	 * Preprocess data. Use this if you do not want to use chain context. This typically simply calls
	 * {@link #preprocessData(Map, PreprocessChainContext)} with <code>null</code> context.
	 * 
	 * @param data to be preprocessed - may be changed during call!
	 * @return preprocessed data - typically same object ad <code>data</code> parameter, but with changed structure.
	 */
	Map<String, Object> preprocessData(Map<String, Object> data);

	/**
	 * Preprocess data.
	 * 
	 * @param data to be preprocessed - may be changed during call!
	 * @param chainContext context of one data preprocessing in preprocessors chain. May be null!
	 * @return preprocessed data - typically same object ad <code>data</code> parameter, but with changed structure.
	 * @since 1.3.1
	 * @since 1.2.9
	 */
	Map<String, Object> preprocessData(Map<String, Object> data, PreprocessChainContext chainContext);

}
