/*
 * JBoss, Home of Professional Open Source
 * Copyright 2014 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

/**
 * Interface of context object used to pass into chain of preprocessors used to preprocess one data item.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public interface PreprocessChainContext {

	/**
	 * Add warning message about problem in data, so it may be consumed by calling application.
	 * 
	 * @param preprocessorName name of preprocessor producing warning
	 * @param warningMessage message with warning description. It is a good idea to write name of data field with problem
	 *          in this message.
	 * @see StructuredContentPreprocessorBase#addDataWarning(PreprocessChainContext, String)
	 * @throws IllegalArgumentException if any of two params is null
	 */
	public void addDataWarning(String preprocessorName, String warningMessage) throws IllegalArgumentException;

}
