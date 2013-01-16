/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

/**
 * Exception thrown from 'validating' preprocessors in case of invalid data.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 * @see RequiredValidatorPreprocessor
 */
public class InvalidDataException extends RuntimeException {

	/**
	 * Constructor.
	 * 
	 * @param message describing invalid data
	 */
	public InvalidDataException(String message) {
		super(message);
	}

}
