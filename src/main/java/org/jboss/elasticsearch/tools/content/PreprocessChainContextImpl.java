/*
 * JBoss, Home of Professional Open Source
 * Copyright 2014 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Basic implementation of {@link PreprocessChainContext}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class PreprocessChainContextImpl implements PreprocessChainContext {

	protected List<DataWarning> warnings = new ArrayList<>();

	@Override
	public void addDataWarning(String preprocessorName, String warningMessage) {
		if (preprocessorName == null || warningMessage == null) {
			throw new IllegalArgumentException("preprocessorName nor warningMessage can be null");
		}
		warnings.add(new DataWarning(preprocessorName, warningMessage));
	}

	/**
	 * Get list of watnings.
	 * 
	 * @return list of warnings, newer null
	 */
	public List<DataWarning> getWarnings() {
		return warnings;
	}

	/**
	 * Check if some warning is available.
	 * 
	 * @return true if there is any warning available.
	 */
	public boolean isWarning() {
		return !warnings.isEmpty();
	}

	@Override
	public String toString() {
		return "PreprocessChainContextImpl [warnings=" + warnings + "]";
	}

	public static final class DataWarning implements Serializable {
		private String preprocessorName;
		private String warningMessage;

		public DataWarning(String preprocessorName, String warningMessage) {
			super();
			this.preprocessorName = preprocessorName;
			this.warningMessage = warningMessage;
		}

		public String getPreprocessorName() {
			return preprocessorName;
		}

		public String getWarningMessage() {
			return warningMessage;
		}

		@Override
		public String toString() {
			return "DataWarning [preprocessorName=" + preprocessorName + ", warningMessage=" + warningMessage + "]";
		}

	}

}
