/*
 * JBoss, Home of Professional Open Source
 * Copyright 2014 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.elasticsearch.common.settings.SettingsException;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit test for {@link StructuredContentPreprocessorBase}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class StructuredContentPreprocessorBaseTest {

	@SuppressWarnings("unchecked")
	@Test
	public void preprocessData() {

		StructuredContentPreprocessorBase tested = Mockito.mock(StructuredContentPreprocessorMock.class);
		Mockito.doCallRealMethod().when(tested).preprocessData(Mockito.anyMap());

		Map<String, Object> data = new HashMap<String, Object>();
		tested.preprocessData(data);

		Mockito.verify(tested).preprocessData(data, null);

	}

	@Test
	public void addDataWarning() {
		StructuredContentPreprocessorBase tested = new StructuredContentPreprocessorMock();
		tested.name = "my preprocessor";

		// case - no exception when context is empty
		tested.addDataWarning(null, "msq");

		PreprocessChainContext contextMock = Mockito.mock(PreprocessChainContext.class);
		// case - exception if message is not provided
		try {
			tested.addDataWarning(contextMock, null);
			Assert.fail("IllegalArgumentException expected");
		} catch (IllegalArgumentException e) {
			Mockito.verifyZeroInteractions(contextMock);
			// OK
		}

		// case - successful add of warning
		Mockito.reset(contextMock);
		tested.addDataWarning(contextMock, "my message");
		Mockito.verify(contextMock).addDataWarning(tested.name, "my message");
		Mockito.verifyNoMoreInteractions(contextMock);
	}

	@Test
	public void validateConfigurationObjectNotEmpty() {
		StructuredContentPreprocessorBase tested = new StructuredContentPreprocessorMock();

		try {
			tested.validateConfigurationObjectNotEmpty(null, "cf");
			Assert.fail("SettingsException expected");
		} catch (SettingsException e) {
			// OK
		}

		try {
			tested.validateConfigurationObjectNotEmpty("", "cf");
			Assert.fail("SettingsException expected");
		} catch (SettingsException e) {
			// OK
		}

		try {
			tested.validateConfigurationObjectNotEmpty("   ", "cf");
			Assert.fail("SettingsException expected");
		} catch (SettingsException e) {
			// OK
		}

		try {
			tested.validateConfigurationObjectNotEmpty(new ArrayList<String>(), "cf");
			Assert.fail("SettingsException expected");
		} catch (SettingsException e) {
			// OK
		}

		{
			List<String> data = new ArrayList<String>();
			data.add("aa");
			tested.validateConfigurationObjectNotEmpty(data, "cf");
		}

		{
			tested.validateConfigurationObjectNotEmpty(new Integer(10), "cf");
		}

	}

}
