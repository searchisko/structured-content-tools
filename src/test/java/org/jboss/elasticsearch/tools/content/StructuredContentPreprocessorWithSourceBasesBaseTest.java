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

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.SettingsException;
import org.jboss.elasticsearch.tools.content.testtools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit test for {@link StructuredContentPreprocessorWithSourceBasesBase}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class StructuredContentPreprocessorWithSourceBasesBaseTest {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test(expected = SettingsException.class)
	public void init_error() {
		StructuredContentPreprocessorWithSourceBasesBase tested = Mockito
				.mock(StructuredContentPreprocessorWithSourceBasesBase.class);
		Mockito.doCallRealMethod().when(tested).init(Mockito.anyMap());
		tested.init(null);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void init() {
		StructuredContentPreprocessorWithSourceBasesBase tested = Mockito
				.mock(StructuredContentPreprocessorWithSourceBasesBase.class);
		Mockito.doCallRealMethod().when(tested).init(Mockito.anyMap());
		Mockito.doCallRealMethod().when(tested).getSourceBases();

		Map<String, Object> settings = new HashMap<String, Object>();
		tested.init(settings);
		Assert.assertNull(tested.getSourceBases());

		List<String> baseslist = new ArrayList<String>();
		settings.put(StructuredContentPreprocessorWithSourceBasesBase.CFG_source_bases, baseslist);
		tested.init(settings);
		Assert.assertEquals(baseslist, tested.getSourceBases());
	}

	@Test
	public void getFullFieldName() {
		Assert.assertEquals("field", StructuredContentPreprocessorWithSourceBasesBase.getFullFieldName(null, "field"));
		Assert.assertEquals("base1.base2.field",
				StructuredContentPreprocessorWithSourceBasesBase.getFullFieldName("base1.base2", "field"));
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void preprocessData_noData() {
		StructuredContentPreprocessorWithSourceBasesBase tested = Mockito
				.mock(StructuredContentPreprocessorWithSourceBasesBase.class);
		Mockito.doCallRealMethod().when(tested).init(Mockito.anyMap());
		Mockito.doCallRealMethod().when(tested).preprocessData(Mockito.anyMap(), Mockito.any(PreprocessChainContext.class));
		Mockito.doCallRealMethod().when(tested).getSourceBases();
		Map<String, Object> settings = new HashMap<String, Object>();
		tested.init(settings);

		Assert.assertNull(tested.preprocessData(null, null));

		Mockito.verify(tested).init(settings);
		Mockito.verify(tested).preprocessData(Mockito.anyMap(), Mockito.any(PreprocessChainContext.class));
		Mockito.verify(tested, Mockito.times(0)).createContext();
		Mockito.verifyNoMoreInteractions(tested);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void preprocessData_noBases() {
		StructuredContentPreprocessorWithSourceBasesBase tested = Mockito
				.mock(StructuredContentPreprocessorWithSourceBasesBase.class);
		Mockito.doCallRealMethod().when(tested).init(Mockito.anyMap());
		Mockito.doCallRealMethod().when(tested).preprocessData(Mockito.anyMap(), Mockito.any(PreprocessChainContext.class));
		Mockito.doCallRealMethod().when(tested).getSourceBases();
		Map<String, Object> settings = new HashMap<String, Object>();
		tested.init(settings);

		Map<String, Object> data = new HashMap<String, Object>();
		Assert.assertEquals(data, tested.preprocessData(data, null));

		Mockito.verify(tested).init(settings);
		Mockito.verify(tested).preprocessData(data, null);
		Mockito.verify(tested).processOneSourceValue(data, null, null, null);
		Mockito.verify(tested, Mockito.times(0)).createContext();
		Mockito.verifyNoMoreInteractions(tested);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void preprocessData_bases() {
		StructuredContentPreprocessorWithSourceBasesBase tested = Mockito
				.mock(StructuredContentPreprocessorWithSourceBasesBase.class);
		tested.name = "mypreproc";
		Mockito.doCallRealMethod().when(tested).init(Mockito.anyMap());
		Mockito.doCallRealMethod().when(tested).preprocessData(Mockito.anyMap(), Mockito.any(PreprocessChainContext.class));
		Mockito.doCallRealMethod().when(tested).getSourceBases();
		Mockito.doCallRealMethod().when(tested)
				.addDataWarning(Mockito.any(PreprocessChainContext.class), Mockito.anyString());
		tested.logger = Mockito.mock(ESLogger.class);
		Object mockContext = new Object();
		Mockito.when(tested.createContext()).thenReturn(mockContext);

		Map<String, Object> settings = TestUtils.loadJSONFromClasspathFile("/ESLookupValue_preprocessData-bases.json");
		tested.init(settings);

		Map<String, Object> data = new HashMap<String, Object>();
		Map<String, Object> authorMock = new HashMap<String, Object>();
		data.put("author", authorMock);
		// will be skipped as it is bad type
		data.put("editor", "badtype");
		List<Object> commentsMock = new ArrayList<>();
		data.put("comments", commentsMock);
		Map<String, Object> comment1Mock = new HashMap<String, Object>();
		commentsMock.add(comment1Mock);

		// author missing - no exception
		Map<String, Object> editor1Mock = new HashMap<String, Object>();
		comment1Mock.put("editor", editor1Mock);

		Map<String, Object> comment2Mock = new HashMap<String, Object>();
		commentsMock.add(comment2Mock);
		Map<String, Object> author2Mock = new HashMap<String, Object>();
		comment2Mock.put("author", author2Mock);
		// will be skipped as it is bad type
		comment2Mock.put("editor", "bad type 2");

		PreprocessChainContextImpl chainContext = new PreprocessChainContextImpl();
		Assert.assertEquals(data, tested.preprocessData(data, chainContext));

		Mockito.verify(tested).init(settings);
		Mockito.verify(tested).preprocessData(data, chainContext);
		Mockito.verify(tested).processOneSourceValue(authorMock, mockContext, "author", chainContext);
		Mockito.verify(tested, Mockito.times(1)).processOneSourceValue(Mockito.eq(author2Mock), Mockito.eq(mockContext),
				Mockito.eq("comments.author"), Mockito.eq(chainContext));
		Mockito.verify(tested).processOneSourceValue(editor1Mock, mockContext, "comments.editor", chainContext);
		Mockito.verify(tested, Mockito.times(1)).createContext();
		Mockito.verify(tested, Mockito.times(2)).addDataWarning(Mockito.eq(chainContext), Mockito.anyString());
		Mockito.verifyNoMoreInteractions(tested);
		Assert.assertTrue(chainContext.isWarning());
		Assert.assertEquals(2, chainContext.getWarnings().size());
	}

}
