/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import static org.jboss.elasticsearch.tools.content.IsDateInRangePreprocessor.CFG_CHECKED_DATE;
import static org.jboss.elasticsearch.tools.content.IsDateInRangePreprocessor.CFG_DEFAULT_DATE_FORMAT;
import static org.jboss.elasticsearch.tools.content.IsDateInRangePreprocessor.CFG_LEFT_DATE;
import static org.jboss.elasticsearch.tools.content.IsDateInRangePreprocessor.CFG_RESULT_FIELD;
import static org.jboss.elasticsearch.tools.content.IsDateInRangePreprocessor.CFG_RIGHT_DATE;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.elasticsearch.common.settings.SettingsException;
import org.jboss.elasticsearch.tools.content.testtools.TestUtils;
import org.junit.Test;

/**
 * Unit tests for {@link IsDateInRangePreprocessor}.
 * 
 * @author Ryszard Kozmik (rkozmik at redhat dot com)
 */
public class IsDateInRangePreprocessorTest {

	@Test
	public void init_settingerrors() {
		IsDateInRangePreprocessor tested = new IsDateInRangePreprocessor();
		Map<String, Object> settings = null;

		// case - settings mandatory
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals("'settings' section is not defined for preprocessor Test mapper", e.getMessage());
		}

		// case - source_field mandatory
		settings = new HashMap<String, Object>();
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals(
					"Missing or empty 'settings/"+CFG_CHECKED_DATE+"' configuration value for 'Test mapper' preprocessor",
					e.getMessage());
		}

		// case - result_field mandatory
		settings.put(CFG_CHECKED_DATE, "tested_date");
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals(
					"Missing or empty 'settings/"+CFG_RESULT_FIELD+"' configuration value for 'Test mapper' preprocessor",
					e.getMessage());
		}
		
		// case - at least one of left_date or right_date must be provided
		settings.put(CFG_RESULT_FIELD, "target");
		try {
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals(
					"At least one of dates defining range, settings/"+CFG_LEFT_DATE+" or settings/"+CFG_RIGHT_DATE+" need to be provided.",
					e.getMessage());
		}

		// case - no more mandatory setting fields
		settings.put(CFG_RIGHT_DATE, "right_date");
		tested.init("Test mapper", null, settings); 
	}

	@Test
	public void init() {
		
		// case - mandatory fields only filled in
		IsDateInRangePreprocessor tested = new IsDateInRangePreprocessor();
		Map<String, Object> settings = new HashMap<String, Object>();
		settings.put( CFG_CHECKED_DATE, "tested_date" );
		settings.put( CFG_RESULT_FIELD, "target" );
		settings.put( CFG_LEFT_DATE, "left_date" );

		tested.init( "Test mapper", null, settings );
		Assert.assertEquals( "Test mapper", tested.getName() );
		Assert.assertEquals( "tested_date", tested.checkedDateField );
		Assert.assertEquals( "target" , tested.resultField );
		Assert.assertEquals( "left_date" , tested.leftDateField );
		Assert.assertNull( tested.rightDateField );
		Assert.assertNull( tested.sourceBases );
		
		// Checking if default date format is used if not provided
		Assert.assertNotNull( tested.leftDateFormat );
		Assert.assertEquals( CFG_DEFAULT_DATE_FORMAT , tested.leftDateFormat );
		Assert.assertNotNull( tested.rightDateFormat );
		Assert.assertEquals( CFG_DEFAULT_DATE_FORMAT , tested.rightDateFormat );
	}

	@Test
	public void preprocessData() {
		IsDateInRangePreprocessor tested = new IsDateInRangePreprocessor();
		Map<String,Object> settings = TestUtils.loadJSONFromClasspathFile("/IsDateInRangePreprocessor_preprocessData.json");
		tested.init("Test mapper", null, settings );
		
		// case - not NPE
		tested.preprocessData(null, null);

		// case - nothing in input, so false in output as the value is not in the not given range
		{
			Map<String, Object> values = new HashMap<String, Object>();
			tested.preprocessData(values, null);
			Assert.assertTrue( "false".compareTo(values.get("result").toString())==0 );
		}
		
		// case - one of the dates is not parseable according to the given format then nothing is being done
		{
			Map<String,Object> values = new HashMap<String,Object>();
			values.put("start_date", "2011:01:01:00:01:01:000");
			tested.preprocessData(values,null);
			Assert.assertNull(values.get("result"));
		}
		
		// case - one of the dates is not given as a String value
		{
			Map<String,Object> values = new HashMap<String,Object>();
			List<Object> list = new LinkedList<Object>();
			values.put("end_date", list);
			
			tested.preprocessData(values,null);
			Assert.assertNull(values.get("result"));
		}
		
		// case - mixed left and right date. Even if right date is before the left one the check should still work.
		{
			Map<String,Object> values = new HashMap<String,Object>();
			values.put("start_date","2014-12-13");
			values.put("end_date","2011-12-13");
			values.put("tested_date","2013-12-13");
			tested.preprocessData(values,null);
			Assert.assertTrue( "true".compareTo(values.get("result").toString())==0);
		}
		
		// case - only left date is given for the range check
		{
			Map<String,Object> values = new HashMap<String,Object>();
			values.put("start_date","2014-12-13");
			values.put("tested_date","2013-12-13");
			tested.preprocessData(values,null);
			Assert.assertTrue( "false".compareTo(values.get("result").toString())==0);
		}
		
		// case - only left date is given for the range check
		{
			Map<String,Object> values = new HashMap<String,Object>();
			values.put("end_date","2014-12-13");
			values.put("tested_date","2013-12-13");
			tested.preprocessData(values,null);
			Assert.assertTrue( "true".compareTo(values.get("result").toString())==0);
		}
		
		// case - source based standard test
		{
			IsDateInRangePreprocessor sourceBasedTested = new IsDateInRangePreprocessor();
			Map<String,Object> sourceBasedSettings = TestUtils.loadJSONFromClasspathFile("/IsDateInRangePreprocessor_preprocessData.json");
			List<String> sourceBases = new LinkedList<String>();
			sourceBases.add("nested");
			sourceBasedSettings.put("source_bases", sourceBases);
			// Because with source bases the checked date is resolved against the root.
			sourceBasedSettings.put(CFG_CHECKED_DATE,"nested.tested_date");
			
			sourceBasedTested.init("Test mapper", null, sourceBasedSettings );
			
			Map<String,Object> nestedMap = new HashMap<String,Object>();
			Map<String,Object> values = new HashMap<String,Object>();
			values.put("nested", nestedMap);
			
			nestedMap.put("start_date","2014-12-13");
			nestedMap.put("end_date","2011-12-13");
			nestedMap.put("tested_date","2013-12-13");
			
			sourceBasedTested.preprocessData(values, null);
			Assert.assertTrue( "true".compareTo(nestedMap.get("result").toString())==0);
		}
	}
	
}
