/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.jboss.elasticsearch.tools.content.testtools.ESRealClientTestBase;
import org.jboss.elasticsearch.tools.content.testtools.TestUtils;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit test for {@link ESLookupValuePreprocessor}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class ESLookupValuePreprocessorTest extends ESRealClientTestBase {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void init_settingerrors() {
		ESLookupValuePreprocessor tested = new ESLookupValuePreprocessor();
		Client client = Mockito.mock(Client.class);

		// case - ES client mandatory
		try {
			Map<String, Object> settings = TestUtils.loadJSONFromClasspathFile("/ESLookupValue_preprocessData-nobases.json");
			tested.init("Test mapper", null, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals("ElasticSearch client is required for preprocessor Test mapper", e.getMessage());
		}

		// case - settings mandatory
		try {
			tested.init("Test mapper", client, null);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals("'settings' section is not defined for preprocessor Test mapper", e.getMessage());
		}

		// case - all fields mandatory
		try {
			Map<String, Object> settings = TestUtils.loadJSONFromClasspathFile("/ESLookupValue_preprocessData-nobases.json");
			settings.remove(ESLookupValuePreprocessor.CFG_index_name);
			tested.init("Test mapper", client, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals("Missing or empty 'settings/index_name' configuration value for 'Test mapper' preprocessor",
					e.getMessage());
		}
		try {
			Map<String, Object> settings = TestUtils.loadJSONFromClasspathFile("/ESLookupValue_preprocessData-nobases.json");
			settings.remove(ESLookupValuePreprocessor.CFG_index_type);
			tested.init("Test mapper", client, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals("Missing or empty 'settings/index_type' configuration value for 'Test mapper' preprocessor",
					e.getMessage());
		}
		try {
			Map<String, Object> settings = TestUtils.loadJSONFromClasspathFile("/ESLookupValue_preprocessData-nobases.json");
			settings.remove(ESLookupValuePreprocessor.CFG_source_field);
			tested.init("Test mapper", client, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert
					.assertEquals(
							"At least one of 'settings/source_field' or 'settings/source_value' configuration value must be defined for 'Test mapper' preprocessor",
							e.getMessage());
		}
		try {
			Map<String, Object> settings = TestUtils.loadJSONFromClasspathFile("/ESLookupValue_preprocessData-nobases.json");
			((List<Map>) settings.get(ESLookupValuePreprocessor.CFG_result_mapping)).get(0).remove(
					ESLookupValuePreprocessor.CFG_target_field);
			tested.init("Test mapper", client, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals(
					"Missing or empty 'settings/result_mapping/target_field' configuration value for 'Test mapper' preprocessor",
					e.getMessage());
		}
		try {
			Map<String, Object> settings = TestUtils.loadJSONFromClasspathFile("/ESLookupValue_preprocessData-nobases.json");
			settings.remove(ESLookupValuePreprocessor.CFG_idx_search_field);
			tested.init("Test mapper", client, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert.assertEquals(
					"Missing or empty 'settings/idx_search_field' configuration value for 'Test mapper' preprocessor",
					e.getMessage());
		}
		try {
			Map<String, Object> settings = TestUtils.loadJSONFromClasspathFile("/ESLookupValue_preprocessData-nobases.json");
			((List<Map>) settings.get(ESLookupValuePreprocessor.CFG_result_mapping)).get(0).remove(
					ESLookupValuePreprocessor.CFG_idx_result_field);
			tested.init("Test mapper", client, settings);
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			Assert
					.assertEquals(
							"Missing or empty 'settings/result_mapping/idx_result_field' configuration value for 'Test mapper' preprocessor",
							e.getMessage());
		}

		// case - no more mandatory setting fields
		{
			Map<String, Object> settings = TestUtils.loadJSONFromClasspathFile("/ESLookupValue_preprocessData-nobases.json");
			tested.init("Test mapper", client, settings);
		}
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void init() {

		{
			ESLookupValuePreprocessor tested = new ESLookupValuePreprocessor();
			Client client = Mockito.mock(Client.class);

			Map<String, Object> settings = TestUtils.loadJSONFromClasspathFile("/ESLookupValue_preprocessData-nobases.json");
			tested.init("Test mapper", client, settings);
			Assert.assertEquals(client, tested.client);
			Assert.assertEquals("projects", tested.indexName);
			Assert.assertEquals("project", tested.indexType);
			Assert.assertEquals("fields.projectcode", tested.sourceField);
			Assert.assertNull(tested.sourceValuePattern);
			Assert.assertEquals("jbossorg_jira_project", tested.idxSearchField);
			Assert.assertEquals(2, ((List) tested.resultMapping).size());
			Assert.assertEquals("code", tested.resultMapping.get(0).get(ESLookupValuePreprocessor.CFG_idx_result_field));
			Assert.assertEquals("project.code", tested.resultMapping.get(0).get(ESLookupValuePreprocessor.CFG_target_field));
			Assert.assertEquals("defval", tested.resultMapping.get(0).get(ESLookupValuePreprocessor.CFG_value_default));
			Assert.assertEquals("name", tested.resultMapping.get(1).get(ESLookupValuePreprocessor.CFG_idx_result_field));
			Assert.assertEquals("project_name", tested.resultMapping.get(1).get(ESLookupValuePreprocessor.CFG_target_field));
			Assert.assertEquals(null, tested.resultMapping.get(1).get(ESLookupValuePreprocessor.CFG_value_default));
		}

		// case - sourceValue used instead of sourceField
		{
			ESLookupValuePreprocessor tested = new ESLookupValuePreprocessor();
			Client client = Mockito.mock(Client.class);

			Map<String, Object> settings = TestUtils.loadJSONFromClasspathFile("/ESLookupValue_preprocessData-nobases.json");
			settings.put(ESLookupValuePreprocessor.CFG_source_field, "");
			settings.put(ESLookupValuePreprocessor.CFG_source_value, "source {value}");
			tested.init("Test mapper", client, settings);
			Assert.assertEquals("source {value}", tested.sourceValuePattern);
			Assert.assertNull(tested.sourceField);

		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void preprocessData_nobases_sourceField() throws Exception {
		try {
			Client client = prepareESClientForUnitTest();

			ESLookupValuePreprocessor tested = new ESLookupValuePreprocessor();
			tested.init("Test mapper", client,
					TestUtils.loadJSONFromClasspathFile("/ESLookupValue_preprocessData-nobases.json"));

			// case - lookup index is missing so default value is used
			{
				Map<String, Object> values = new HashMap<String, Object>();
				StructureUtils.putValueIntoMapOfMaps(values, tested.sourceField, "AAA");
				tested.preprocessData(values);
				Assert.assertEquals("defval", (String) XContentMapValues.extractValue("project.code", values));
				Assert.assertNull(XContentMapValues.extractValue("project_name", values));

				// case - test rewrite on target field
				StructureUtils.putValueIntoMapOfMaps(values, tested.sourceField, "BBB");
				StructureUtils.putValueIntoMapOfMaps(values, "project.code", "jjj");
				StructureUtils.putValueIntoMapOfMaps(values, "project_name", "aaa");
				tested.preprocessData(values);
				Assert.assertEquals("defval", (String) XContentMapValues.extractValue("project.code", values));
				Assert.assertNull(XContentMapValues.extractValue("project_name", values));
			}

			// fill testing data
			prepareTestData(client, tested);

			// case - lookup for existing value
			{
				Map<String, Object> values = new HashMap<String, Object>();
				StructureUtils.putValueIntoMapOfMaps(values, tested.sourceField, "ORG");
				tested.preprocessData(values);
				Assert.assertEquals("jbossorg", (String) XContentMapValues.extractValue("project.code", values));
				Assert.assertEquals("jboss.org", (String) XContentMapValues.extractValue("project_name", values));

				StructureUtils.putValueIntoMapOfMaps(values, tested.sourceField, "ORGA");
				tested.preprocessData(values);
				Assert.assertEquals("jbossorg", (String) XContentMapValues.extractValue("project.code", values));
				Assert.assertEquals("jboss.org", (String) XContentMapValues.extractValue("project_name", values));

				StructureUtils.putValueIntoMapOfMaps(values, tested.sourceField, "ISPN");
				tested.preprocessData(values);
				Assert.assertEquals("infinispan", (String) XContentMapValues.extractValue("project.code", values));
				Assert.assertEquals("Infinispan", (String) XContentMapValues.extractValue("project_name", values));
			}

			// case - lookup for nonexisting value, test both default and not default value defined
			{
				Map<String, Object> values = new HashMap<String, Object>();
				StructureUtils.putValueIntoMapOfMaps(values, tested.sourceField, "AAA");
				tested.preprocessData(values);
				Assert.assertEquals("defval", (String) XContentMapValues.extractValue("project.code", values));
				Assert.assertNull(XContentMapValues.extractValue("project_name", values));

				// case - test rewrite on target field
				StructureUtils.putValueIntoMapOfMaps(values, tested.sourceField, "BBB");
				StructureUtils.putValueIntoMapOfMaps(values, "project.code", "jjj");
				StructureUtils.putValueIntoMapOfMaps(values, "project_name", "aaa");
				tested.preprocessData(values);
				Assert.assertEquals("defval", (String) XContentMapValues.extractValue("project.code", values));
				Assert.assertNull(XContentMapValues.extractValue("project_name", values));
			}

			// test rewrite on target field and default pattern
			{
				Map<String, Object> values = new HashMap<String, Object>();
				tested.resultMapping.get(0)
						.put(ESLookupValuePreprocessor.CFG_value_default, "unknown {field} for {__original}");
				StructureUtils.putValueIntoMapOfMaps(values, tested.sourceField, "BBB");
				StructureUtils.putValueIntoMapOfMaps(values, "field", "jj");
				tested.preprocessData(values);
				Assert.assertEquals("unknown jj for BBB", (String) XContentMapValues.extractValue("project.code", values));
			}

			// case - test handling when source field contains list of values
			{
				tested.resultMapping.get(0).put(ESLookupValuePreprocessor.CFG_value_default, "unknown");
				Map<String, Object> values = new HashMap<String, Object>();
				List<Object> obj = new ArrayList<Object>();
				obj.add("ORG");
				obj.add("ISPN");
				obj.add("AAA");
				StructureUtils.putValueIntoMapOfMaps(values, tested.sourceField, obj);
				tested.preprocessData(values);
				List<Object> l = (List<Object>) XContentMapValues.extractValue("project.code", values);
				Assert.assertEquals(3, l.size());
				Assert.assertTrue(l.contains("jbossorg"));
				Assert.assertTrue(l.contains("infinispan"));
				Assert.assertTrue(l.contains("unknown"));
				l = (List<Object>) XContentMapValues.extractValue("project_name", values);
				Assert.assertEquals(2, l.size());
				Assert.assertTrue(l.contains("jboss.org"));
				Assert.assertTrue(l.contains("Infinispan"));
			}

		} finally {
			finalizeESClientForUnitTest();
		}
	}

	@Test
	public void preprocessData_nobases_sourceValue() throws Exception {
		try {
			Client client = prepareESClientForUnitTest();

			String testInputField = "testinputfield";

			Map<String, Object> settings = TestUtils.loadJSONFromClasspathFile("/ESLookupValue_preprocessData-nobases.json");
			settings.remove(ESLookupValuePreprocessor.CFG_source_field);
			settings.put(ESLookupValuePreprocessor.CFG_source_value, "{" + testInputField + "}");

			ESLookupValuePreprocessor tested = new ESLookupValuePreprocessor();
			tested.init("Test mapper", client, settings);

			prepareTestData(client, tested);

			// case - lookup for existing value - simple pattern in source_value
			{
				settings.put(ESLookupValuePreprocessor.CFG_source_value, "{" + testInputField + "}");
				tested = new ESLookupValuePreprocessor();
				tested.init("Test mapper", client, settings);

				Map<String, Object> values = new HashMap<String, Object>();
				StructureUtils.putValueIntoMapOfMaps(values, testInputField, "ORG");
				tested.preprocessData(values);
				Assert.assertEquals("jbossorg", (String) XContentMapValues.extractValue("project.code", values));
				Assert.assertEquals("jboss.org", (String) XContentMapValues.extractValue("project_name", values));

				StructureUtils.putValueIntoMapOfMaps(values, testInputField, "ORGA");
				tested.preprocessData(values);
				Assert.assertEquals("jbossorg", (String) XContentMapValues.extractValue("project.code", values));
				Assert.assertEquals("jboss.org", (String) XContentMapValues.extractValue("project_name", values));

				StructureUtils.putValueIntoMapOfMaps(values, testInputField, "ISPN");
				tested.preprocessData(values);
				Assert.assertEquals("infinispan", (String) XContentMapValues.extractValue("project.code", values));
				Assert.assertEquals("Infinispan", (String) XContentMapValues.extractValue("project_name", values));
			}

			// case - lookup for existing value - complete pattern in source_value
			{
				settings.put(ESLookupValuePreprocessor.CFG_source_value, "{sv1}.{sv2}");
				tested = new ESLookupValuePreprocessor();
				tested.init("Test mapper", client, settings);

				Map<String, Object> values = new HashMap<String, Object>();
				StructureUtils.putValueIntoMapOfMaps(values, "sv1", "test");
				StructureUtils.putValueIntoMapOfMaps(values, "sv2", "org");
				tested.preprocessData(values);
				Assert.assertEquals("jbossorg", (String) XContentMapValues.extractValue("project.code", values));
				Assert.assertEquals("jboss.org", (String) XContentMapValues.extractValue("project_name", values));
			}

			// case - lookup for nonexisting value, test both default and not default value defined
			{
				settings.put(ESLookupValuePreprocessor.CFG_source_value, "{" + testInputField + "}");
				tested = new ESLookupValuePreprocessor();
				tested.init("Test mapper", client, settings);

				Map<String, Object> values = new HashMap<String, Object>();
				StructureUtils.putValueIntoMapOfMaps(values, testInputField, "AAA");
				tested.preprocessData(values);
				Assert.assertEquals("defval", (String) XContentMapValues.extractValue("project.code", values));
				Assert.assertNull(XContentMapValues.extractValue("project_name", values));

				// case - test rewrite on target field
				StructureUtils.putValueIntoMapOfMaps(values, testInputField, "BBB");
				StructureUtils.putValueIntoMapOfMaps(values, "project.code", "jjj");
				StructureUtils.putValueIntoMapOfMaps(values, "project_name", "aaa");
				tested.preprocessData(values);
				Assert.assertEquals("defval", (String) XContentMapValues.extractValue("project.code", values));
				Assert.assertNull(XContentMapValues.extractValue("project_name", values));
			}

			// test rewrite on target field and default pattern
			{
				settings.put(ESLookupValuePreprocessor.CFG_source_value, "{" + testInputField + "}");
				tested = new ESLookupValuePreprocessor();
				tested.init("Test mapper", client, settings);

				Map<String, Object> values = new HashMap<String, Object>();
				tested.resultMapping.get(0)
						.put(ESLookupValuePreprocessor.CFG_value_default, "unknown {field} for {__original}");
				StructureUtils.putValueIntoMapOfMaps(values, testInputField, "BBB");
				StructureUtils.putValueIntoMapOfMaps(values, "field", "jj");
				tested.preprocessData(values);
				Assert.assertEquals("unknown jj for BBB", (String) XContentMapValues.extractValue("project.code", values));
			}

		} finally {
			finalizeESClientForUnitTest();
		}
	}

	@Test
	public void preprocessData_bases_sourceField() throws Exception {
		try {
			Client client = prepareESClientForUnitTest();

			ESLookupValuePreprocessor tested = new ESLookupValuePreprocessor();
			tested.init("Test mapper", client,
					TestUtils.loadJSONFromClasspathFile("/ESLookupValue_preprocessData-bases.json"));

			// fill testing data
			prepareTestData(client, tested);

			// case - lookup for existing value
			{
				Map<String, Object> values = new HashMap<String, Object>();
				values.put("author", createProjectStructureMap("ORG", "jboss.org project"));
				values.put("editor", createProjectStructureMap("ISPN", "Infinispan"));
				List<Map<String, Object>> comments = new ArrayList<Map<String, Object>>();
				values.put("comments", comments);

				Map<String, Object> comment1 = new HashMap<String, Object>();
				comment1.put("author", createProjectStructureMap("ORG", "jboss.org project"));
				comment1.put("editor", createProjectStructureMap("ORG", "jboss.org project"));
				comments.add(comment1);

				Map<String, Object> comment2 = new HashMap<String, Object>();
				comment2.put("author", createProjectStructureMap("ISPN", "Infinispan"));
				comments.add(comment2);

				tested.preprocessData(values);

				assertProjectStructure(values.get("author"), "ORG", "jboss.org project", "jbossorg");
				assertProjectStructure(values.get("editor"), "ISPN", "Infinispan", "infinispan");

				Assert.assertEquals(2, comments.size());
				assertProjectStructure(comment1.get("author"), "ORG", "jboss.org project", "jbossorg");
				assertProjectStructure(comment1.get("editor"), "ORG", "jboss.org project", "jbossorg");
				assertProjectStructure(comment2.get("author"), "ISPN", "Infinispan", "infinispan");
			}

		} finally {
			finalizeESClientForUnitTest();
		}
	}

	private void prepareTestData(Client client, ESLookupValuePreprocessor tested) {
		// fill testing data
		client.admin().indices().prepareCreate(tested.indexName).execute().actionGet();
		client.admin().indices().preparePutMapping(tested.indexName).setType(tested.indexType)
				.setSource(TestUtils.loadJSONFromClasspathFile("/ESLookupValue_preprocessData-mapping.json")).execute()
				.actionGet();
		client.prepareIndex(tested.indexName, tested.indexType).setId("data1")
				.setSource(TestUtils.loadJSONFromClasspathFile("/ESLookupValue_preprocessData_data1.json")).execute()
				.actionGet();
		client.prepareIndex(tested.indexName, tested.indexType).setId("data2")
				.setSource(TestUtils.loadJSONFromClasspathFile("/ESLookupValue_preprocessData_data2.json")).execute()
				.actionGet();
		client.prepareIndex(tested.indexName, tested.indexType).setId("data3")
				.setSource(TestUtils.loadJSONFromClasspathFile("/ESLookupValue_preprocessData_data3.json")).execute()
				.actionGet();
		client.admin().indices().prepareRefresh(tested.indexName).execute().actionGet();
	}

	private Map<String, Object> createProjectStructureMap(String projectcode, String projectname) {
		Map<String, Object> ret = new HashMap<String, Object>();
		ret.put("projectcode", projectcode);
		ret.put("projectname", projectname);
		return ret;
	}

	@SuppressWarnings("unchecked")
	private void assertProjectStructure(Object dataObj, String projectcode, String projectname, String transformedcode) {
		Map<String, Object> data = (Map<String, Object>) dataObj;
		Assert.assertEquals(projectcode, data.get("projectcode"));
		Assert.assertEquals(projectname, data.get("projectname"));
		Assert.assertEquals(transformedcode, data.get("transformedcode"));
	}
}
