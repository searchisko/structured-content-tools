/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

/**
 * Content preprocessor which allows to Look up value over ElasticSearch search request containing some value from data
 * structure. This preprocessor requires ElasticSearch client to be passed into
 * {@link #init(String, org.elasticsearch.client.Client, Map)}! Example of configuration for this preprocessor for
 * single value lookup:
 * 
 * <pre>
 * { 
 *     "name"     : "EL Lookup transformer",
 *     "class"    : "org.jboss.elasticsearch.tools.content.ESLookupValuePreprocessor",
 *     "settings" : {
 *         "index_name"        : "projects",
 *         "index_type"        : "project",
 *         "source_field"      : "jira_project_code",
 *         "idx_search_field"  : "jira_project_code",
 *         "idx_result_field"  : "universal_code",
 *         "target_field"      : "project_code",
 *         "value_default"     : "unknown"
 *     } 
 * }
 * </pre>
 * 
 * Options are:
 * <ul>
 * <li><code>index_name<code> - name of search index to lookup values in
 * <li><code>index_type<code> - name of type in search index to lookup values in
 * <li><code>source_field<code> - source field in input data to be used as 'lookup key'. Dot notation for nested values can be used here.
 * <li><code>idx_search_field<code> - field in search index document to be asked for 'lookup key' obtained from source field. ElasticSearch <code>text</code>
 * filter is used against this field. Search is not performed if 'lookup key' is empty.
 * <li>
 * <code>idx_result_field<code> - field in found search index document to be placed into target field. First document from search index is used if more found (WARN is logged in this case).
 * <li><code>target_field<code> - target field in data to store looked up value into. Can be same as input field. Dot
 * notation can be used here for structure nesting.
 * <li><code>value_default</code> - optional default value used if lookup do not provide value. If not set then target
 * field is leaved empty for values not found in mapping. You can use pattern for keys replacement from other values in
 * data in default value. Keys are enclosed in curly braces, dot notation for deeper nesting may be used in keys.
 * Special key '<code>__original</code>' means that original value from source field will be used here. Example of
 * default value with replacement keys ' <code>No mapping found for value {__original}.</code> '.
 * <li><code>source_bases</code> - list of fields in source data which are used as bases for lookups evaluation. If
 * defined then lookup is performed for each of this fields, <code>source_field</code>, <code>target_field</code> and
 * keys in <code>value_default</code> are resolved relatively against this base. Base must provide object or list of
 * objects. See example later.
 * </ul>
 * 
 * 
 * Example of configuration for this preprocessor for lookup of multiple values of same structure:
 * 
 * <pre>
 * { 
 *     "name"     : "EL Lookup transformer",
 *     "class"    : "org.jboss.elasticsearch.tools.content.ESLookupValuePreprocessor",
 *     "settings" : {
 *         "index_name"        : "peoples",
 *         "index_type"        : "person",
 *         "source_field"      : "username",
 *         "idx_search_field"  : "user_name",
 *         "idx_result_field"  : "email_company",
 *         "target_field"      : "email",
 *         "source_bases"      : ["author", "editor", "comments.author"]
 *     } 
 * }
 * </pre>
 * 
 * So for example next input document:
 * 
 * <pre>
 * {
 *   "author": {
 *     "username" : "joe",
 *     "name"     : "Joe Gulf"
 *   },
 *   "editor": {
 *     "username" : "dan"
 *     "name"     : "Dan Doe"
 *   },
 *   "comments" : [
 *     {
 *       "text": "Posted first version",
 *       "author": {
 *         "username" : "joe",
 *         "name"     : "Joe Gulf"
 *        }
 *     },{
 *       "text": "We need to work on it little more.",
 *       "author": {
 *         "username" : "dan",
 *         "name"     : "Dan Doe"
 *        }
 *     }
 *   ]
 * }
 * </pre>
 * 
 * will be transformed to the:
 * 
 * <pre>
 * {
 *   "author": {
 *     "username" : "joe",
 *     "name"     : "Joe Gulf",
 *     "email"    : "joe@edit.com"
 *   },
 *   "editor": {
 *     "username" : "dan"
 *     "name"     : "Dan Doe",
 *     "email"    : "dan@edit.com"
 *   },
 *   "comments" : [
 *     {
 *       "text": "Posted first version",
 *       "author": {
 *         "username" : "joe",
 *         "name"     : "Joe Gulf",
 *         "email"    : "joe@edit.com"
 *        }
 *     },{
 *       "text": "We need to work on it little more.",
 *       "author": {
 *         "username" : "dan",
 *         "name"     : "Dan Doe",
 *         "email"    : "dan@edit.com"
 *        }
 *     }
 *   ]
 * }
 * 
 * <pre>
 * if there will be necessary documents in Elastic Search index called <code>peoples</code> with type <code>people</code>
 * with content like:
 * 
 * <pre>
 * { 
 *   "user_name"     : "joe",
 *   "email_company" : "joe@edit.com"
 *  }
 * </pre>
 * 
 * and
 * 
 * <pre>
 * { 
 *   "user_name"     : "dan",
 *   "email_company" : "dan@edit.com"
 *  }
 * </pre>
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 * @see StructuredContentPreprocessorFactory
 */
public class ESLookupValuePreprocessor extends StructuredContentPreprocessorBase {

  protected static final String CFG_index_name = "index_name";
  protected static final String CFG_index_type = "index_type";
  protected static final String CFG_source_field = "source_field";
  protected static final String CFG_idx_search_field = "idx_search_field";
  protected static final String CFG_idx_result_field = "idx_result_field";
  protected static final String CFG_target_field = "target_field";
  protected static final String CFG_value_default = "value_default";
  protected static final String CFG_source_bases = "source_bases";

  protected List<String> sourceBases;

  protected String indexName;
  protected String indexType;
  protected String sourceField;
  protected String idxSearchField;
  protected String idxResultField;
  protected String targetField;
  protected String valueDefault;

  @SuppressWarnings("unchecked")
  @Override
  public void init(Map<String, Object> settings) throws SettingsException {
    if (client == null) {
      throw new SettingsException("ElasticSearch client is required for preprocessor " + name);
    }
    if (settings == null) {
      throw new SettingsException("'settings' section is not defined for preprocessor " + name);
    }
    indexName = XContentMapValues.nodeStringValue(settings.get(CFG_index_name), null);
    validateConfigurationStringNotEmpty(indexName, CFG_index_name);
    indexType = XContentMapValues.nodeStringValue(settings.get(CFG_index_type), null);
    validateConfigurationStringNotEmpty(indexType, CFG_index_type);
    sourceField = XContentMapValues.nodeStringValue(settings.get(CFG_source_field), null);
    validateConfigurationStringNotEmpty(sourceField, CFG_source_field);
    targetField = XContentMapValues.nodeStringValue(settings.get(CFG_target_field), null);
    validateConfigurationStringNotEmpty(targetField, CFG_target_field);
    idxSearchField = XContentMapValues.nodeStringValue(settings.get(CFG_idx_search_field), null);
    validateConfigurationStringNotEmpty(idxSearchField, CFG_idx_search_field);
    idxResultField = XContentMapValues.nodeStringValue(settings.get(CFG_idx_result_field), null);
    validateConfigurationStringNotEmpty(idxResultField, CFG_idx_result_field);
    valueDefault = ValueUtils.trimToNull(XContentMapValues.nodeStringValue(settings.get(CFG_value_default), null));
    sourceBases = (List<String>) settings.get(CFG_source_bases);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> preprocessData(Map<String, Object> data) {
    if (data == null)
      return null;

    if (sourceBases == null) {
      processOneSourceValue(data, null);
    } else {
      LookupContenxt context = new LookupContenxt();
      for (String base : sourceBases) {
        Object obj = XContentMapValues.extractValue(base, data);
        if (obj != null) {
          if (obj instanceof Map) {
            processOneSourceValue((Map<String, Object>) obj, context);
          } else if (obj instanceof Collection) {
            for (Object o : (Collection<Object>) obj) {
              if (o instanceof Map) {
                processOneSourceValue((Map<String, Object>) o, context);
              } else {
                logger.warn("Source base {} contains collection with invalid value to be processed {}", base, obj);
              }
            }
          } else {
            logger.warn("Source base {} contains invalid value to be processed {}", base, obj);
          }
        }
      }
    }
    return data;
  }

  private void processOneSourceValue(Map<String, Object> data, LookupContenxt context) {
    Object sourceValue = XContentMapValues.extractValue(sourceField, data);
    Object targetValue = null;
    if (sourceValue instanceof Collection) {
      if (context == null)
        context = new LookupContenxt();
      @SuppressWarnings("unchecked")
      Collection<Object> col = (Collection<Object>) sourceValue;
      List<Object> colTarget = new ArrayList<Object>();
      for (Object o : col) {
        colTarget.add(lookupValue(o, data, context));
      }
      targetValue = colTarget;
    } else {
      targetValue = lookupValue(sourceValue, data, context);
    }

    StructureUtils.putValueIntoMapOfMaps(data, targetField, targetValue);
  }

  private boolean esExceptionWarned = false;

  /**
   * Perform lookup for one value in ES with default handling.
   * 
   * @param sourceValue to be looked up
   * @param data used in default pattern evaluation
   * @return looked up value - default already handled
   */
  protected Object lookupValue(Object sourceValue, Map<String, Object> data, LookupContenxt context) {
    Object value = null;

    if (sourceValue != null) {

      if (context != null && context.lookupCache.containsKey(sourceValue))
        return context.lookupCache.get(sourceValue);

      try {
        SearchRequestBuilder req = client.prepareSearch(indexName).setTypes(indexType).addField(idxResultField)
            .setQuery(QueryBuilders.matchAllQuery())
            .setFilter(FilterBuilders.queryFilter(QueryBuilders.matchQuery(idxSearchField, sourceValue)));

        SearchResponse resp = req.execute().actionGet();

        if (resp.getHits().getTotalHits() > 0) {
          if (resp.getHits().getTotalHits() > 1) {
            logger.warn("More results found for lookup over value {}", sourceValue);
          }
          SearchHit hit = resp.getHits().hits()[0];
          value = hit.field(idxResultField).getValue();
        }
        esExceptionWarned = false;
      } catch (ElasticSearchException e) {
        if (!esExceptionWarned) {
          esExceptionWarned = true;
          logger.warn("ElasticSearch lookup failed due '{}:{}' so default value is used for field instead", e
              .getClass().getName(), e.getMessage());
        }
      }
    }

    if (value == null && valueDefault != null)
      value = ValueUtils.processStringValuePatternReplacement(valueDefault, data, sourceValue);

    if (context != null && sourceValue != null)
      context.lookupCache.put(sourceValue, value);

    return value;
  }

  private class LookupContenxt {
    Map<Object, Object> lookupCache = new HashMap<Object, Object>();
  }

}
