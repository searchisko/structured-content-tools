/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.common.jackson.core.io.JsonStringEncoder;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.jboss.elasticsearch.tools.content.ValueUtils.IValueEncoder;

/**
 * Content preprocessor which allows to perform REST call based on data available in the data structure and map results
 * of the REST call back into the data structure. Authentication is not supported for now. Error from REST call is
 * catched, written to log file as <code>WARNING</code> and data warning is returned back to the calling application.
 * Example of configuration for this preprocessor:
 * 
 * <pre>
 * { 
 *     "name"     : "Data enhancer",
 *     "class"    : "org.jboss.elasticsearch.tools.content.RESTCallPreprocessor",
 *     "settings" : {
 *         "request_method" : "GET",
 *         "request_url"  : "http://test.org/api/getData?param1={id}&param2={type}",
 *         "response_mapping" : [ 
 *             {"rest_response_field":"code", "target_field":"project_code", "value_default":"unknown"},
 *             {"rest_response_field":"name", "target_field":"project_name"} 
 *         ]
 *     } 
 * }
 * </pre>
 * 
 * Options are:
 * <ul>
 * <li><code>request_method</code> - http method for REST request, possible values are "GET" and "POST". Defaults to
 * "GET".
 * <li><code>request_url</code> - url for REST request. This value can contain pattern for keys replacement from other
 * values in the data. Keys are enclosed in curly braces, dot notation for deeper nesting may be used in keys.
 * <li><code>request_timeout</code> - optional field defining timeout for REST request in milliseconds. Default value is
 * <code>10000</code>.
 * <li><code>request_max_parallel</code> - optional field defining how much of REST request may be performed in
 * parallel. Default value is <code>10</code>.
 * <li><code>request_user_agent_header</code> - optional field with value for <code>User-Agent</code> header used in REST
 * request. Default value is <code>SearchiskoContenPreprocessor (preprocessor name)</code>.
 * <li><code>request_accept_header</code> - optional field with value for <code>Accept</code> header used in REST
 * request. Default value is <code>application/json</code>.
 * <li><code>request_content_type_header</code> - optional field with value for <code>Content-Type</code> header used in
 * "POST" REST request. Default value is <code>application/json</code>.
 * <li><code>request_content</code> - template of JSON request content for "POST" type. This value can contain pattern
 * for keys replacement from other values in the data. Keys are enclosed in dollar sign (<code>$</code>), dot notation
 * for deeper nesting may be used in keys. Do not forget to escape quotation marks of content JSON there as template
 * must be valid JSON String in config!
 * <li>
 * <code>response_mapping<code> - array of mappings from REST call result to the data. Each mapping definition may contain these fields:
 * <ul>
 * <li><code>rest_response_field<code> - field in REST response to be placed into target field. You can use `_source` value there to get whole content of document returned by REST response. Dot notation may be used there for structure nesting.
 * <li><code>target_field<code> - target field in the data to store REST returned value into. Can be same as input field. Dot
 * notation can be used here for structure nesting.
 * <li><code>value_default</code> - optional default value used if REST call do not provide value. If not set then
 * target field is leaved empty for values not found in mapping. You can use pattern for keys replacement from other
 * values in the data in default value. Keys are enclosed in curly braces, dot notation for deeper nesting may be used
 * in keys.
 * </ul>
 * </ul>
 * 
 * Example for <code>POST<code> request:
 * 
 * <pre>
 * { 
 *     "name"     : "Data enhancer",
 *     "class"    : "org.jboss.elasticsearch.tools.content.RESTCallPreprocessor",
 *     "settings" : {
 *         "request_method" : "POST",
 *         "request_url"  : "http://test.org/api/getData",
 *         "request_content" : "{\"id\" : \"$id$\", \"type\" : \"$type$\" }",
 *         "response_mapping" : [ 
 *             {"rest_response_field":"code", "target_field":"project_code", "value_default":"unknown"},
 *             {"rest_response_field":"name", "target_field":"project_name"} 
 *         ]
 *     } 
 * }
 * </pre>
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 * @see StructuredContentPreprocessorFactory
 * @see ValueUtils#processStringValuePatternReplacement(String, Map, Object)
 */
public class RESTCallPreprocessor extends StructuredContentPreprocessorBase {

    protected static final String CFG_REQUEST_METHOD = "request_method";
    protected static final String CFG_REQUEST_URL = "request_url";
    protected static final String CFG_REQUEST_TIMEOUT = "request_timeout";
    protected static final String CFG_REQUEST_MAX_PARALLEL = "request_max_parallel";
    protected static final String CFG_REQUEST_ACCEPT_HEADER = "request_accept_header";
    protected static final String CFG_REQUEST_USER_AGENT_HEADER = "request_user_agent_header";
    protected static final String CFG_REQUEST_CONTENT_TYPE_HEADER = "request_content_type_header";
    protected static final String CFG_REQUEST_CONTENT = "request_content";
    protected static final String CFG_RESPONSE_MAPPING = "response_mapping";
    protected static final String CFG_rest_response_field = "rest_response_field";
    protected static final String CFG_target_field = "target_field";
    protected static final String CFG_value_default = "value_default";

    protected HttpMethodType request_method;
    protected String request_url;
    protected String request_content_template;
    protected Map<String, String> headers = new HashMap<>();
    protected List<Map<String, String>> responseMapping;

    protected CloseableHttpClient httpclient;

    @SuppressWarnings("unchecked")
    @Override
    public void init(Map<String, Object> settings) throws SettingsException {
        if (settings == null) {
            throw new SettingsException("'settings' section is not defined for preprocessor " + name);
        }

        String httpMethodStr = ValueUtils.trimToNull(XContentMapValues.nodeStringValue(settings.get(CFG_REQUEST_METHOD), null));
        request_method = httpMethodStr == null ? HttpMethodType.GET : HttpMethodType.valueOf(httpMethodStr);

        request_url = XContentMapValues.nodeStringValue(settings.get(CFG_REQUEST_URL), null);
        validateConfigurationStringNotEmpty(request_url, CFG_REQUEST_URL);

        responseMapping = (List<Map<String, String>>) settings.get(CFG_RESPONSE_MAPPING);
        validateResultMappingConfiguration(responseMapping, CFG_RESPONSE_MAPPING);

        request_content_template = XContentMapValues.nodeStringValue(settings.get(CFG_REQUEST_CONTENT), null);

        headers.put("Accept", XContentMapValues.nodeStringValue(settings.get(CFG_REQUEST_ACCEPT_HEADER), "application/json"));
        headers.put("Content-Type", XContentMapValues.nodeStringValue(settings.get(CFG_REQUEST_CONTENT_TYPE_HEADER), "application/json"));
        headers.put("User-Agent", "SearchiskoContenPreprocessor ("+getName()+")");
        
        initHttpClient(settings);
    }

    /**
     * Validate rest response mapping configuration part.
     * 
     * @param value to check
     * @param configFieldName name of field in preprocessor settings structure. Used for error message.
     * @throws SettingsException thrown if value is not valid
     */
    protected void validateResultMappingConfiguration(List<Map<String, String>> value, String configFieldName) throws SettingsException {
        if (value == null || value.isEmpty()) {
            throw new SettingsException("Missing or empty 'settings/" + configFieldName + "' configuration array for '" + name + "' preprocessor");
        }
        for (Map<String, String> mappingRecord : value) {
            if (ValueUtils.isEmpty(mappingRecord.get(CFG_rest_response_field))) {
                throw new SettingsException("Missing or empty 'settings/" + configFieldName + "/" + CFG_rest_response_field + "' configuration value for '" + name + "' preprocessor");
            }
            if (ValueUtils.isEmpty(mappingRecord.get(CFG_target_field))) {
                throw new SettingsException("Missing or empty 'settings/" + configFieldName + "/" + CFG_target_field + "' configuration value for '" + name + "' preprocessor");
            }
        }
    }

    @Override
    public Map<String, Object> preprocessData(Map<String, Object> data, PreprocessChainContext context) {
        if (data == null)
            return null;

        String url = prepareUrl(data);

        String content = prepareContent(data);

        try {
            HttpResponseContent resp = performHttpCall(url, content, headers, request_method);

            processResponse(data, resp);

        } catch (Exception e) {
            if (logger.isWarnEnabled())
                logger.warn("REST request failed: {}", e, e.getMessage());
            if (context != null)
                context.addDataWarning(getName(), "REST request failed due to: " + e.getMessage());
        }

        return data;
    }

    /**
     * @param data we are working with
     * @param response to process
     * @throws Exception
     */
    protected void processResponse(Map<String, Object> data, HttpResponseContent response) throws Exception {

        if (logger.isDebugEnabled())
            logger.debug("ResponseData: {}", response);

        Map<String, Object> responseParsed = ValueUtils.parseJSON(response.content);
        if (logger.isDebugEnabled())
            logger.debug("Parsed ResponseData: {}", responseParsed);

        if (logger.isDebugEnabled())
            logger.debug("Data before processing: {}", data);

        for (Map<String, String> mappingRecord : responseMapping) {
            String restResponseField = mappingRecord.get(CFG_rest_response_field);
            Object v = null;
            if ("_source".equals(restResponseField)) {
                v = responseParsed;
            } else {
                if (restResponseField.contains(".")) {
                    v = XContentMapValues.extractValue(restResponseField, responseParsed);
                } else {
                    v = responseParsed.get(restResponseField);
                }
            }

            if (v == null && mappingRecord.get(CFG_value_default) != null) {
                v = ValueUtils.processStringValuePatternReplacement(mappingRecord.get(CFG_value_default), data, null);
            }
            StructureUtils.putValueIntoMapOfMaps(data, mappingRecord.get(CFG_target_field), v);
        }

        if (logger.isDebugEnabled())
            logger.debug("Data after processing: {}", data);

    }

    protected static final IValueEncoder jsonValueEncoder = new IValueEncoder() {

        @Override
        public String encode(Object value) {
            if (value == null)
                return "";

            try {
                return new String(JsonStringEncoder.getInstance().quoteAsUTF8(value.toString()), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    };

    /**
     * Prepare content for request based on configuration.
     * 
     * @param data to be used in key replacement
     * @return content for request, always null for GET request
     * 
     * @see #request_content_template
     */
    protected String prepareContent(Map<String, Object> data) {
        if (HttpMethodType.POST.equals(request_method) && request_content_template != null) {
            return ValueUtils.processStringValuePatternReplacement(request_content_template, data, null, '$', '$', jsonValueEncoder);
        }
        return null;
    }

    protected static final IValueEncoder urlValueEncoder = new IValueEncoder() {

        @Override
        public String encode(Object value) {
            if (value == null)
                return "";

            try {
                return URLEncoder.encode(value.toString(), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    };

    /**
     * Prepare url for request based on configuration.
     * 
     * @param data to be used in key replacement
     * @return url for request
     * 
     * @see #request_url
     */
    protected String prepareUrl(Map<String, Object> data) {
        return ValueUtils.processStringValuePatternReplacement(request_url, data, null, urlValueEncoder);
    }

    /**
     * This method performs a HTTP request with the defined GET or POST method.
     * 
     * @param url to perform GET request for
     * @param content used in case of POST
     * @param headers to be used for request. Can be null.
     * @param method either GET(default) or POST http method type.
     * @return response from server if successful
     * @throws HttpCallException in case of failed http call (response other than 200)
     * @throws Exception in case of unsuccessful call
     */
    protected HttpResponseContent performHttpCall(String url, String content, Map<String, String> headers, HttpMethodType methodType) throws Exception, HttpCallException {

        if (logger.isDebugEnabled())
            logger.debug("Going to perform {} REST request to url {} with content: {} ", methodType, url, content);

        HttpRequestBase method = null;
        URIBuilder builder = new URIBuilder(url);
        if (methodType.equals(HttpMethodType.POST)) {
            HttpPost postMethod = new HttpPost(url);
            postMethod.setEntity(new StringEntity(content));
            method = postMethod;

        } else {
            method = new HttpGet(builder.build());
        }

        if (headers != null) {
            for (String headerName : headers.keySet())
                method.addHeader(headerName, headers.get(headerName));
        }
        CloseableHttpResponse response = null;
        try {
            HttpHost targetHost = new HttpHost(builder.getHost(), builder.getPort(), builder.getScheme());

            HttpClientContext localcontext = HttpClientContext.create();

            response = httpclient.execute(targetHost, method, localcontext);
            int statusCode = response.getStatusLine().getStatusCode();
            byte[] responseContent = null;
            if (response.getEntity() != null) {
                responseContent = EntityUtils.toByteArray(response.getEntity());
            }
            if (statusCode != HttpStatus.SC_OK) {
                throw new HttpCallException(url, statusCode, responseContent != null ? new String(responseContent) : "");
            }
            Header h = response.getFirstHeader("Content-Type");

            return new HttpResponseContent(h != null ? h.getValue() : null, responseContent);
        } finally {
            if (response != null)
                response.close();
            method.releaseConnection();
        }
    }

    /**
     * Prepare http client used for requests.
     * 
     * @param settings to be read config from
     */
    protected void initHttpClient(Map<String, Object> settings) {

        int maxParallel = XContentMapValues.nodeIntegerValue(settings.get(CFG_REQUEST_MAX_PARALLEL), 10);

        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setDefaultMaxPerRoute(maxParallel);
        connManager.setMaxTotal(maxParallel);

        ConnectionConfig connectionConfig = ConnectionConfig.custom().setCharset(Consts.UTF_8).build();
        connManager.setDefaultConnectionConfig(connectionConfig);

        HttpClientBuilder clientBuilder = HttpClients.custom().setConnectionManager(connManager);

        int timeout = XContentMapValues.nodeIntegerValue(settings.get(CFG_REQUEST_TIMEOUT), 10000);

        RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(timeout).setConnectTimeout(timeout).build();
        clientBuilder.setDefaultRequestConfig(requestConfig);

        httpclient = clientBuilder.build();

        logger.info("http client initialized");
    }

    @Override
    protected void finalize() throws Throwable {
        if (httpclient != null)
            httpclient.close();
        httpclient = null;
    }

    /**
     * Enum with supported REST http call methods.
     */
    public static enum HttpMethodType {
        GET, POST;
    }

    public static final class HttpResponseContent {
        public String contentType;
        public byte[] content;

        public HttpResponseContent(String contentType, byte[] content) {
            super();
            this.contentType = contentType;
            this.content = content;
        }

        @Override
        public String toString() {
            return "HttpResponseContent [contentType=" + contentType + ", content=" + content != null ? new String(content) : "" + "]";
        }

    }

}
