/*
 * JBoss, Home of Professional Open Source
 * Copyright 2017 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.jboss.elasticsearch.tools.content.RESTCallPreprocessor.HttpMethodType;
import org.jboss.elasticsearch.tools.content.RESTCallPreprocessor.HttpResponseContent;
import org.jboss.elasticsearch.tools.content.testtools.TestUtils;
import org.junit.Test;

import junit.framework.Assert;

/**
 * Unit test for {@link RESTCallPreprocessor}. It also contains <code>main</code> method for integration testing.
 *
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class RESTCallPreprocessorTest {
    
    //private static final String INT_TEST_CONFIG = "/RESTCallPreprocessor_integrationTest_GET.json";
    private static final String INT_TEST_CONFIG = "/RESTCallPreprocessor_integrationTest_POST.json";
    
    
    public static void main(String[] args) {
        RESTCallPreprocessor tested = getTested();
        Map<String, Object> settings = TestUtils.loadJSONFromClasspathFile(INT_TEST_CONFIG);
        tested.init(settings);
        
        Map<String, Object> data = new HashMap<>();
        data.put("id", "JBEAP-2377");
        data.put("type", "p&2\"aa");
        
        tested.preprocessData(data );
        
        tested.logger.info("Data after integration test {}", data);
    }
    
    @Test
    public void prepareContent(){
        RESTCallPreprocessor tested = getTested();
        tested.request_content_template = "{ \"test\" : \"$val$\", \"test2\":\"$val2.val$\"}";

        //always null for GET
        tested.request_method = HttpMethodType.GET;
        Assert.assertEquals(null, tested.prepareContent(null));
        
        
        tested.request_method = HttpMethodType.POST;
        Assert.assertEquals("{ \"test\" : \"\", \"test2\":\"\"}", tested.prepareContent(null));
        
        
        Map<String, Object> data = new HashMap<>();
        //we also test JSON encoding there!!
        data.put("val", "aha\" aha");
        StructureUtils.putValueIntoMapOfMaps(data, "val2.val", "jo \\ jo");
        Assert.assertEquals("{ \"test\" : \"aha\\\" aha\", \"test2\":\"jo \\\\ jo\"}", tested.prepareContent(data ));
        
    }
    
    @Test
    public void prepareUrl(){
        RESTCallPreprocessor tested = getTested();
        tested.request_url = "http://test.org/api/method?param1={val}&param2={val2.val}";
        
        Assert.assertEquals("http://test.org/api/method?param1=&param2=", tested.prepareUrl(null));
        Map<String, Object> data = new HashMap<>();
        //we also test URL encoding there!!
        data.put("val", "aha&aha");
        StructureUtils.putValueIntoMapOfMaps(data, "val2.val", "jo&jo");
        Assert.assertEquals("http://test.org/api/method?param1=aha%26aha&param2=jo%26jo", tested.prepareUrl(data));
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void processResponse_no_response_data() throws Exception{
        RESTCallPreprocessor tested = getTested();
        tested.responseMapping = (List<Map<String, String>>) TestUtils.loadJSONFromClasspathFile("/RESTCallPreprocessor_settings_correct_with_defaults.json").get(RESTCallPreprocessor.CFG_RESPONSE_MAPPING);
        
        
        Map<String, Object> data = new HashMap<>();
        
        byte[] content = new byte[]{};
        HttpResponseContent response = new HttpResponseContent("ct", content );
        tested.processResponse(data, response);
        Assert.assertEquals(3, data.size());
        Assert.assertEquals("unknown", data.get("project_code"));
        Assert.assertNotNull(data.get("project"));
        Assert.assertEquals(null, XContentMapValues.extractValue("project.project_name", data));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void processResponse_with_response_data() throws Exception{
        RESTCallPreprocessor tested = getTested();
        tested.responseMapping = (List<Map<String, String>>) TestUtils.loadJSONFromClasspathFile("/RESTCallPreprocessor_settings_correct_with_defaults.json").get(RESTCallPreprocessor.CFG_RESPONSE_MAPPING);
        
        
        Map<String, Object> data = new HashMap<>();
        
        byte[] content = "{\"code\":\"125\", \"name\":\"myproj\"}".getBytes();
        HttpResponseContent response = new HttpResponseContent("ct", content );
        tested.processResponse(data, response);
        Assert.assertEquals(3, data.size());
        Assert.assertEquals("125", data.get("project_code"));
        Assert.assertEquals("myproj", XContentMapValues.extractValue("project.project_name", data));
        Assert.assertNotNull(data.get("whole"));
    }

    @Test(expected = SettingsException.class)
    public void init_no_settings(){
        RESTCallPreprocessor tested = getTested();
        Map<String, Object> settings = null;
        tested.init(settings );
    }
    
    @Test(expected = SettingsException.class)
    public void init_no_url(){
        RESTCallPreprocessor tested = getTested();
        Map<String, Object> settings = TestUtils.loadJSONFromClasspathFile("/RESTCallPreprocessor_settings_nourl.json");
        tested.init(settings );
    }
    
    @Test(expected = SettingsException.class)
    public void init_no_response_mappings(){
        RESTCallPreprocessor tested = getTested();
        Map<String, Object> settings = TestUtils.loadJSONFromClasspathFile("/RESTCallPreprocessor_settings_noresponsemappings.json");
        tested.init(settings );
    }
    
    @Test
    public void init_OK(){
        RESTCallPreprocessor tested = getTested();
        Map<String, Object> settings = TestUtils.loadJSONFromClasspathFile("/RESTCallPreprocessor_settings_correct.json");
        tested.init(settings);
        
        Assert.assertNotNull(tested.httpclient);
        Assert.assertEquals(HttpMethodType.POST, tested.request_method);
        Assert.assertEquals("http://test.org/api/getData?param1={id}&param2={type}", tested.request_url);
        Assert.assertEquals("content $content$", tested.request_content_template);
        Assert.assertNotNull(tested.headers);
        Assert.assertEquals("ahead", tested.headers.get("Accept"));
        Assert.assertEquals("cthead", tested.headers.get("Content-Type"));
        Assert.assertEquals("SearchiskoContenPreprocessor (testPreproc)", tested.headers.get("User-Agent"));
        Assert.assertEquals(2, tested.responseMapping.size());
        
        
    }
    
    @Test
    public void init_OK_with_defaults(){
        RESTCallPreprocessor tested = getTested();
        Map<String, Object> settings = TestUtils.loadJSONFromClasspathFile("/RESTCallPreprocessor_settings_correct_with_defaults.json");
        tested.init(settings);
        
        Assert.assertNotNull(tested.httpclient);
        Assert.assertEquals(HttpMethodType.GET, tested.request_method);
        Assert.assertEquals("http://test.org/api/getData?param1={id}&param2={type}", tested.request_url);
        Assert.assertEquals(null, tested.request_content_template);
        Assert.assertNotNull(tested.headers);
        Assert.assertEquals("application/json", tested.headers.get("Accept"));
        Assert.assertEquals("application/json", tested.headers.get("Content-Type"));
        Assert.assertEquals(3, tested.responseMapping.size());
        
    }
    
    protected static RESTCallPreprocessor getTested(){
        RESTCallPreprocessor tested = new RESTCallPreprocessor();
        tested.name="testPreproc";
        return tested;
    }

}
