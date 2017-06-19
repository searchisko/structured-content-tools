/*
 * JBoss, Home of Professional Open Source
 * Copyright 2017 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

/**
 * Http call exception.
 *
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class HttpCallException extends Exception {
    int statusCode;

    public HttpCallException(String url, int statusCode, String responseContent) {
        super("Failed remote system HTTP GET request to the url '" + url + "'. HTTP error code: " + statusCode
                + " Response body: " + responseContent);
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }
}
