/*
 * JBoss, Home of Professional Open Source
 * Copyright 2014 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import junit.framework.Assert;

import org.junit.Test;

/**
 * Unit test for {@link PreprocessChainContextImpl}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class PreprocessChainContextImplTest {

	@Test(expected = IllegalArgumentException.class)
	public void addDataWarning_noname() {
		PreprocessChainContextImpl tested = new PreprocessChainContextImpl();
		tested.addDataWarning(null, "msg");
	}

	@Test(expected = IllegalArgumentException.class)
	public void addDataWarning_nomsg() {
		PreprocessChainContextImpl tested = new PreprocessChainContextImpl();
		tested.addDataWarning("name", null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void addDataWarning_noany() {
		PreprocessChainContextImpl tested = new PreprocessChainContextImpl();
		tested.addDataWarning(null, null);
	}

	@Test
	public void addDataWarning() {
		PreprocessChainContextImpl tested = new PreprocessChainContextImpl();
		Assert.assertFalse(tested.isWarning());

		tested.addDataWarning("name1", "msg1");
		Assert.assertTrue(tested.isWarning());
		Assert.assertEquals(1, tested.getWarnings().size());
		Assert.assertEquals("name1", tested.getWarnings().get(0).getPreprocessorName());
		Assert.assertEquals("msg1", tested.getWarnings().get(0).getWarningMessage());

		tested.addDataWarning("name2", "msg2");
		Assert.assertTrue(tested.isWarning());
		Assert.assertEquals(2, tested.getWarnings().size());
		Assert.assertEquals("name1", tested.getWarnings().get(0).getPreprocessorName());
		Assert.assertEquals("msg1", tested.getWarnings().get(0).getWarningMessage());
		Assert.assertEquals("name2", tested.getWarnings().get(1).getPreprocessorName());
		Assert.assertEquals("msg2", tested.getWarnings().get(1).getWarningMessage());
	}

}
