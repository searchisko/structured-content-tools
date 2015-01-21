/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link ValueUtils}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class ValueUtilsTest {

	@Test
	public void isEmpty() {
		Assert.assertTrue(ValueUtils.isEmpty(null));
		Assert.assertTrue(ValueUtils.isEmpty(""));
		Assert.assertTrue(ValueUtils.isEmpty("     "));
		Assert.assertTrue(ValueUtils.isEmpty(" "));
		Assert.assertFalse(ValueUtils.isEmpty("a"));
		Assert.assertFalse(ValueUtils.isEmpty(" a"));
		Assert.assertFalse(ValueUtils.isEmpty("a "));
		Assert.assertFalse(ValueUtils.isEmpty(" a "));
	}

	@Test
	public void trimToNull() {
		Assert.assertNull(ValueUtils.trimToNull(null));
		Assert.assertNull(ValueUtils.trimToNull(""));
		Assert.assertNull(ValueUtils.trimToNull("     "));
		Assert.assertNull(ValueUtils.trimToNull(" "));
		Assert.assertEquals("a", ValueUtils.trimToNull("a"));
		Assert.assertEquals("a", ValueUtils.trimToNull(" a"));
		Assert.assertEquals("a", ValueUtils.trimToNull("a "));
		Assert.assertEquals("a", ValueUtils.trimToNull(" a "));
	}

	@Test
	public void parseCsvString() {
		Assert.assertNull(ValueUtils.parseCsvString(null));
		Assert.assertNull(ValueUtils.parseCsvString(""));
		Assert.assertNull(ValueUtils.parseCsvString("    "));
		Assert.assertNull(ValueUtils.parseCsvString("  ,, ,   ,   "));
		List<String> r = ValueUtils.parseCsvString(" ORG ,,UUUU, , PEM  , ,SU07  ");
		Assert.assertEquals(4, r.size());
		Assert.assertEquals("ORG", r.get(0));
		Assert.assertEquals("UUUU", r.get(1));
		Assert.assertEquals("PEM", r.get(2));
		Assert.assertEquals("SU07", r.get(3));
	}

	@Test
	public void createCsvString() {
		Assert.assertNull(ValueUtils.createCsvString(null));
		List<String> c = new ArrayList<String>();
		Assert.assertEquals("", ValueUtils.createCsvString(c));
		c.add("ahoj");
		Assert.assertEquals("ahoj", ValueUtils.createCsvString(c));
		c.add("b");
		c.add("task");
		Assert.assertEquals("ahoj,b,task", ValueUtils.createCsvString(c));
	}

	@Test
	public void processStringValuePatternReplacement() {

		// case - no NPE on empty patternValue
		Assert.assertNull(ValueUtils.processStringValuePatternReplacement(null, null, null));
		Map<String, Object> data = new HashMap<String, Object>();
		Assert.assertNull(ValueUtils.processStringValuePatternReplacement(null, data, null));

		// case - empty patternValue handling
		Assert.assertEquals("", ValueUtils.processStringValuePatternReplacement("", null, null));
		Assert.assertEquals("", ValueUtils.processStringValuePatternReplacement("", data, null));

		// case - patternValue without keys
		Assert.assertEquals("Ahoj", ValueUtils.processStringValuePatternReplacement("Ahoj", null, null));
		Assert.assertEquals("Ahoj", ValueUtils.processStringValuePatternReplacement("Ahoj", data, null));

		// case - unclosed braces
		Assert.assertEquals("Ahoj{", ValueUtils.processStringValuePatternReplacement("Ahoj{", data, null));
		Assert.assertEquals("Ahoj{doma", ValueUtils.processStringValuePatternReplacement("Ahoj{doma", data, null));
		Assert.assertEquals("{Ahoj", ValueUtils.processStringValuePatternReplacement("{Ahoj", data, null));

		// case - simple one level key - not in data
		Assert.assertEquals("Ahoj , welcome.",
				ValueUtils.processStringValuePatternReplacement("Ahoj {name}, welcome.", null, null));
		Assert.assertEquals("Ahoj , welcome.",
				ValueUtils.processStringValuePatternReplacement("Ahoj {name}, welcome.", data, null));
		Assert.assertEquals("", ValueUtils.processStringValuePatternReplacement("{name}", data, null));

		// case - simple one level key - String found in data
		data.put("name", "Joe");
		Assert.assertEquals("Ahoj Joe, welcome.",
				ValueUtils.processStringValuePatternReplacement("Ahoj {name}, welcome.", data, null));
		Assert.assertEquals("Joe", ValueUtils.processStringValuePatternReplacement("{name}", data, null));

		// case - simple one level key, multiple keys - Nonstring found in data
		data.put("count", new Integer(10));
		Assert.assertEquals("Ahoj Joe, welcome 10 times.",
				ValueUtils.processStringValuePatternReplacement("Ahoj {name}, welcome {count} times.", data, null));

		// case - unclosed braces after key
		Assert.assertEquals("Ahoj Joe, welcome 10 time{s.",
				ValueUtils.processStringValuePatternReplacement("Ahoj {name}, welcome {count} time{s.", data, null));

		// case - dot notation in key - not in data
		Assert.assertEquals("Ahoj , welcome.",
				ValueUtils.processStringValuePatternReplacement("Ahoj {user.name}, welcome.", null, null));
		Assert.assertEquals("Ahoj , welcome.",
				ValueUtils.processStringValuePatternReplacement("Ahoj {user.name}, welcome.", data, null));

		// case - dot notation in key - found in data
		{
			Map<String, Object> user = new HashMap<String, Object>();
			user.put("name", "Lena");
			data.put("user", user);
			Assert.assertEquals("Ahoj Lena, welcome 10 times.",
					ValueUtils.processStringValuePatternReplacement("Ahoj {user.name}, welcome {count} times.", data, null));

			Assert.assertEquals("10 - Lena",
					ValueUtils.processStringValuePatternReplacement("{count} - {user.name}", data, null));
		}

		// case - original value replacement mechanism tests
		Assert
				.assertEquals("Ahoj Pool, welcome Pool times.", ValueUtils.processStringValuePatternReplacement(
						"Ahoj {__original}, welcome {__original} times.", null, "Pool"));
		data.put("count", new Integer(10));
		Assert.assertEquals("Ahoj Pool, welcome 10 times.",
				ValueUtils.processStringValuePatternReplacement("Ahoj {__original}, welcome {count} times.", data, "Pool"));
		Assert.assertEquals("Ahoj , welcome 10 times.",
				ValueUtils.processStringValuePatternReplacement("Ahoj {__original}, welcome {count} times.", data, null));
	}

	@Test
	public void formatISODateTime() {
		Assert.assertNull(ValueUtils.formatISODateTime(null));
        Assert.assertEquals( ISODateTimeFormat.dateTime().withZoneUTC().print(1344945600000L),
                ValueUtils.formatISODateTime(new Date(1344945600000L))); 
		Assert.assertEquals(
				"2012-08-14T12:00:00.000Z",
				ValueUtils.formatISODateTime(ISODateTimeFormat.dateTimeParser().parseDateTime("2012-08-14T13:00:00.0+0100")
						.toDate()));
		
	}

}
