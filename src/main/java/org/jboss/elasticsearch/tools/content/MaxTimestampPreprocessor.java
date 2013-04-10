/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.util.Map;

import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

/**
 * Content preprocessor which finds maximal timestamp in defined source field and store it to some target field. Example
 * of configuration for this preprocessor:
 * 
 * <pre>
 * { 
 *     "name"     : "Updated field setter",
 *     "class"    : "org.jboss.dcp.api.util.MaxTimestampPreprocessor",
 *     "settings" : {
 *         "source_field" : "dcp_activity_dates",
 *         "target_field" : "dcp_last_activity_date"
 *     } 
 * }
 * </pre>
 * 
 * Options are:
 * <ul>
 * <li><code>source_field</code> - source field in data. Value is JSON array of Strings with ISO formated date time
 * value, eg. <code>2012-09-17T15:56:52.383+02:00</code>. Values with bad date format are ignored. If value is String
 * with valid date format it's copied to target field too.
 * <li><code>target_field</code> - target field in data to store biggest timestamp into. Value is String with ISO
 * formated date time value, eg. <code>2012-09-17T15:56:52.383+02:00</code>. <code>null</code> is given here if source
 * field is empty or do not contains any valid timestamp.
 * </ul>
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 * @see StructuredContentPreprocessorFactory
 */
public class MaxTimestampPreprocessor extends StructuredContentPreprocessorBase {

	protected static final String CFG_TARGET_FIELD = "target_field";
	protected static final String CFG_SOURCE_FIELD = "source_field";

	protected String fieldTarget;
	protected String fieldSource;

	@Override
	public void init(Map<String, Object> settings) throws SettingsException {
		if (settings == null) {
			throw new SettingsException("'settings' section is not defined for preprocessor " + name);
		}
		fieldTarget = XContentMapValues.nodeStringValue(settings.get(CFG_TARGET_FIELD), null);
		fieldSource = XContentMapValues.nodeStringValue(settings.get(CFG_SOURCE_FIELD), null);
		validateConfigurationStringNotEmpty(fieldSource, CFG_SOURCE_FIELD);
		validateConfigurationStringNotEmpty(fieldTarget, CFG_TARGET_FIELD);
	}

	@Override
	public Map<String, Object> preprocessData(Map<String, Object> data) {
		if (data == null)
			return null;

		String maxTimestamp = null;
		long maxTimestampParsed = 0;

		Object sourceData = XContentMapValues.extractValue(fieldSource, data);
		if (sourceData != null) {
			DateTimeFormatter dateParser = ISODateTimeFormat.dateTimeParser();
			if (sourceData instanceof Iterable) {
				for (Object o : (Iterable<?>) sourceData) {
					if (o instanceof String) {
						try {
							String timestamp = (String) o;
							if (timestamp != null && !timestamp.trim().isEmpty()) {
								long timestampParsed = dateParser.parseMillis(timestamp.trim());
								if (timestampParsed > maxTimestampParsed) {
									maxTimestampParsed = timestampParsed;
									maxTimestamp = timestamp;
								}
							}
						} catch (Exception e) {
							logger.debug("Value {} is not valid timestamp", 0);
						}
					}
				}
			} else if (sourceData instanceof String) {
				try {
					String timestamp = (String) sourceData;
					if (timestamp != null && !timestamp.trim().isEmpty()) {
						timestamp = timestamp.trim();
						// parse it to check format
						dateParser.parseMillis(timestamp);
						maxTimestamp = timestamp;
					}
				} catch (Exception e) {
					logger.debug("Value {} is not valid timestamp", 0);
				}
			} else {
				logger.debug("Value for field {} is not Iterable nor String but is {}", fieldSource, sourceData.getClass()
						.getName());
			}
		} else {
			logger.debug("Value for field {} not found in data", fieldSource);
		}

		logger.debug("Max timestamp found in {} is {}", fieldSource, maxTimestamp);

		StructureUtils.putValueIntoMapOfMaps(data, fieldTarget, maxTimestamp);
		return data;
	}

	public String getFieldTarget() {
		return fieldTarget;
	}

	public String getFieldSource() {
		return fieldSource;
	}

}
