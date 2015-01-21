/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.tools.content;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

/**
 * <pre>
 * { 
 *     "name"     : "Date range checker.",
 *     "class"    : "org.jboss.elasticsearch.tools.content.IsDateInRangePreprocessor",
 *     "settings" : {
 *         "left_date"  : "start_date",
 *         "left_date_format" : "yyyy-MM-dd'T'HH:mm:ss.SSSXX",
 *         "right_date"  : "end_date",
 *         "right_date_format"  : "yyyy-MM-dd'T'HH:mm:ss.SSSXX",
 *         "checked_date"  : "tested_date",
 *         "checked_date_format"  : "yyyy-MM-dd'T'HH:mm:ss.SSSXX",
 *         "result_field" : "result",
 *         "default_value" : "false"
 *     } 
 * }
 * </pre>
 * 
 * Options are:
 * <ul>
 * <li><code>left_date</code> - An optional parameter specifying location where left-hand side date can be found for
 * range checking. If not given an open range is assumed. However at least one of date parameters needs to be provided.
 * Dot notation for nested values can be used here (see {@link XContentMapValues#extractValue(String, Map)}).
 * <li><code>left_date_format</code> - This parameter defines date format for the left-hand side date. It's optional and
 * defaults to <code>yyyy-MM-dd'T'HH:mm:ss.SSSXX</code>
 * <li><code>right_date</code> - An optional parameter specifying location where right-hand side date can be found for
 * range checking. If not given an open range is assumed. However at least one of date parameters needs to be provided.
 * Dot notation for nested values can be used here (see {@link XContentMapValues#extractValue(String, Map)}).
 * <li><code>right_date_format</code> - This parameter defines date format for the right-hand side date. It's optional
 * and defaults to <code>yyyy-MM-dd'T'HH:mm:ss.SSSXX</code>
 * <li><code>checked_date</code> - The parameter specifies location where the date for range checking is located. Dot
 * notation for nested values can be used here (see {@link XContentMapValues#extractValue(String, Map)}).
 * <li><code>checked_date_format</code> - This parameter defines date format for the checked date. It's optional and
 * defaults to <code>yyyy-MM-dd'T'HH:mm:ss.SSSXX</code>
 * <li><code>result_field</code> - result field in data to store boolean result of comparison. Dot notation can be used
 * here for structure nesting.
 * <li><code>source_bases</code> - list of fields in source data which are used as bases. If defined then range
 * comparison is done for each of this fields. <code>left_date</code>, <code>right_date</code> and
 * <code>target_field</code> are resolved relatively against this base. Base must provide object or list of objects.
 * </ul>
 * 
 * @author Ryszard Kozmik (rkozmik at redhat dot com)
 * 
 */
public class IsDateInRangePreprocessor extends StructuredContentPreprocessorWithSourceBasesBase<Map<String, Object>> {

	protected static final String CFG_LEFT_DATE = "left_date";
	protected static final String CFG_RIGHT_DATE = "right_date";
	protected static final String CFG_CHECKED_DATE = "checked_date";
	protected static final String CFG_LEFT_DATE_FORMAT = "left_date_format";
	protected static final String CFG_RIGHT_DATE_FORMAT = "right_date_format";
	protected static final String CFG_CHECKED_DATE_FORMAT = "checked_date_format";
	protected static final String CFG_RESULT_FIELD = "result_field";
	protected static final String CFG_DEFAULT_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXX";

	protected SimpleDateFormat dateFormatter = new SimpleDateFormat();

	protected String leftDateField;
	protected String rightDateField;
	protected String checkedDateField;
	protected String resultField;
	protected String leftDateFormat;
	protected String rightDateFormat;
	protected String checkedDateFormat;

	@Override
	public void init(Map<String, Object> settings) throws SettingsException {

		super.init(settings);

		leftDateField = XContentMapValues.nodeStringValue(settings.get(CFG_LEFT_DATE), null);
		leftDateField = leftDateField != null && leftDateField.isEmpty() ? null : leftDateField;
		leftDateFormat = XContentMapValues.nodeStringValue(settings.get(CFG_LEFT_DATE_FORMAT), CFG_DEFAULT_DATE_FORMAT);

		rightDateField = XContentMapValues.nodeStringValue(settings.get(CFG_RIGHT_DATE), null);
		rightDateField = rightDateField != null && rightDateField.isEmpty() ? null : rightDateField;
		rightDateFormat = XContentMapValues.nodeStringValue(settings.get(CFG_RIGHT_DATE_FORMAT), CFG_DEFAULT_DATE_FORMAT);

		checkedDateField = XContentMapValues.nodeStringValue(settings.get(CFG_CHECKED_DATE), null);
		validateConfigurationObjectNotEmpty(checkedDateField, CFG_CHECKED_DATE);
		checkedDateFormat = XContentMapValues.nodeStringValue(settings.get(CFG_CHECKED_DATE_FORMAT),
				CFG_DEFAULT_DATE_FORMAT);

		resultField = XContentMapValues.nodeStringValue(settings.get(CFG_RESULT_FIELD), null);
		validateConfigurationStringNotEmpty(resultField, CFG_RESULT_FIELD);

		// At least one of date ranges fields need to be provided.
		if (leftDateField == null && rightDateField == null) {
			throw new SettingsException("At least one of dates defining range, settings/" + CFG_LEFT_DATE + " or settings/"
					+ CFG_RIGHT_DATE + " need to be provided.");
		}
	}

	@Override
	protected Map<String, Object> createContext(Map<String, Object> data) {
		return data;
	}

	@Override
	protected void processOneSourceValue(Map<String, Object> data, Map<String, Object> context, String base,
			PreprocessChainContext chainContext) {

		if (data == null)
			return;

		Boolean result = null;
		Date leftDate = null;
		Date rightDate = null;
		Date checkedDate = null;

		try {
			leftDate = handleDateExtractionAndParsing(leftDateField, leftDateFormat, data, base, chainContext);
			rightDate = handleDateExtractionAndParsing(rightDateField, rightDateFormat, data, base, chainContext);
			checkedDate = handleDateExtractionAndParsing(checkedDateField, checkedDateFormat,
					(base != null ? context : data), null, chainContext);
		} catch (DataProblemException e) {
			return;
		}

		// If needed we switch the dates around so that leftDate is before rightDate.
		if (leftDate != null && rightDate != null && leftDate.after(rightDate)) {
			Date tmpDate = leftDate;
			leftDate = rightDate;
			rightDate = tmpDate;
		}

		if (leftDate != null && rightDate != null) {
			result = checkedDate.compareTo(leftDate) >= 0 && checkedDate.compareTo(rightDate) <= 0 ? true : false;
		} else if (leftDate != null) {
			result = checkedDate.compareTo(leftDate) >= 0 ? true : false;
		} else if (rightDate != null) {
			result = checkedDate.compareTo(rightDate) <= 0 ? true : false;
		} else {
			result = false;
		}

		StructureUtils.putValueIntoMapOfMaps(data, resultField, result);
	}

	@Override
	public List<String> getSourceBases() {
		return sourceBases;
	}

	/**
	 * An util method to extract date value out from the field and parse it using the given date format.
	 * 
	 * @param settings
	 * @param cfgDateLocation
	 * @param cfgDateFormat
	 * @return parsed date object
	 */
	protected Date handleDateExtractionAndParsing(String dateField, String dateFormat, Map<String, Object> data,
			String base, PreprocessChainContext chainContext) throws DataProblemException {

		if (dateField == null)
			return null;

		Date resultDate = null;

		Object dateFieldData = null;
		if (dateField.contains(".")) {
			dateFieldData = XContentMapValues.extractValue(dateField, data);
		} else {
			dateFieldData = data.get(dateField);
		}

		if (dateFieldData != null) {
			if (!(dateFieldData instanceof String)) {
				String msg = "Value for field '" + dateField + "' is not a String, so can't be parsed to the date object.";
				addDataWarning(chainContext, msg);
				throw new DataProblemException();
			} else {
				String dateStr = dateFieldData.toString();
				if (dateStr != null && !dateStr.isEmpty()) {
				    synchronized(dateFormatter) {
    					dateFormatter.applyPattern(dateFormat);
    					try {
    						resultDate = dateFormatter.parse(dateStr);
    					} catch (ParseException e) {
    						String msg = dateField + " parameter value of " + dateStr + " could not be parsed using " + dateFormat
    								+ " format.";
    						addDataWarning(chainContext, msg);
    						throw new DataProblemException();
    					}
				    }
				}
			}
		}

		return resultDate;
	}

	/**
	 * Overrided warnings handler helps to save the information that any data parsing was problematic. If anything wrong
	 * happened during the processing of data, we don't want to preprocess on it further on.
	 */
	protected void addDataWarning(PreprocessChainContext chainContext, String message) {
		super.addDataWarning(chainContext, message);
		logger.debug(message);
	}

	/**
	 * An utility exception to handle data exceptions navigation nicely in this preprocessor.
	 */
	class DataProblemException extends Exception {
		private static final long serialVersionUID = 1L;
	}

}
