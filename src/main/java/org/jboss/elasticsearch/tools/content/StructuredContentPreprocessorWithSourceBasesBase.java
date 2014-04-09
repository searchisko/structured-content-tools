package org.jboss.elasticsearch.tools.content;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

/**
 * Abstract base class for preprocessors supporting concept of "source_bases".
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public abstract class StructuredContentPreprocessorWithSourceBasesBase<T> extends StructuredContentPreprocessorBase {

	protected static final String CFG_source_bases = "source_bases";

	protected List<String> sourceBases;

	@SuppressWarnings("unchecked")
	@Override
	public void init(Map<String, Object> settings) throws SettingsException {
		if (settings == null) {
			throw new SettingsException("'settings' section is not defined for preprocessor " + name);
		}
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
			T context = createContext();
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

	/**
	 * Do preprocessing of data. If "source_bases" concept is used then called multiple times for each base,
	 * <code>data<code> are relative for this base now.
	 * 
	 * @param data to run preprocessing on.
	 * @param context from {@link #createContext()}
	 */
	protected abstract void processOneSourceValue(Map<String, Object> data, T context);

	/**
	 * Create shared context object passed to each call of {@link #preprocessData(Map)} if "source_bases" concept is used.
	 * Not called when "source_bases" concept is not used.
	 * 
	 * @return context object or null
	 */
	protected abstract T createContext();

	/**
	 * Get configured source bases
	 * 
	 * @return source bases or null if not configured
	 */
	public List<String> getSourceBases() {
		return sourceBases;
	}

}
