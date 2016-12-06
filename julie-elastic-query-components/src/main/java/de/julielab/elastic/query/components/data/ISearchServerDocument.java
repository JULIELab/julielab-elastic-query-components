package de.julielab.elastic.query.components.data;

import java.util.List;
import java.util.Map;

public interface ISearchServerDocument {
	/**
	 * Shortcut to {@link #getFieldValue(String)}.
	 * @param fieldName
	 * @return
	 */
	<V> V get(String fieldName);
	<V> V getFieldValue(String fieldName);
	<V> V getFieldPayload(String fieldName);
	List<Object> getFieldValues(String fieldName);
	Map<String, List<ISearchServerDocument>> getInnerHits();
	String getId();
	String getIndexType();
	Map<String, List<String>> getHighlights();
	float getScore();
}
