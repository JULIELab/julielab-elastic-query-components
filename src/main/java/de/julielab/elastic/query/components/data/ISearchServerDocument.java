package de.julielab.elastic.query.components.data;

import java.util.List;
import java.util.Map;

public interface ISearchServerDocument {
	/**
	 * Shortcut to {@link #getFieldValue(String)}.
	 * 
	 * @param fieldName
	 * @return
	 */
	<V> V get(String fieldName);

	<V> V getFieldValue(String fieldName);

	<V> V getFieldPayload(String fieldName);

	List<Object> getFieldValues(String fieldName);

	/**
	 * <p>
	 * Returns inner document hits, if existing. They exist if a nested query
	 * was performed and the inner hits were set to be returned and the result
	 * document actually has at least one queried inner hit. That means the
	 * field value of the nested field must be non-empty on the document.
	 * </p>
	 * <p>
	 * The keys of the map are the nested field names, e.g. "events" or
	 * "sentences". The values are the list of the inner hits - which are
	 * documents themselves - for the respective field.
	 * </p>
	 * 
	 * @return The inner hits of the document, ordered by nested field name.
	 */
	Map<String, List<ISearchServerDocument>> getInnerHits();

	String getId();

	String getIndexType();

	Map<String, List<String>> getHighlights();

	float getScore();
}
