package de.julielab.elastic.query.components.data;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;

public interface ISearchServerDocument {
	/**
	 * Shortcut to {@link #getFieldValue(String)}.
	 * 
	 * @param fieldName
	 * @return
	 */
	<V> V get(String fieldName);

	<V> V getFieldValue(String fieldName);

	List<Object> getFieldValues(String fieldName);

	float getScore();

	/**
	 * Expects a map from field name to a list of highlighted strings for the
	 * respective field.
	 * 
	 * @param highlightedFields
	 */
	default void setHighlighting(Map<String, List<String>> highlightedFields) {
		throw new NotImplementedException();
	};

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
	default public Map<String, List<ISearchServerDocument>> getInnerHits() {
		throw new NotImplementedException();
	}

	default public String getId() {
		throw new NotImplementedException();
	}

	default public String getIndexType() {
		throw new NotImplementedException();
	}

	default public Map<String, List<String>> getHighlights() {
		throw new NotImplementedException();
	}
	
	default <V> V getFieldPayload(String fieldName) {
		throw new NotImplementedException();
	}
}
