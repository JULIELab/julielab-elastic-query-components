package de.julielab.elastic.query.components.data;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang.NotImplementedException;

public interface ISearchServerDocument {
    /**
     * Shortcut to {@link #getFieldValue(String)}.
     *
     * @param fieldName
     * @return
     */
    <V> Optional<V> get(String fieldName);

    <V> Optional<V> getFieldValue(String fieldName);

    Optional<List<Object>> getFieldValues(String fieldName);

    float getScore();

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
    default Map<String, List<ISearchServerDocument>> getInnerHits() {
        throw new NotImplementedException();
    }

    default String getId() {
        throw new NotImplementedException();
    }

    default Map<String, List<String>> getHighlights() {
        throw new NotImplementedException();
    }

    default <V> Optional<V> getFieldPayload(String fieldName) {
        throw new NotImplementedException();
    }
}
