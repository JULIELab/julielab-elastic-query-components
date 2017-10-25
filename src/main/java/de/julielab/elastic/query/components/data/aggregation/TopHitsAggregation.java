package de.julielab.elastic.query.components.data.aggregation;

import java.util.ArrayList;
import java.util.List;

import de.julielab.elastic.query.components.data.SearchServerRequest;

/**
 * This aggregation collects the top document hits in its scope. This scope is given by the level of sub-aggregation
 * this aggregation is located on. This means, if this aggregation is a top-level aggregation, it basically returns the
 * main search result document list. As a sub-aggregation however, it returns the top scoring documents in the scope of
 * its super aggregation.
 * 
 * @see http 
 *      ://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-aggregations-metrics-top-hits-aggregation
 *      .html
 * @author faessler
 * 
 */
public class TopHitsAggregation extends AggregationRequest {
	/**
	 * <p>
	 * The fields for which values are to be returned. This is similar to the {@link SearchServerRequest#fieldsToReturn}
	 * parameter. The asterisk wildcard can be used to specify multiple or even all fields.
	 * </p>
	 * <p>
	 * <em>NOTE</em>: In ElasticSearch, for any field value to be returned, the special <tt>_source</tt> field must be
	 * activated at <em>index creation time</em>. For some reason, one cannot just return the stored fields of a
	 * document.
	 * </p>
	 */
	public List<String> includeFields;
	/**
	 * <p>
	 * The fields that should not be returned, even if in {@link #addIncludeField(String)} a wildcard expression did
	 * match the field. The asterisk wildcard can be used to specify multiple or even all fields.
	 * </p>
	 */
	public List<String> excludeFields;
	/**
	 * For number of documents to return. This is similar to the {@link SearchServerRequest#rows} parameter.
	 */
	public Integer size;

	public void addIncludeField(String field) {
		if (null == includeFields)
			includeFields = new ArrayList<>();
		includeFields.add(field);
	}
}
