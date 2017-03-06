package de.julielab.elastic.query.services;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import de.julielab.elastic.query.components.data.IFacetField;
import de.julielab.elastic.query.components.data.ISearchServerDocument;
import de.julielab.elastic.query.components.data.aggregation.AggregationCommand;
import de.julielab.elastic.query.components.data.aggregation.IAggregationResult;

public interface ISearchServerResponse {

	/**
	 * 
	 * @return Facet fields, i.e. term aggregations
	 * @deprecated Use {@link #getAggregationResult(AggregationCommand)}
	 */
	@Deprecated
	List<IFacetField> getFacetFields();

	/**
	 * Returns the document results of the respective search request. For
	 * multi-batch requests like the ElasticSearch scroll request, this method
	 * wraps subsequent batch calls into the stream. Thus, depleting the stream
	 * will cause all requested documents to be read.
	 * 
	 * @return A Stream of search server documents reflecting the documents in the search response.
	 */
	Stream<ISearchServerDocument> getDocumentResults();

	/**
	 * NOTE: Only returns not-nested highlighting.
	 * 
	 * @return highlighting
	 * @deprecated It does not work with scanning to return document results and
	 *             highlighting separately. Thus, each document should know its
	 *             highlighting itself.
	 */
	@Deprecated
	Map<String, Map<String, List<String>>> getHighlighting();

	IAggregationResult getAggregationResult(AggregationCommand aggCmd);

	long getNumFound();

	long getNumSuggestions();

	List<ISearchServerDocument> getSuggestionResults();

	boolean isSuggestionSearchResponse();

	void setSuggestionSearchResponse(boolean isSuggestionSearchResponse);

}
