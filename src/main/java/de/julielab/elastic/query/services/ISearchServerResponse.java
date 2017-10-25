package de.julielab.elastic.query.services;

import java.util.List;
import java.util.stream.Stream;

import de.julielab.elastic.query.components.data.ISearchServerDocument;
import de.julielab.elastic.query.components.data.QueryError;
import de.julielab.elastic.query.components.data.aggregation.AggregationRequest;
import de.julielab.elastic.query.components.data.aggregation.IAggregationResult;

public interface ISearchServerResponse {


	/**
	 * Returns the document results of the respective search request. For
	 * multi-batch requests like the ElasticSearch scroll request, this method
	 * wraps subsequent batch calls into the stream. Thus, depleting the stream
	 * will cause all requested documents to be read.
	 * 
	 * @return A Stream of search server documents reflecting the documents in the search response.
	 */
	Stream<ISearchServerDocument> getDocumentResults();

	IAggregationResult getAggregationResult(AggregationRequest aggCmd);

	long getNumFound();

	long getNumSuggestions();

	List<ISearchServerDocument> getSuggestionResults();

	boolean isSuggestionSearchResponse();

	void setSuggestionSearchResponse(boolean isSuggestionSearchResponse);
	
	QueryError getQueryError();
	
	String getQueryErrorMessage();

	boolean hasQueryError();

}
