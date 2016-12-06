package de.julielab.elastic.query.services;

import java.util.List;
import java.util.Map;

import de.julielab.elastic.query.components.data.ISearchServerDocument;
import de.julielab.elastic.query.components.data.IFacetField;
import de.julielab.elastic.query.components.data.aggregation.AggregationCommand;
import de.julielab.elastic.query.components.data.aggregation.IAggregationResult;

public interface ISearchServerResponse {

	List<IFacetField> getFacetFields();

	List<ISearchServerDocument> getDocumentResults();
	
	IAggregationResult getAggregationResult(AggregationCommand aggCmd);

	long getNumFound();

	long getNumSuggestions();

	/**
	 * NOTE: Only returns not-nested highlighting.
	 * @return
	 */
	Map<String, Map<String, List<String>>> getHighlighting();

	List<ISearchServerDocument> getSuggestionResults();
	
	boolean isSuggestionSearchResponse();
	
	void setSuggestionSearchResponse(boolean isSuggestionSearchResponse);

}
