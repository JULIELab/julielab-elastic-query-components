package de.julielab.elastic.query.components;

import de.julielab.elastic.query.components.data.SearchCarrier;
import de.julielab.elastic.query.services.ISearchServerResponse;

/**
 * Interface for the search component that wraps the concrete search server
 * calls, i.e. API-dependent calls to Solr or ElastiSearch.
 * 
 * @author faessler
 * 
 */
public interface ISearchServerComponent<C extends SearchCarrier<? extends ISearchServerResponse>> extends ISearchComponent<C> {
	String REVIEW_TERM = "Review";
}
