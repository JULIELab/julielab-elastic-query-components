package de.julielab.elastic.query.services;

import de.julielab.elastic.query.components.data.QueryError;

/**
 * A super interface for all responses that can come from a search server or search technology, not restricted
 * to ElasticSearch.
 * It does not reveal any actual data because different search technologies return different kind of data. A topic model,
 * for example, does simply return a ranked list of document IDs but not actual documents. Subinterfaces of this
 * one add more capabilities. This interface is supposed to maintain a single set of service provider interfaces
 * for search, even when incorporating other search technologies than ElasticSearch.
 */
public interface ISearchServerResponse {
    long getNumFound();

    String getQueryErrorType();

    String getQueryErrorMessage();

    boolean hasQueryError();
}
