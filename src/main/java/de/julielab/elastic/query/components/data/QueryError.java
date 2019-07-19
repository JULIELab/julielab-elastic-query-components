package de.julielab.elastic.query.components.data;

public enum QueryError {
	/**
	 * Indicates that the search server could be reached but it did not respond.
	 * The most like reason for this behavior would be an error on the server
	 * side, e.g. in a plugin.
	 */
	NO_RESPONSE,
	/**
	 * Indicates that no node of the configured ElasticSearch connection could
	 * be reached. This means that the given IP, port or cluster name did not
	 * lead to the successful discovery of a cluster. This could just mean that
	 * the cluster is down.
	 */
	NO_NODE_AVAILABLE,
	/**
	 * Indicates a general error during querying. The exact reasons are to be found in application and/or server logs.
	 */
	QUERY_ERROR
}
