package de.julielab.elastic.query.components.data.query;

/**
 * <p>
 * A query formulated in the Lucene query syntax.
 * </p>
 * <p>
 * This is the most generic query type, using a query syntax parser in the search server, and can be used with the
 * common Solr query facilities and with the ElasticSearch <tt>query string</tt> query type.
 * </p>
 * 
 * @see http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html
 * @see http://www.solrtutorial.com/solr-query-syntax.html
 * 
 */
public class LuceneSyntaxQuery extends SearchServerQuery {

	public String query;
	public String defaultField;
	public String analyzer;
}
