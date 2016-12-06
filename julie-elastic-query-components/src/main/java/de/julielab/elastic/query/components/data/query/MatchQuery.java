package de.julielab.elastic.query.components.data.query;

/**
 * <p>
 * For this query, the query string itself and a single field to search in is to be provided. The query string will be
 * analyzed for each field separately before matching. The resulting tokens will result in a boolean query, connected by
 * the operator given by the <tt>operator</tt> field which defaults to <tt>or</tt>. The query string is not parsed in
 * any way for boolean operators, field names, boosts etc.
 * </p>
 * <p>
 * This query type is closely related to {@link MultiMatchQuery} but can only use a single field for search.
 * </p>
 * 
 * @author faessler
 * 
 */
public class MatchQuery extends SearchServerQuery {
	/**
	 * The query. It is analyzed by the search server and the resulting tokens matched against the terms in
	 * {@link #field};
	 */
	public String query;
	/**
	 * The field to search in.
	 */
	public String field;
	/**
	 * The boolean operator to connect the tokens of the analyzed query with.
	 */
	public String operator = "or";
	public String analyzer;
	@Override
	public String toString() {
		return "MatchQuery [query=" + query + ", field=" + field + ", operator=" + operator + "]";
	}
}
