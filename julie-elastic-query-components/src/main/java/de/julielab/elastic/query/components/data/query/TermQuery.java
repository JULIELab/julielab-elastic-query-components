package de.julielab.elastic.query.components.data.query;


/**
 * The simplest query: the delivered term is searched in the delivered field. No analysis, no parsing, just a string
 * match into the index.
 * 
 * @author faessler
 * 
 */
public class TermQuery extends SearchServerQuery {
	/**
	 * The term to look for in {@link #field}. Can be a string, a number or possibly other types. For more information,
	 * refer to the documentation of the used search server.
	 */
	public Object term;
	/**
	 * The field in which to look for {@link #term}.
	 */
	public String field;
}
