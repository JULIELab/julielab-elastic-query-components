package de.julielab.elastic.query.components.data.query;

import java.util.ArrayList;
import java.util.List;

/**
 * For this query, the query string itself and the fields, on which to perform the query, are separated (in contrast to
 * {@link LuceneSyntaxQuery}). The query string will be analyzed for each field separately before matching. The
 * resulting tokens will result in a boolean query, connected by the operator given by the <tt>operator</tt> field which
 * defaults to <tt>or</tt>. The query string is not parsed in any way for boolean operators, field names, boosts etc.
 * 
 * @author faessler
 * 
 */
public class MultiMatchQuery extends SearchServerQuery{

	/**
	 * Determines the scoring strategy for multi field searches. For example, only the best scoring field could be
	 * counted for each document, or the more fields are matched, the higher the score.
	 * 
	 * @see <pre>
	 * http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-multi-match-query.html#multi-match-types
	 * </pre>
	 * 
	 */
	public enum Type {
		best_fields, most_fields, cross_fields, phrase, phrase_prefix
	}

	public String query;
	public List<String> fields;
	/**
	 * Must be parallel to {@link #fields} or <tt>null</tt>, this means especially that
	 * <tt>fieldWeights</tt> must be of the same length as <tt>searchFields</tt>, if not <tt>null</tt>. Delivers
	 * field-based score boosting, a higher number means a higher boost.
	 */
	public List<Float> fieldWeights;
	public String operator = "or";
	/**
	 * @see Type
	 */
	public Type type;

	public void addField(String field) {
		if (null == fields)
			fields = new ArrayList<>();
		fields.add(field);
	}

	@Override
	public String toString() {
		StringBuilder fieldsSb = new StringBuilder();
		for (int i = 0; i < fields.size(); i++) {
			fieldsSb.append(fields.get(i));
			if (fieldWeights != null && i < fieldWeights.size())
				fieldsSb.append("^").append(String.valueOf(fieldWeights.get(i)));
			fieldsSb.append(",");
		}
		fieldsSb.deleteCharAt(fieldsSb.length() - 1);
		return "MultiMatch [fields:'" + fieldsSb + "', query: '" + query + "', operator: '"+operator+"', type: '" + type + "']";
	}
}
