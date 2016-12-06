package de.julielab.elastic.query.components.data.query;

import java.util.ArrayList;
import java.util.List;

/**
 * This query type is equivalent to Lucene's <tt>BooleanQuery</tt>. It wraps around other queries and with that, forms
 * <em>clauses</em> with the operators <em>must</em> <em>should</em> and <em>must_not</em>. For a document to match, it
 * has to match all <tt>must</tt> clauses, gets better score for <tt>should</tt> clauses and is rejected for matching
 * <tt>must_not</tt> clauses. A clause may contain multiple nested queries. In this case, the three operators roughly
 * translate into boolean <em>and</em>, <em>or</em> and <em>not</em> operators, respectively.
 * 
 * @see http://lucene.apache.org/core/4_10_0/core/org/apache/lucene/search/BooleanQuery.html
 * @author faessler
 * 
 */
public class BoolQuery extends SearchServerQuery {
	
	public List<BoolClause> clauses;
	public String minimumShouldMatch;

	public void addClause(BoolClause clause) {
		if (null == clause)
			throw new IllegalArgumentException("The passed clause was null.");
		if (clause.queries.isEmpty())
			throw new IllegalArgumentException("The passed clause was empty.");
		if (null == clauses)
			clauses = new ArrayList<>();
		clauses.add(clause);
	}

	@Override
	public String toString() {
		return "BoolQuery [clauses=" + clauses + ", minimumShouldMatch=" + minimumShouldMatch + "]";
	}
}
