package de.julielab.elastic.query.components.data.query;

/**
 * A meta-query class that cannot be used directly as a query to the search
 * server. Its purpose is to associate a query with a negative (<1) or positive
 * (>1) boost. Such a boost is only meaningful if there are other queries with
 * different boosts in a compound query, e.g. a {@link BoolQuery}. In such a
 * case, the boost gives a hint to the importance of a query (e.g. queries on
 * titles could be deemed more important than queries on the complete full text
 * of a document). The boost defaults to 1 (neutral).
 * 
 * @author faessler
 * @deprecated Was never needed, all queries may define their own boost
 */
@Deprecated
public class BoostedSearchServerQuery {
	public BoostedSearchServerQuery(SearchServerQuery query, float boost) {
		this.query = query;
		this.boost = boost;
	}

	/**
	 * Specifies the default boost of 1 (neutral).
	 * @param query
	 */
	public BoostedSearchServerQuery(SearchServerQuery query) {
		this(query, 1f);
	}

	public SearchServerQuery query;
	public float boost;
}
