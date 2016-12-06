package de.julielab.elastic.query.components.data.query;

import java.util.ArrayList;
import java.util.List;

public class DisMaxQuery extends SearchServerQuery {
	public double tieBreaker;
	public List<SearchServerQuery> queries;

	public void addQuery(SearchServerQuery query) {
		if (null == queries)
			queries = new ArrayList<>();
		queries.add(query);
	}
}
