package de.julielab.elastic.query.components.data.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BoolClause {
	public enum Occur {
		MUST, SHOULD, MUST_NOT, FILTER
	}

	public Occur occur;
	/**
	 * The queries that are supposed to be found in searched documents according
	 * to the {@link #occur} specification.
	 */
	public List<SearchServerQuery> queries = Collections.emptyList();

	public void addQuery(SearchServerQuery query) {
		if (null == query)
			throw new IllegalArgumentException("The passed query was null.");
		if (queries.isEmpty())
			queries = new ArrayList<>(3);
		queries.add(query);

	}

	@Override
	public String toString() {
		return "BoolClause [" + queries + ", " + occur + "]";
	}
}
