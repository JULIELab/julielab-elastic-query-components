package de.julielab.elastic.query.components.data.aggregation;

import java.util.List;

import de.julielab.elastic.query.components.data.ISearchServerDocument;

public interface ITopHitsAggregationResult extends IAggregationResult {
	long getNumberTotalHits();

	List<ISearchServerDocument> getTopHits();
}
