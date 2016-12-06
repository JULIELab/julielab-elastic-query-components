package de.julielab.elastic.query.components.data.aggregation;

import java.util.List;

public interface ITermsAggregationResult extends IAggregationResult {
	/**
	 * Returns the aggregated terms, their counts and sub-aggregation results, if any are present.
	 * @return
	 */
	List<ITermsAggregationUnit> getAggregationUnits();
}
