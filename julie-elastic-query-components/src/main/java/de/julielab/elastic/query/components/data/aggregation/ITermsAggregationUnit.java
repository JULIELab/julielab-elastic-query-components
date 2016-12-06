package de.julielab.elastic.query.components.data.aggregation;

import java.util.Map;

public interface ITermsAggregationUnit {
	Object getTerm();
	long getCount();
	Map<String, IAggregationResult> getSubaggregationResults();
	IAggregationResult getSubaggregationResult(String name);
}
