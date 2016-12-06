package de.julielab.elastic.query.components.data.aggregation;

import java.util.List;

public interface ISignificantTermsAggregationResult extends IAggregationResult {
	List<ISignificantTermsAggregationUnit> getAggregationUnits();
}
