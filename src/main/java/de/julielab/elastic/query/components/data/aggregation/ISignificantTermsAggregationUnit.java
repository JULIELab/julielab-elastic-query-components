package de.julielab.elastic.query.components.data.aggregation;

public interface ISignificantTermsAggregationUnit {
	String getTerm();
	long getDocCount();
}
