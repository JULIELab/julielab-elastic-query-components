package de.julielab.elastic.query.components.data.aggregation;

public class SignificantTermsAggregation extends AggregationRequest {
	public String field;

	@Override
	public AggregationRequest clone() throws CloneNotSupportedException {
		return super.clone();
	}
}
