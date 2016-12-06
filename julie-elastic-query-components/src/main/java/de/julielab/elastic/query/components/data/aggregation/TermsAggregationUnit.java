package de.julielab.elastic.query.components.data.aggregation;

import java.util.HashMap;
import java.util.Map;

public class TermsAggregationUnit implements ITermsAggregationUnit {

	private Object term;
	private long count;
	private Map<String, IAggregationResult> subaggregationResults;

	public void setTerm(Object term) {
		this.term = term;
	}

	public void setCount(long count) {
		this.count = count;
	}

	@Override
	public Object getTerm() {
		return term;
	}

	@Override
	public long getCount() {
		return count;
	}

	@Override
	public Map<String, IAggregationResult> getSubaggregationResults() {
		return subaggregationResults;
	}

	public void addSubaggregationResult(IAggregationResult aggResult) {
		if (null == subaggregationResults)
			subaggregationResults = new HashMap<>();
		subaggregationResults.put(aggResult.getName(), aggResult);
	}

	@Override
	public IAggregationResult getSubaggregationResult(String name) {
		if (null != subaggregationResults)
			return subaggregationResults.get(name);
		return null;
	}

}
