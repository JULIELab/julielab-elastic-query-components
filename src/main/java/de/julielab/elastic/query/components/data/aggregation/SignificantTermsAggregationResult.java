package de.julielab.elastic.query.components.data.aggregation;

import java.util.ArrayList;
import java.util.List;

public class SignificantTermsAggregationResult implements ISignificantTermsAggregationResult {
private String name;
private List<ISignificantTermsAggregationUnit> aggregationUnits;
	@Override
	public String getName() {
		return name;
	}

	@Override
	public List<ISignificantTermsAggregationUnit> getAggregationUnits() {
		return aggregationUnits;
	}
	
	public void addAggregationUnit(ISignificantTermsAggregationUnit unit) {
		if (null == aggregationUnits)
			aggregationUnits = new ArrayList<>();
		aggregationUnits.add(unit);
	}

	public void setName(String name) {
		this.name = name;
	}

}
