package de.julielab.elastic.query.components.data.aggregation;

import java.util.ArrayList;
import java.util.List;

public class TermsAggregationResult implements ITermsAggregationResult {

	private String name;
	private List<ITermsAggregationUnit> aggregationUnits;

	@Override
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public List<ITermsAggregationUnit> getAggregationUnits() {
		return aggregationUnits;
	}

	public void addAggregationUnit(ITermsAggregationUnit unit) {
		if (null == aggregationUnits)
			aggregationUnits = new ArrayList<>();
		aggregationUnits.add(unit);
	}

}
