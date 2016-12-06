package de.julielab.elastic.query.components.data.aggregation;

public class SignificantTermsAggregationUnit implements ISignificantTermsAggregationUnit {
	private String term;
	private long docCount;

	public void setTerm(String term) {
		this.term = term;
	}

	public void setDocCount(long docCount) {
		this.docCount = docCount;
	}

	@Override
	public String getTerm() {
		return term;
	}

	@Override
	public long getDocCount() {
		return docCount;
	}

}
