package de.julielab.elastic.query.components.data.aggregation;

import java.util.ArrayList;
import java.util.List;

import de.julielab.elastic.query.components.data.ISearchServerDocument;

public class TopHitsAggregationResult implements ITopHitsAggregationResult {
	private String name;

	private List<ISearchServerDocument> topHits;
	private long totalHits;
	public void addTopHitsDocument(ISearchServerDocument topHit) {
		if (null == topHits)
			topHits = new ArrayList<>();
		topHits.add(topHit);
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public long getNumberTotalHits() {
		return totalHits;
	}

	@Override
	public List<ISearchServerDocument> getTopHits() {
		return topHits;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setTotalHits(long totalHits) {
		this.totalHits = totalHits;
	}

}
