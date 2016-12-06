package de.julielab.elastic.query.components.data.query;

public class NestedQuery extends SearchServerQuery {
	public enum ScoreMode {
		sum, avg, min, max, none
	}

	public String path;
	/**
	 * Defaults to {@link ScoreMode#avg}.
	 */
	public ScoreMode scoreMode = ScoreMode.avg;
	public SearchServerQuery query;
	public InnerHits innerHits;
}
