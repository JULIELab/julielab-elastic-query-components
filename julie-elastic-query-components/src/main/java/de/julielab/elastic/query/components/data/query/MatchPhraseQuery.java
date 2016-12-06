package de.julielab.elastic.query.components.data.query;

public class MatchPhraseQuery extends SearchServerQuery {
	public String phrase;
	public String field;
	public int slop;
}
