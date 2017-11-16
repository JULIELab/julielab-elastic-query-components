package de.julielab.elastic.query.components.data.query;

public class MatchPhraseQuery extends SearchServerQuery {
	public String field;
	public String phrase;
	/**
	 * The number of term movements allowed for each term in the matched
	 * document in comparison with the query terms to be still a match. Defaults
	 * to 0.
	 */
	public int slop = 0;
	@Override
	public String toString() {
		return "MatchPhraseQuery [" + field + ":" +phrase +"~"+slop+"]";
	}
	
}
