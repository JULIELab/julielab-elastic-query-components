package de.julielab.elastic.query.components.data.query;

public class WildcardQuery extends SearchServerQuery {
	public String query;
	public String field;
	@Override
	public String toString() {
		return "WildcardQuery [query=" + query + ", field=" + field + "]";
	}
}
