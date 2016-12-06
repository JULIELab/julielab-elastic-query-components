package de.julielab.elastic.query.components.data;


public class SortCommand {
	
	public SortCommand(String field, SortOrder order) {
		this.field = field;
		this.order = order;
	}
	public enum SortOrder { ASCENDING, DESCENDING }
	
	public String field;
	@Override
	public String toString() {
		return "SortCommand [field=" + field + ", order=" + order + "]";
	}
	public SortOrder order;
}
