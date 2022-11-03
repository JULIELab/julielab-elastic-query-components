package de.julielab.elastic.query.components.data.query;

public class RangeQuery extends SearchServerQuery{
    public String field;
    public Object greaterThan;
    public Object greaterThanOrEqual;
    public Object lessThan;
    public Object lessThanOrEqual;
    public String format;
    public RangeRelation relation;
    public String timeZone;

    public enum RangeRelation {INTERSECTS, CONTAINS, WITHIN}
}
