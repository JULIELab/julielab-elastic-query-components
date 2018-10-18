package de.julielab.elastic.query.components.data.query;

import java.util.List;

public class SimpleQueryStringQuery extends SearchServerQuery {
    public String query;
    public List<String> fields;
    public List<Float> fieldBoosts;
    public Operator defaultOperator;
    public String analyzer;
    public List<Flag> flags;
    public boolean analyzeWildcard;
    public boolean lenient;
    public String minimumShouldMatch;
    public String quoteFieldSuffix;
    public enum Operator {AND, OR}
    public enum Flag {
        ALL,
        NONE,
        AND,
        NOT,
        OR,
        PREFIX,
        PHRASE,
        PRECEDENCE,
        ESCAPE,
        WHITESPACE,
        FUZZY,
        // NEAR and SLOP are synonymous, since "slop" is a more familiar term than "near"
        NEAR,
        SLOP
    }
}
