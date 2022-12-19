/**
 * HighlightCommand.java
 * <p>
 * Copyright (c) 2013, JULIE Lab.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Common Public License v1.0
 * <p>
 * Author: faessler
 * <p>
 * Current version: 1.0
 * Since version:   1.0
 * <p>
 * Creation date: 08.04.2013
 */

/**
 *
 */
package de.julielab.elastic.query.components.data;

import java.util.ArrayList;
import java.util.List;

import de.julielab.elastic.query.components.data.query.SearchServerQuery;

public class HighlightCommand {
    public List<HlField> fields = new ArrayList<>();

    public HlField addField(String field, int fragnum, int fragsize) {
        if (null == fields)
            fields = new ArrayList<>();
        HlField hlField = new HlField();
        hlField.field = field;
        hlField.fragnum = fragnum;
        hlField.fragsize = fragsize;
        fields.add(hlField);
        return hlField;
    }

    public HlField addField(String field) {
        if (null == fields)
            fields = new ArrayList<>();
        HlField hlField = new HlField();
        hlField.field = field;
        hlField.fragnum = 3;
        hlField.fragsize = 100;
        fields.add(hlField);
        return hlField;
    }

    public static final class Highlighter {
        public static final String plain = "plain";
        public static final String fastvector = "fvh";
        public static final String postings = "postings";
    }

    public static class HlField {
        public int fragnum = Integer.MIN_VALUE;
        public int fragsize = Integer.MIN_VALUE;
        /**
         * If the field to be highlighted does not match anything, this
         * parameter specifies a default portion of the field value to be
         * returned anyway (without any highlighting, obviously).
         */
        public int noMatchSize = Integer.MIN_VALUE;
        public String field;
        public SearchServerQuery highlightQuery;
        public boolean requirefieldmatch = true;
        public String pre;// = "<em>";
        public String post;// = "</em>";
        public String type;
        public char[] boundaryChars;
        /**
         * One of <tt>chars</tt>, <tt>sentence</tt> or <tt>word</tt>.
         */
        public String boundaryScanner;
        public Integer boundaryMaxScan;
        public String boundaryScannerLocale;
        /**
         * The fields to collect highlights for. Supports the * wildcard.
         */
        public String[] fields;
        public boolean forceSource;
        /**
         * <tt>simple</tt> or <tt>span</tt>
         */
        public String fragmenter;
        /**
         * Only valid when using {@link Highlighter#fastvector} highlighter.
         */
        public Integer fragmentOffset;
        public String[] matchedFields;
        public String order;
        /**
         * Only valid when using {@link Highlighter#fastvector} highlighter.
         */
        public Integer phraseLimit;
        public String tagsSchema;
    }

}
