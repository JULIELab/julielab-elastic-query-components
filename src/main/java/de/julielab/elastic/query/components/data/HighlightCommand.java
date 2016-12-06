/**
 * HighlightCommand.java
 *
 * Copyright (c) 2013, JULIE Lab.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Common Public License v1.0
 *
 * Author: faessler
 *
 * Current version: 1.0
 * Since version:   1.0
 *
 * Creation date: 08.04.2013
 **/

/**
 * 
 */
package de.julielab.elastic.query.components.data;

import java.util.ArrayList;
import java.util.List;

import de.julielab.elastic.query.components.data.query.SearchServerQuery;

public class HighlightCommand {
	public static final class Highlighter {
		public static final String plain = "plain";
		public static final String fastvector = "fvh";
		public static final String postings = "postings";
	}

	public static class HlField {
		public int fragnum = Integer.MIN_VALUE;
		public int fragsize = Integer.MIN_VALUE;
		public String field;
		public SearchServerQuery highlightQuery;
		public boolean requirefieldmatch = true;
		public String pre;// = "<em>";
		public String post;// = "</em>";
		public String type;
	}

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

}
