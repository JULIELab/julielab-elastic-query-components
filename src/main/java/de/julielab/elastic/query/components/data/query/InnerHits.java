package de.julielab.elastic.query.components.data.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import de.julielab.elastic.query.components.data.HighlightCommand;

public class InnerHits {
	// TODO support
	// Highlighting
	// Explain
	// Source filtering
	// Script fields
	// Fielddata fields
	// Include versions

//	public static class Highlight {
//
//		public static class HlField {
//			public int fragnum;
//			public int fragsize;
//			public String field;
//		}
//
//		public List<HlField> fields;
//
//		public void addField(HlField field) {
//			if (null == fields)
//				fields = new ArrayList<>();
//			fields.add(field);
//		}
//
//		public void addField(String field, int fragnum, int fragsize) {
//			if (null == fields)
//				fields = new ArrayList<>();
//			HlField hlField = new HlField();
//			hlField.field = field;
//			hlField.fragnum = fragnum;
//			hlField.fragsize = fragsize;
//			fields.add(hlField);
//		}
//	}

//	public Highlight highlight;
	public HighlightCommand highlight;
	public boolean fetchSource;
	public List<String> fields = Collections.emptyList();
	public boolean explain;
	public Integer size;

	public void addField(String field) {
		if (fields.isEmpty())
			fields = new ArrayList<>();
		fields.add(field);
	}
}
