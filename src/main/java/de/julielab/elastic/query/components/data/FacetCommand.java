/**
 * FacetCommand.java
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
import java.util.Collection;
import java.util.List;

public class FacetCommand {
	public List<String> fields = new ArrayList<String>();
	public int mincount = Integer.MIN_VALUE;
	public int limit = Integer.MIN_VALUE;
	public Collection<String> terms;
	public SortOrder sort;
	public String name;
	public String filterExpression;

	public enum SortOrder {
		COUNT, TERM, REVERSE_COUNT, REVERSE_TERM, DOC_SCORE, REVERSE_DOC_SCORE
	}

	public void addFacetField(String field) {
		fields.add(field);
	}

	@Override
	public String toString() {
		return "FacetCommand [fields=" + fields + ", terms=" + terms
				+ ", name=" + name + "]";
	}
	
	
}
