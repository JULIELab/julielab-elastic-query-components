/**
 * SearchComponent.java
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
 * Creation date: 06.04.2013
 **/

/**
 * 
 */
package de.julielab.elastic.query.components;

/**
 * Interface for the search component that wraps the concrete search server
 * calls, i.e. API-dependent calls to Solr or ElastiSearch.
 * 
 * @author faessler
 * 
 */
public interface ISearchServerComponent extends ISearchComponent {
	static final String REVIEW_TERM = "Review";
}
