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

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import de.julielab.elastic.query.components.data.SearchCarrier;

/**
 * @author faessler
 * 
 */
public interface ISearchComponent {
	public boolean process(SearchCarrier searchCarrier);

	// Full chains
	@Retention(RetentionPolicy.RUNTIME)
	public @interface DocumentChain {
		// Just a marker annotation
	}

	@Retention(RetentionPolicy.RUNTIME)
	public @interface DocumentPagingChain {
		// Just a marker annotation
	}

	@Retention(RetentionPolicy.RUNTIME)
	public @interface FacetCountChain {
		// Just a marker annotation
	}
	
	@Retention(RetentionPolicy.RUNTIME)
	public @interface TermDocumentFrequencyChain {
		// Just a marker annotation
	}

	@Retention(RetentionPolicy.RUNTIME)
	public @interface ArticleChain {
		// Just a marker annotation
	}

	@Retention(RetentionPolicy.RUNTIME)
	public @interface TermSelectChain {
		// Just a marker annotation
	}
	
	@Retention(RetentionPolicy.RUNTIME)
	public @interface TotalNumDocsChain {
		// Just a marker annotation
	}

	@Retention(RetentionPolicy.RUNTIME)
	public @interface FacetIndexTermsChain {
		// Just a marker annotation
	}
	
	@Retention(RetentionPolicy.RUNTIME)
	public @interface FieldTermsChain {
		// Just a marker annotation
	}

	@Retention(RetentionPolicy.RUNTIME)
	public @interface SuggestionsChain {
		// Just a marker annotation
	}

	// Sub-chains which assembly search component sequences commonly used in full chains.
	@Retention(RetentionPolicy.RUNTIME)
	public @interface FacetedDocumentSearchSubchain {
		// Just a marker annotation
	}

}
