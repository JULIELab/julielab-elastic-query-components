package de.julielab.elastic.query.components.data;

import de.julielab.elastic.query.util.TermCountCursor;

/**
 * This interface defines facet fields and their values as they are needed
 * specifically for Semedic.
 * 
 * @author faessler
 * 
 */
public interface IFacetField {
	/**
	 * This enumeration serves for the keys for possible facet types. For
	 * example, simple faceting just returns the number of occurrences - the
	 * count - for a term in the document search result set. More advanced
	 * faceting may include the document frequency - df - of each term as well
	 * for for further computations.
	 * 
	 * @author faessler
	 * 
	 */
	public static enum FacetType {
		/**
		 * Simple facet count, i.e. the number of documents returned by a query
		 * containing the term.
		 */
		count,
		/**
		 * Document frequency of a term on the whole index, i.e. the number of all,
		 * unrestricted index documents containing the term.
		 */
		documentFrequency
	}

	public TermCountCursor getFacetValues();

	public String getName();
}
