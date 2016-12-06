package de.julielab.elastic.query.components.data.query;

import java.util.ArrayList;
import java.util.List;

/**
 * Like {@link TermQuery} but for multiple terms.
 * 
 * @author faessler
 * 
 */
public class TermsQuery extends SearchServerQuery {
	/**
	 * The terms to look for in {@link #field}. Can be strings, a numbers or possibly other types. For more information,
	 * refer to the documentation of the used search server.
	 */
	public List<Object> terms;
	/**
	 * The field in which to look for {@link #term}.
	 */
	public String field;
	
	public void addTerm(Object term) {
		if (null == terms)
			terms = new ArrayList<>();
		terms.add(term);
	}
}
