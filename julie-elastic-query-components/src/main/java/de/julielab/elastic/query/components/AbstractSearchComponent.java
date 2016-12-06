package de.julielab.elastic.query.components;

import de.julielab.elastic.query.components.data.SearchCarrier;

/**
 * Superclass for all search components to enable some centralized functions
 * such as the creation of a trace in the SearchCarrier about which components
 * have been visited in a chain.
 * 
 * @author faessler
 * 
 */
public abstract class AbstractSearchComponent implements ISearchComponent {

	
	/**
	 * Method to call when actually running the component. Registers this
	 * component in the <tt>searchCarrier</tt> and then calls
	 * {@link #processSearch(SearchCarrier)}.
	 */
	@Override
	public boolean process(SearchCarrier searchCarrier) {
		searchCarrier.enteredComponents.add(getClass().getSimpleName());
		// TODO catch exceptions, output the chain state
		return processSearch(searchCarrier);
	}

	/**
	 * Overriding point for subclasses.
	 * 
	 * @param searchCarrier
	 * @return
	 */
	protected abstract boolean processSearch(SearchCarrier searchCarrier);

}
