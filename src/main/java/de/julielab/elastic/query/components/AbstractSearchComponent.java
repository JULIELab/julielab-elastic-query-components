package de.julielab.elastic.query.components;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.slf4j.Logger;

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

	private BiFunction<Object, String, String> notNull = (o, m) -> o == null ? m + " is null." : null;
	private BiFunction<Collection<?>, String, String> notEmpty = (o, m) -> o.isEmpty() ? m + " is empty." : null;
	protected Logger log;

	public AbstractSearchComponent(Logger log) {
		this.log = log;
	}

	/**
	 * Error messages for state checks.
	 */
	private List<String> errorMessages = new ArrayList<>();

	/**
	 * Checks for null objects and, if found, generates an error message. For
	 * this purpose, the odd-indexed elements must be strings giving a name to
	 * the previous object.
	 * 
	 * @param objects
	 *            A list of pairs where the even-indexed elements are
	 *            {@link Supplier} that provide the object for the null check
	 *            and odd-indexed elements are their names.
	 */
	protected void checkNotNull(Object... objects) {
		if (objects.length % 2 == 1)
			throw new IllegalArgumentException(
					"An even number of arguments is required. The even elements are the objects to test for null, the odd arguments are their names.");
		for (int i = 0; i < objects.length; i++) {
			Object object = objects[i];
			if (i % 2 == 1) {
				if (!(object instanceof CharSequence))
					throw new IllegalArgumentException(
							"All odd arguments must be names describing the previous object but was of class "
									+ object.getClass().getCanonicalName() + ".");
				String returnMessage = notNull.apply(((Supplier<?>) objects[i - 1]).get(), (String) object);
				if (returnMessage != null)
					errorMessages.add(returnMessage);
			}
		}
	}

	/**
	 * Checks for empty collections and, if found, generates an error message.
	 * For this purpose, the odd-indexed elements must be strings giving a name
	 * to the previous object.
	 * 
	 * @param objects
	 *            A list of pair where the even-indexed elements are collections
	 *            for the empty check and odd-indexed elements are their names.
	 */
	protected void checkNotEmpty(Collection<?>... objects) {
		if (objects.length % 2 == 1)
			throw new IllegalArgumentException(
					"An even number of arguments is required. The even elements are the objects to test for null, the odd arguments are their names.");
		for (int i = 0; i < objects.length; i++) {
			Object object = objects[i];
			if (i % 2 == 1) {
				if (!(object instanceof CharSequence))
					throw new IllegalArgumentException(
							"All odd arguments must be names describing the previous object but was of class "
									+ object.getClass().getCanonicalName() + ".");
				String returnMessage = notEmpty.apply(objects[i - 1], (String) object);
				if (returnMessage != null)
					errorMessages.add(returnMessage);
			}
		}
	}

	/**
	 * Checks if there are error messages created by
	 * {@link #checkNotNull(Object...)} and
	 * {@link #checkNotEmpty(Collection...)} which need to be called before this
	 * method. If there is at least one error message, the message is logged on
	 * the ERROR level and an exception is thrown.
	 * 
	 * @throws IllegalArgumentException
	 *             If there was at least one error.
	 */
	protected void stopIfError() {
		if (errorMessages.isEmpty())
			return;
		errorMessages.forEach(log::error);
		throw new IllegalArgumentException("There was at least one failed precondition check for the component "
				+ getClass().getSimpleName() + ". Check the logs above.");
	}

	@SuppressWarnings("unchecked")
	protected <T extends SearchCarrier> T castCarrier(SearchCarrier searchCarrier) {
		return (T) searchCarrier;
	}

	/**
	 * Method to call when actually running the component. Registers this
	 * component in the <tt>searchCarrier</tt> and then calls
	 * {@link #processSearch(SearchCarrier)}.
	 */
	@Override
	public boolean process(SearchCarrier searchCarrier) {
		errorMessages.clear();
		searchCarrier.enteredComponents.add(getClass().getSimpleName());
		try {
			return processSearch(searchCarrier);
		} catch (Exception e) {
			log.error(
					"An exception has occurred in component {}. The visited sequence of components until this point was: {}",
					getClass().getSimpleName(), searchCarrier.enteredComponents);
			throw e;
		}
	}

	/**
	 * Overriding point for subclasses.
	 * 
	 * @param searchCarrier
	 * @return
	 */
	protected abstract boolean processSearch(SearchCarrier searchCarrier);

}
