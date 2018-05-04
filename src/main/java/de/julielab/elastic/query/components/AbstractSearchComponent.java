package de.julielab.elastic.query.components;

import de.julielab.elastic.query.components.data.ElasticSearchCarrier;
import de.julielab.elastic.query.components.data.SearchCarrier;
import de.julielab.elastic.query.services.ISearchServerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Superclass for all search components to enable some centralized functions
 * such as the creation of a trace in the ElasticSearchCarrier about which components
 * have been visited in a chain.
 * 
 * @author faessler
 * 
 */
public abstract class AbstractSearchComponent<C extends SearchCarrier> implements ISearchComponent<C> {

	private BiFunction<Object, String, String> notNull = (o, m) -> o == null ? m + " is null." : null;
	private BiFunction<Collection<?>, String, String> notEmpty = (o, m) -> o.isEmpty() ? m + " is empty." : null;
	protected Logger log;
	
	private static final Logger componentChainLogger = LoggerFactory.getLogger("de.julielab.elastic.query.ComponentChain");

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
	protected void checkNotEmpty(Object... objects) {
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
				String returnMessage = notEmpty.apply((Collection<?>) objects[i - 1], (String) object);
				if (returnMessage != null)
					errorMessages.add(returnMessage);
			}
		}
	}

	/**
	 * Checks if there are error messages created by
	 * {@link #checkNotNull(Object...)} and
	 * {@link #checkNotEmpty(Object...)} which need to be called before this
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
	protected <T extends SearchCarrier<?>> T castCarrier(SearchCarrier<?> elasticSearchCarrier) {
		return (T) elasticSearchCarrier;
	}

	/**
	 * Method to call when actually running the component. Registers this
	 * component in the <tt>elasticSearchCarrier</tt> and then calls
	 * {@link #processSearch(SearchCarrier)}.
	 */
	@Override
	public boolean process(SearchCarrier elasticSearchCarrier) {
		errorMessages.clear();
		elasticSearchCarrier.addEnteredComponent(getClass().getSimpleName());
		try {
			componentChainLogger.debug("Now calling search component \"{}\"", getClass().getSimpleName());
			boolean terminateChain = processSearch(elasticSearchCarrier);
			componentChainLogger.debug("Search component \"{}\" returned {}", getClass().getSimpleName(), terminateChain);
			return terminateChain;
		} catch (Exception e) {
			log.error(
					"An exception has occurred in component {}. The visited sequence of components until this point was: {}",
					getClass().getSimpleName(), elasticSearchCarrier.getEnteredComponents());
			throw e;
		}
	}

	/**
	 * Overriding point for subclasses.
	 * 
	 * @param elasticSearchCarrier
	 * @return
	 */
	protected abstract boolean processSearch(SearchCarrier elasticSearchCarrier);

}
