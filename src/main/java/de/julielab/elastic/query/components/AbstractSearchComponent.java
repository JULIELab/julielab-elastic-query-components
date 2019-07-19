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
 */
public abstract class AbstractSearchComponent<C extends SearchCarrier<? extends ISearchServerResponse>> implements ISearchComponent<C> {

    private static final Logger componentChainLogger = LoggerFactory.getLogger("de.julielab.elastic.query.ComponentChain");
    protected Logger log;

    public AbstractSearchComponent(Logger log) {
        this.log = log;
    }

    /**
     * Method to call when actually running the component. Registers this
     * component in the <tt>elasticSearchCarrier</tt> and then calls
     * {@link #processSearch(SearchCarrier)}.
     */
    @Override
    public boolean process(C elasticSearchCarrier) {
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
    protected abstract boolean processSearch(C elasticSearchCarrier);

}
