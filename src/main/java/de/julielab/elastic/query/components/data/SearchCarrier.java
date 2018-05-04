package de.julielab.elastic.query.components.data;

import de.julielab.elastic.query.services.ISearchServerResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;

import java.util.ArrayList;
import java.util.List;

public class SearchCarrier<R extends ISearchServerResponse> {
    protected List<R> searchResponses;
    protected StopWatch sw;
    protected String chainName;
    protected List<String> enteredComponents;
    protected List<String> errorMessages;

    public SearchCarrier(String chainName) {
        this.chainName = chainName;
        enteredComponents = new ArrayList<>();
        sw = new StopWatch();
        sw.start();
        searchResponses = new ArrayList<>();
    }

    public List<R> getSearchResponses() {
        return searchResponses;
    }

    public void setSearchResponses(List<R> searchResponses) {
        this.searchResponses = searchResponses;
    }

    public R getSearchResponse(int index) {
        return searchResponses.get(index);
    }

    public void addSearchResponse(R searchResponse) {
        if (searchResponses == null)
            searchResponses = new ArrayList<>();
        searchResponses.add(searchResponse);
    }

    public String getChainName() {
        return chainName;
    }

    public void setChainName(String chainName) {
        this.chainName = chainName;
    }

    public List<String> getEnteredComponents() {
        return enteredComponents;
    }

    public void setEnteredComponents(List<String> enteredComponents) {
        this.enteredComponents = enteredComponents;
    }

    public String getEnteredComponent(int index) {
        return enteredComponents.get(index);
    }

    public void addEnteredComponent(String enteredComponent) {
        if (enteredComponents == null)
            enteredComponents = new ArrayList<>();
        enteredComponents.add(enteredComponent);
    }

    public List<String> getErrorMessages() {
        return errorMessages;
    }

    public void setErrorMessages(List<String> errorMessages) {
        this.errorMessages = errorMessages;
    }

    public void addSearchServerResponse(R serverRsp) {
        searchResponses.add(serverRsp);
    }

    public void setElapsedTime() {
        sw.stop();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("State of chain ").append(chainName).append(":\n");
        sb.append("Entered components: ").append(StringUtils.join(enteredComponents, ", "));
        return sb.toString();
    }

    public String getFirstError() {
        if (errorMessages == null)
            errorMessages = new ArrayList<>();
        searchResponses.forEach(r -> {
            if (r.getQueryErrorMessage() != null)
                errorMessages.add(r.getQueryErrorMessage());
        });
        return !errorMessages.isEmpty() ? errorMessages.get(0) : "<no error message>";
    }

    public void clearSearchResponses() {
        searchResponses.clear();
    }
}
