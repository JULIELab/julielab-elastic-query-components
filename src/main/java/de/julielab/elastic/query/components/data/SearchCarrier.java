package de.julielab.elastic.query.components.data;

import de.julielab.elastic.query.services.IElasticServerResponse;
import de.julielab.elastic.query.services.ISearchServerResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;

import java.util.ArrayList;
import java.util.List;

public class SearchCarrier<R extends ISearchServerResponse> {
    public List<R> serverResponses;
    public StopWatch sw;
    public String chainName;
    public List<String> enteredComponents;
    public List<String> errorMessages;

    public SearchCarrier(String chainName) {
        this.chainName = chainName;
        enteredComponents = new ArrayList<>();
        sw = new StopWatch();
        sw.start();
        serverResponses = new ArrayList<>();
    }

    public void addSearchServerResponse(R serverRsp) {
        serverResponses.add(serverRsp);
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
        serverResponses.forEach(r -> {
            if (r.getQueryErrorMessage() != null)
                errorMessages.add(r.getQueryErrorMessage());
        });
        return !errorMessages.isEmpty() ? errorMessages.get(0) : "<no error message>";
    }
}
