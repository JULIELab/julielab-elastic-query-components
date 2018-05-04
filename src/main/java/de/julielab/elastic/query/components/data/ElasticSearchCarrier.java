/**
 * ElasticSearchCarrier.java
 * <p>
 * Copyright (c) 2013, JULIE Lab.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Common Public License v1.0
 * <p>
 * Author: faessler
 * <p>
 * Current version: 1.0
 * Since version:   1.0
 * <p>
 * Creation date: 06.04.2013
 */

/**
 *
 */
package de.julielab.elastic.query.components.data;

import de.julielab.elastic.query.services.IElasticServerResponse;

import java.util.ArrayList;
import java.util.List;

/**
 * @author faessler
 */
public class ElasticSearchCarrier<R extends IElasticServerResponse> extends SearchCarrier<R> {

    protected List<SearchServerRequest> serverRequests;

    public ElasticSearchCarrier(String chainName) {
        super(chainName);
        serverRequests = new ArrayList<>();
    }

    public List<SearchServerRequest> getServerRequests() {
        return serverRequests;
    }

    public void setServerRequests(List<SearchServerRequest> serverRequests) {
        this.serverRequests = serverRequests;
    }

    public void addServerRequest(SearchServerRequest serverRequest) {
        if (serverRequests == null)
            serverRequests = new ArrayList<>();
        serverRequests.add(serverRequest);
    }

    public void addSearchServerRequest(SearchServerRequest serverCmd) {
        serverRequests.add(serverCmd);
    }

    public SearchServerRequest getSingleSearchServerRequestOrCreate() {
        if (serverRequests.size() == 0)
            serverRequests.add(new SearchServerRequest());
        else if (serverRequests.size() > 1)
            throw new IllegalStateException("There are " + serverRequests.size()
                    + " search server commands instead of exactly one.");
        return serverRequests.get(0);
    }

    public SearchServerRequest getSingleSearchServerRequest() {
        if (serverRequests.size() > 1)
            throw new IllegalStateException("There are " + serverRequests.size()
                    + " search server commands instead of exactly one.");
        else if (serverRequests.size() > 0)
            return serverRequests.get(0);
        return null;
    }

    public IElasticServerResponse getSingleSearchServerResponse() {
        if (searchResponses.size() > 1)
            throw new IllegalStateException("There are " + searchResponses.size()
                    + " search server responses instead of exactly one.");
        else if (!searchResponses.isEmpty())
            return searchResponses.get(0);
        return null;
    }


}
