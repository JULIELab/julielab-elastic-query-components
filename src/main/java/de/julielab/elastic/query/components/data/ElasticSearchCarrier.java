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

import java.util.ArrayList;
import java.util.List;

import de.julielab.elastic.query.services.ISearchServerResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;

import de.julielab.elastic.query.services.IElasticServerResponse;

/**
 * @author faessler
 *
 */
public class ElasticSearchCarrier extends SearchCarrier<IElasticServerResponse> {

    public List<SearchServerRequest> serverRequests;

    public ElasticSearchCarrier(String chainName) {
        super(chainName);
        serverRequests = new ArrayList<>();
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
        if (serverResponses.size() > 1)
            throw new IllegalStateException("There are " + serverResponses.size()
                    + " search server responses instead of exactly one.");
        else if (serverResponses.size() > 0)
            return serverResponses.get(0);
        return null;
    }


}
