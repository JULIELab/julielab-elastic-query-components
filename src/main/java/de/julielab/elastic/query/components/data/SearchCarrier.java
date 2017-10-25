/**
 * SearchCarrier.java
 *
 * Copyright (c) 2013, JULIE Lab.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Common Public License v1.0
 *
 * Author: faessler
 *
 * Current version: 1.0
 * Since version:   1.0
 *
 * Creation date: 06.04.2013
 **/

/**
 * 
 */
package de.julielab.elastic.query.components.data;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;

import de.julielab.elastic.query.services.ISearchServerResponse;

/**
 * @author faessler
 * 
 */
public class SearchCarrier {
	
	
	public List<SearchServerRequest> serverRequests;
	public List<ISearchServerResponse> serverResponses;
	public StopWatch sw;
	public String chainName;
	public List<String> enteredComponents;

	public SearchCarrier(String chainName) {
		this.chainName = chainName;
		enteredComponents = new ArrayList<>();
		sw = new StopWatch();
		sw.start();
		serverRequests = new ArrayList<>();
		serverResponses = new ArrayList<>();
	}

	
	public void addSearchServerRequest(SearchServerRequest serverCmd) {
		serverRequests.add(serverCmd);
	}

	public void addSearchServerResponse(ISearchServerResponse serverRsp) {
		serverResponses.add(serverRsp);
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

	public ISearchServerResponse getSingleSearchServerResponse() {
		if (serverResponses.size() > 1)
			throw new IllegalStateException("There are " + serverResponses.size()
					+ " search server responses instead of exactly one.");
		else if (serverResponses.size() > 0)
			return serverResponses.get(0);
		return null;
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
}
