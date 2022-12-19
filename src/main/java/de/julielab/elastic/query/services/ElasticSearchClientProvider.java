package de.julielab.elastic.query.services;

import org.apache.tapestry5.ioc.LoggerSource;
import org.apache.tapestry5.ioc.annotations.PostInjection;
import org.apache.tapestry5.ioc.annotations.Symbol;
import org.apache.tapestry5.ioc.services.RegistryShutdownHub;
import org.slf4j.Logger;

import java.util.Arrays;

import static de.julielab.elastic.query.ElasticQuerySymbolConstants.*;

public class ElasticSearchClientProvider implements ISearchClientProvider {

	private final Logger log;
	private ElasticSearchClient elasticSearchServer;

	public ElasticSearchClientProvider(Logger log, LoggerSource loggerSource,
			@Symbol(ES_CLUSTER_NAME) String clusterName, @Symbol(ES_HOST) String host, @Symbol(ES_PORT) String port, @Symbol(ES_SOCKET_TIMEOUT) int socketTimeout) {
		this.log = log;
		log.info("Got symbol values for ElasticSearch connection; {}:{}, {}:{}, {}:{}", ES_HOST, host, ES_PORT, port, ES_CLUSTER_NAME, clusterName);
		elasticSearchServer = new ElasticSearchClient(
				loggerSource.getLogger(ElasticSearchClient.class), clusterName, host.split(","), Arrays.stream(port.split(",")).mapToInt(Integer::valueOf).toArray(), socketTimeout);
	}

	@Override
	public ISearchClient getSearchClient() {
		return elasticSearchServer;
	}

	@PostInjection
	public void startupService(RegistryShutdownHub shutdownHub) {
		shutdownHub.addRegistryShutdownListener(new Runnable() {
			public void run() {
				log.info("Shutting down elastic search clients.");
				elasticSearchServer.shutdown();
			}
		});
	}
}
