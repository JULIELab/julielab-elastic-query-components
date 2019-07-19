package de.julielab.elastic.query.services;

import static de.julielab.elastic.query.ElasticQuerySymbolConstants.ES_CLUSTER_NAME;
import static de.julielab.elastic.query.ElasticQuerySymbolConstants.ES_HOST;
import static de.julielab.elastic.query.ElasticQuerySymbolConstants.ES_PORT;

import org.apache.tapestry5.ioc.LoggerSource;
import org.apache.tapestry5.ioc.annotations.PostInjection;
import org.apache.tapestry5.ioc.annotations.Symbol;
import org.apache.tapestry5.ioc.services.RegistryShutdownHub;
import org.slf4j.Logger;

import de.julielab.elastic.query.services.ISearchClient;
import de.julielab.elastic.query.services.ISearchClientProvider;
import de.julielab.elastic.query.services.ElasticSearchClient;

import java.util.Arrays;

public class ElasticSearchClientProvider implements ISearchClientProvider {

	private final Logger log;
	private ElasticSearchClient elasticSearchServer;

	public ElasticSearchClientProvider(Logger log, LoggerSource loggerSource,
			@Symbol(ES_CLUSTER_NAME) String clusterName, @Symbol(ES_HOST) String host, @Symbol(ES_PORT) String port) {
		this.log = log;
		elasticSearchServer = new ElasticSearchClient(
				loggerSource.getLogger(ElasticSearchClient.class), clusterName, host.split(","), Arrays.stream(port.split(",")).mapToInt(Integer::valueOf).toArray());
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
public static void main(String args[]) {
	System.out.println(Arrays.toString("eins".split(",")));
}
}
