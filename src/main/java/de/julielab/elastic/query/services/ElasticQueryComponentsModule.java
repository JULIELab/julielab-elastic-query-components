package de.julielab.elastic.query.services;

import org.apache.tapestry5.ioc.ServiceBinder;

import de.julielab.elastic.query.components.ElasticSearchComponent;
import de.julielab.elastic.query.components.ISearchServerComponent;

public class ElasticQueryComponentsModule {
	public static void bind(ServiceBinder binder) {
		binder.bind(ISearchClientProvider.class, ElasticSearchClientProvider.class).withSimpleId();
		binder.bind(ISearchServerComponent.class, ElasticSearchComponent.class).withSimpleId();
		binder.bind(IIndexingService.class, ElasticSearchIndexingService.class).withSimpleId();
	}
}
