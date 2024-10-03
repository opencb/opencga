package org.opencb.opencga.catalog.db.mongodb;

import org.bson.Document;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.EventDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.OpenCgaMongoConverter;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.events.OpencgaEvent;
import org.opencb.opencga.core.events.OpencgaProcessedEvent;
import org.slf4j.LoggerFactory;

public class EventMongoDBAdaptor extends MongoDBAdaptor implements EventDBAdaptor {

    private final MongoDBCollection eventCollection;
    private final OpenCgaMongoConverter<OpencgaProcessedEvent> eventConverter;

    public EventMongoDBAdaptor(MongoDBCollection eventCollection, Configuration configuration) {
        super(configuration, LoggerFactory.getLogger(EventMongoDBAdaptor.class));
        this.eventCollection = eventCollection;

        this.eventConverter = new OpenCgaMongoConverter<>(OpencgaProcessedEvent.class);
    }

    @Override
    public void insert(OpencgaProcessedEvent event) {
        Document document = eventConverter.convertToStorageType(event);
        eventCollection.insert(document, QueryOptions.empty());
    }

    @Override
    public void addSubscriber(OpencgaEvent event, String subscriber, Status status) {

    }

    @Override
    public void finishEvent(OpencgaEvent opencgaEvent) {

    }
}
