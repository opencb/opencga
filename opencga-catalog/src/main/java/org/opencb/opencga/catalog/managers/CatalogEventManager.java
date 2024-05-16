package org.opencb.opencga.catalog.managers;

import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.events.EventManager;
import org.opencb.opencga.core.events.OpencgaProcessedEvent;

public class CatalogEventManager extends EventManager {

    private final DBAdaptorFactory dbAdaptorFactory;

    public CatalogEventManager(DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        super((opencgaEvent) -> {
            catalogDBAdaptorFactory.getEventDBAdaptor(opencgaEvent.getOrganizationId()).insert(new OpencgaProcessedEvent());
            System.out.println("Pre event consumer: " + opencgaEvent);
        }, (opencgaEvent) -> {

        }, configuration);

        this.dbAdaptorFactory = catalogDBAdaptorFactory;
    }



}
