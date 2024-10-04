package org.opencb.opencga.catalog.managers;

import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.events.EventFactory;
import org.opencb.opencga.core.events.IEventHandler;
import org.opencb.opencga.core.events.OpenCgaObserver;
import org.opencb.opencga.core.models.event.CatalogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;

public final class EventManager implements Closeable {

    private static volatile EventManager eventManager;

    private final IEventHandler eventHandler;
    private final DBAdaptorFactory dbAdaptorFactory;
    private final Logger logger = LoggerFactory.getLogger(EventManager.class);

    private EventManager(DBAdaptorFactory dbAdaptorFactory) {
        this(dbAdaptorFactory, null, null, null);
    }

    private EventManager(DBAdaptorFactory dbAdaptorFactory, java.util.function.Consumer<CatalogEvent> preEventObserver,
                         java.util.function.Consumer<CatalogEvent> postEventObserver, Configuration configuration) {
        // TODO: Read configuration to get the event handler class
        this.eventHandler = EventFactory.buildEventHandler();
        this.dbAdaptorFactory = dbAdaptorFactory;

        initEventHandler(preEventObserver, postEventObserver);
    }

    private void initEventHandler(java.util.function.Consumer<CatalogEvent> preEventObserver,
                                  java.util.function.Consumer<CatalogEvent> postEventObserver) {
        this.eventHandler.addPreEventObserver(event -> {
            // Register event in database
            try {
                dbAdaptorFactory.getEventDBAdaptor(event.getEvent().getOrganizationId()).insert(event);
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
                logger.error("Internal error: Could not write in database.", e);
            }
            if (preEventObserver != null) {
                preEventObserver.accept(event);
            }
        });

        this.eventHandler.addPostEventObserver(event -> {
            // Register end of event in database
            try {
                dbAdaptorFactory.getEventDBAdaptor(event.getEvent().getOrganizationId()).finishEvent(event);
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
                logger.error("Internal error: Could not write in database.", e);
            }
            if (postEventObserver != null) {
                postEventObserver.accept(event);
            }
        });

        this.eventHandler.addOnCompleteConsumer((event, observer) -> {
            logger.info("Event '{}' was properly processed by observer '{}'. Marking as successful.", event, observer);
            try {
                dbAdaptorFactory.getEventDBAdaptor(event.getEvent().getOrganizationId())
                        .updateSubscriber(event, observer.getResource(), true);
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
                logger.error("Internal error: Could not write in database.", e);
            }
        });
        this.eventHandler.addOnErrorConsumer((event, observer) -> {
            logger.info("Event '{}' was not properly processed by observer '{}'. Marking as unsuccessful.", event, observer);
            try {
                dbAdaptorFactory.getEventDBAdaptor(event.getEvent().getOrganizationId())
                        .updateSubscriber(event, observer.getResource(), false);
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
                logger.error("Internal error: Could not write in database.", e);
            }
        });
    }

    public static void configure(DBAdaptorFactory dbAdaptorFactory) {
        if (eventManager == null) {
            synchronized (EventManager.class) {
                if (eventManager == null) {
                    eventManager = new EventManager(dbAdaptorFactory);
                }
            }
        }
    }

    public static void configure(DBAdaptorFactory dbAdaptorFactory, java.util.function.Consumer<CatalogEvent> preObserver,
                                 java.util.function.Consumer<CatalogEvent> postObserver, Configuration configuration) {
        if (eventManager == null) {
            synchronized (EventManager.class) {
                if (eventManager == null) {
                    eventManager = new EventManager(dbAdaptorFactory, preObserver, postObserver, configuration);
                }
            }
        }
    }

    public static EventManager getInstance() {
        if (eventManager == null) {
            throw new IllegalStateException("EventManager is not yet initialised.");
        }
        return eventManager;
    }

    public void subscribe(String eventId, OpenCgaObserver observer) {
        eventHandler.subscribe(eventId, observer);
    }

    public void notify(CatalogEvent event) throws CatalogParameterException {
        validateNewEvent(event);
        logger.info("Notified '{}' event", event.getEvent().getEventId());
        eventHandler.notify(event);
    }

    private void validateNewEvent(CatalogEvent event) throws CatalogParameterException {
        ParamUtils.checkObj(event, "CatalogEvent");
        ParamUtils.checkObj(event.getId(), "CatalogEvent.id");
        ParamUtils.checkObj(event.getEvent(), "CatalogEvent.event");
        ParamUtils.checkParameter(event.getEvent().getEventId(), "CatalogEvent.event.id");
        ParamUtils.checkParameter(event.getEvent().getOrganizationId(), "CatalogEvent.event.organizationId");
        ParamUtils.checkParameter(event.getEvent().getToken(), "CatalogEvent.event.token");
        ParamUtils.checkParameter(event.getEvent().getId(), "CatalogEvent.event.id");
        ParamUtils.checkParameter(event.getEvent().getUserId(), "CatalogEvent.event.userId");
        ParamUtils.checkParameter(event.getEvent().getStudy(), "CatalogEvent.event.study");
        ParamUtils.checkObj(event.getEvent().getResult(), "CatalogEvent.event.result");

        event.setCreationDate(TimeUtils.getTime());
        event.setModificationDate(TimeUtils.getTime());
        event.setSubscribers(Collections.emptyList());
        event.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.EVENT));
        event.setSuccessful(false);
    }

    @Override
    public void close() throws IOException {
        if (this.eventHandler != null) {
            this.eventHandler.close();
        }
    }
}
