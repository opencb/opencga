package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.EventDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.events.EventFactory;
import org.opencb.opencga.core.events.IEventHandler;
import org.opencb.opencga.core.events.OpenCgaObserver;
import org.opencb.opencga.core.events.OpencgaEvent;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.common.EntryParam;
import org.opencb.opencga.core.models.event.CatalogEvent;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.function.Supplier;

public final class EventManager extends AbstractManager implements Closeable {

    private static volatile EventManager eventManager;

    private final IEventHandler eventHandler;
    private final DBAdaptorFactory dbAdaptorFactory;
    private final Logger logger = LoggerFactory.getLogger(EventManager.class);

    private EventManager(CatalogManager catalogManager, DBAdaptorFactory dbAdaptorFactory) {
        this(catalogManager, dbAdaptorFactory, null, null);
    }

    private EventManager(CatalogManager catalogManager, DBAdaptorFactory dbAdaptorFactory,
                         java.util.function.Consumer<CatalogEvent> preEventObserver,
                         java.util.function.Consumer<CatalogEvent> postEventObserver) {
        super(null, null, catalogManager, dbAdaptorFactory, catalogManager.getConfiguration());
        this.eventHandler = EventFactory.buildEventHandler();
        this.dbAdaptorFactory = dbAdaptorFactory;

        initEventHandler(preEventObserver, postEventObserver);
    }

    public static EventManager getInstance() {
        if (eventManager == null) {
            throw new IllegalStateException("EventManager is not yet initialised.");
        }
        return eventManager;
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
            logger.info("Event '{}' was properly processed by observer '{}'. Marking as successful.", event, observer.getResource());
            try {
                dbAdaptorFactory.getEventDBAdaptor(event.getEvent().getOrganizationId())
                        .updateSubscriber(event, observer.getResource(), true);
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
                logger.error("Internal error: Could not write in database.", e);
            }
        });
        this.eventHandler.addOnErrorConsumer((event, observer) -> {
            logger.info("Event '{}' was not properly processed by observer '{}'. Marking as unsuccessful.", event, observer.getResource());
            try {
                dbAdaptorFactory.getEventDBAdaptor(event.getEvent().getOrganizationId())
                        .updateSubscriber(event, observer.getResource(), false);
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
                logger.error("Internal error: Could not write in database.", e);
            }
        });
    }

    public static void configure(CatalogManager catalogManager, DBAdaptorFactory dbAdaptorFactory) {
        if (eventManager == null) {
            synchronized (EventManager.class) {
                if (eventManager == null) {
                    eventManager = new EventManager(catalogManager, dbAdaptorFactory);
                }
            }
        }
    }

    public static void configure(CatalogManager catalogManager, DBAdaptorFactory dbAdaptorFactory,
                                 java.util.function.Consumer<CatalogEvent> preObserver,
                                 java.util.function.Consumer<CatalogEvent> postObserver) {
        if (eventManager == null) {
            synchronized (EventManager.class) {
                if (eventManager == null) {
                    eventManager = new EventManager(catalogManager, dbAdaptorFactory, preObserver, postObserver);
                }
            }
        }
    }

    public void subscribe(String eventId, OpenCgaObserver observer) {
        eventHandler.subscribe(eventId, observer);
    }

    public void notify(CatalogEvent event) throws CatalogParameterException {
        validateNewEvent(event);
        logger.info("Notified '{}' event", event.getEvent().getEventId());
        eventHandler.notify(event);
    }

    public CatalogEvent notify(String eventId, String organizationId, Supplier<Study> studySupplier, @Nullable EntryParam entryParam,
                               String userId, ObjectMap params, JwtPayload payload, ExecuteOperation<?> executeOperation)
            throws CatalogException {
        CatalogEvent catalogEvent = new CatalogEvent(eventId, new OpencgaEvent(eventId, params, organizationId, userId,
                payload.getToken()));
        validateNewEvent(catalogEvent);
        try {
            // Obtain study
            Study study = studySupplier.get();
            catalogEvent.getEvent().setStudyUuid(study.getUuid());
            catalogEvent.getEvent().setStudyFqn(study.getFqn());

            if (entryParam != null) {
                catalogEvent.getEvent().setResourceId(entryParam.getId());
                catalogEvent.getEvent().setResourceUuid(entryParam.getUuid());
            }
            logger.info("Executing '{}' event", eventId);
            OpenCGAResult<?> execute = executeOperation.execute(organizationId, study, userId, payload);
            // Do it again because it may have been filled after execution
            if (entryParam != null) {
                catalogEvent.getEvent().setResourceId(entryParam.getId());
                catalogEvent.getEvent().setResourceUuid(entryParam.getUuid());
            }

            catalogEvent.getEvent().setResult(execute);
        } catch (Exception e) {
            eventHandler.notify(catalogEvent, e);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
            throw new CatalogException(e.getMessage(), e);
        }

        logger.info("Notifying of event '{}'.", eventId);
        eventHandler.notify(catalogEvent);
        return catalogEvent;
    }

    public OpenCGAResult<CatalogEvent> search(String organizationId, Query query, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String userId = tokenPayload.getUserId(organizationId);
        catalogManager.getAuthorizationManager().checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);
        fixQueryObject(query, tokenPayload);

        return dbAdaptorFactory.getEventDBAdaptor(organizationId).get(query, QueryOptions.empty());
    }

    void fixQueryObject(Query query, JwtPayload tokenPayload) throws CatalogException {
        String studyParam = query.getString(ParamConstants.STUDY_PARAM);
        if (StringUtils.isNotEmpty(studyParam)) {
            CatalogFqn catalogFqn = CatalogFqn.extractFqnFromStudy(studyParam, tokenPayload);
            Study study = catalogManager.getStudyManager().resolveId(catalogFqn, QueryOptions.empty(), tokenPayload);
            query.put(EventDBAdaptor.QueryParams.EVENT_STUDY_FQN.key(), study.getFqn());
            query.remove(ParamConstants.STUDY_PARAM);
        }
    }

    public OpenCGAResult<CatalogEvent> archiveEvent(String organizationId, String eventId, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String userId = tokenPayload.getUserId(organizationId);
        catalogManager.getAuthorizationManager().checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);

        Query query = new Query(EventDBAdaptor.QueryParams.UUID.key(), eventId);
        OpenCGAResult<CatalogEvent> eventResult = dbAdaptorFactory.getEventDBAdaptor(organizationId).get(query, QueryOptions.empty());
        if (eventResult.getNumResults() == 0) {
            throw new CatalogException("Event '" + eventId + "' not found");
        }

        dbAdaptorFactory.getEventDBAdaptor(organizationId).archiveEvent(eventResult.first());
        return eventResult;
    }

    public OpenCGAResult<CatalogEvent> retryEvent(String organizationId, String eventId, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String userId = tokenPayload.getUserId(organizationId);
        catalogManager.getAuthorizationManager().checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);

        Query query = new Query(EventDBAdaptor.QueryParams.UUID.key(), eventId);
        OpenCGAResult<CatalogEvent> eventResult = dbAdaptorFactory.getEventDBAdaptor(organizationId).get(query, QueryOptions.empty());
        if (eventResult.getNumResults() == 0) {
            throw new CatalogException("Event '" + eventId + "' not found");
        }

        notify(eventResult.first());
        return eventResult;
    }

    private void validateNewEvent(CatalogEvent event) throws CatalogParameterException {
        ParamUtils.checkObj(event, "CatalogEvent");
        ParamUtils.checkObj(event.getId(), "CatalogEvent.id");
        ParamUtils.checkObj(event.getEvent(), "CatalogEvent.event");
        ParamUtils.checkParameter(event.getEvent().getEventId(), "CatalogEvent.event.id");
        ParamUtils.checkParameter(event.getEvent().getOrganizationId(), "CatalogEvent.event.organizationId");
        ParamUtils.checkParameter(event.getEvent().getToken(), "CatalogEvent.event.token");
        ParamUtils.checkParameter(event.getEvent().getUserId(), "CatalogEvent.event.userId");

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
