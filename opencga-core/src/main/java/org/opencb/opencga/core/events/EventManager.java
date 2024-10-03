package org.opencb.opencga.core.events;

import org.opencb.opencga.core.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventManager implements AutoCloseable {

    private static volatile EventManager eventManager;

    private final IEventHandler eventHandler;
    private final Logger logger = LoggerFactory.getLogger(EventManager.class);

    private EventManager(java.util.function.Consumer<OpencgaEvent> preEventObserver,
                        java.util.function.Consumer<OpencgaEvent> postEventObserver, Configuration configuration) {
        // TODO: Read configuration to get the event handler class
        this.eventHandler = EventFactory.buildEventHandler();
        if (preEventObserver != null) {
            this.eventHandler.addPreEventObserver(preEventObserver);
        }
        if (postEventObserver != null) {
            this.eventHandler.addPostEventObserver(postEventObserver);
        }
    }

    public static void configure(java.util.function.Consumer<OpencgaEvent> preObserver,
                                 java.util.function.Consumer<OpencgaEvent> postObserver, Configuration configuration) {
        if (eventManager == null) {
            synchronized (EventManager.class) {
                if (eventManager == null) {
                    eventManager = new EventManager(preObserver, postObserver, configuration);
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

    public void notify(String eventId, OpencgaEvent event) {
        logger.info("Notified '{}' event", eventId);
        eventHandler.notify(eventId, event);
    }

    @Override
    public void close() throws Exception {
        if (this.eventHandler != null) {
            this.eventHandler.close();
        }
    }

}
