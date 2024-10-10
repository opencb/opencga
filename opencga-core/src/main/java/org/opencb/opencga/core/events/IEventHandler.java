package org.opencb.opencga.core.events;

import org.opencb.opencga.core.models.event.CatalogEvent;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface IEventHandler extends Closeable {

    /**
     * Method to subscribe to an event.
     *
     * @param eventId Event id to which the observer wants to subscribe.
     * @param observer Observer containing the lambda to be executed when the event is triggered.
     */
    void subscribe(String eventId, OpenCgaObserver observer);

    /**
     * Method to notify of a new event.
     *
     * @param event Event to be notified.
     */
    default void notify(CatalogEvent event) {
        notify(event, null);
    }

    /**
     * Method to notify of a new event with an error.
     *
     * @param event Event to be notified.
     * @param e     Exception to be notified.
     */
    void notify(CatalogEvent event, @Nullable Exception e);

    /**
     * Method to retry notifying the subscribers of an event.
     *
     * @param event Event to be notified.
     */
    void retry(CatalogEvent event);

    /**
     * Lambda to be executed right before a new event is triggered.
     *
     * @param eventObserver Lambda to be executed.
     */
    void addPreEventObserver(Consumer<CatalogEvent> eventObserver);

    /**
     * Lambda to be executed right after a successful subscriber lambda execution.
     *
     * @param onComplete Lambda to be executed.
     */
    void addOnCompleteConsumer(BiConsumer<CatalogEvent, OpenCgaObserver> onComplete);

    /**
     * Lambda to be executed right after an error occurs during the subscriber lambda execution.
     *
     * @param onError Lambda to be executed.
     */
    void addOnErrorConsumer(BiConsumer<CatalogEvent, OpenCgaObserver> onError);

    /**
     * Lambda to be executed after all the subscribers have been notified and executed.
     *
     * @param eventObserver Lambda to be executed.
     */
    void addPostEventObserver(Consumer<CatalogEvent> eventObserver);

}