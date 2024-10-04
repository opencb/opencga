package org.opencb.opencga.core.events;

import org.opencb.opencga.core.models.event.CatalogEvent;

import java.io.Closeable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface IEventHandler extends Closeable {

    int MAX_NUM_ATTEMPTS = 3;

    void addPreEventObserver(Consumer<CatalogEvent> eventObserver);

    void addPostEventObserver(Consumer<CatalogEvent> eventObserver);

    void subscribe(String eventId, OpenCgaObserver observer);

    void notify(CatalogEvent event);

    void addOnErrorConsumer(BiConsumer<CatalogEvent, OpenCgaObserver> onError);

    void addOnCompleteConsumer(BiConsumer<CatalogEvent, OpenCgaObserver> onComplete);

}