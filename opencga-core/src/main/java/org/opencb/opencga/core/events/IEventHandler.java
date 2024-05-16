package org.opencb.opencga.core.events;

import java.util.function.Consumer;

public interface IEventHandler extends AutoCloseable {

    void addPreEventObserver(Consumer<OpencgaEvent> eventObserver);

    void addPostEventObserver(Consumer<OpencgaEvent> eventObserver);

    void subscribe(String eventId, OpenCgaObserver observer);

    void notify(String eventId, OpencgaEvent event);

}