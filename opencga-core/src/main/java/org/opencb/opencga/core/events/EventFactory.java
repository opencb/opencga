package org.opencb.opencga.core.events;

public class EventFactory {

    public static IEventHandler buildEventHandler() {
        return new ReactiveXEventHandler();
    }

    public static IEventHandler buildEventHandler(String clazz) {
        IEventHandler eventHandler;
        if (clazz != null && !clazz.isEmpty()) {
            try {
                Class<IEventHandler> aClass = (Class<IEventHandler>) Class.forName(clazz);
                eventHandler = aClass.newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            eventHandler = new ReactiveXEventHandler();
        }
        return eventHandler;
    }

}
