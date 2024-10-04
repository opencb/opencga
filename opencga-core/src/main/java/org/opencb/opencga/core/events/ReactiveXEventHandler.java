package org.opencb.opencga.core.events;

import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.functions.Consumer;
import org.opencb.opencga.core.models.event.CatalogEvent;
import org.opencb.opencga.core.models.event.EventSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.BiConsumer;

public class ReactiveXEventHandler implements IEventHandler {

    private Consumer<CatalogEvent> preEventObserver;
    private Consumer<CatalogEvent> postEventObserver;

    private BiConsumer<CatalogEvent, OpenCgaObserver> onComplete;
    private BiConsumer<CatalogEvent, OpenCgaObserver> onError;

    private final Map<String, List<OpenCgaObserver>> eventSubscriberMap;
    private final Map<String, OpenCgaObserver> observerMap;

    private final Logger logger = LoggerFactory.getLogger(ReactiveXEventHandler.class);

    public ReactiveXEventHandler() {
        this.observerMap = new HashMap<>();
        this.eventSubscriberMap = new HashMap<>();
    }

    @Override
    public void addPreEventObserver(java.util.function.Consumer<CatalogEvent> eventObserver) {
        this.preEventObserver = eventObserver != null ? eventObserver::accept : null;
    }

    @Override
    public void addPostEventObserver(java.util.function.Consumer<CatalogEvent> eventObserver) {
        this.postEventObserver = eventObserver != null ? eventObserver::accept : null;
    }

    @Override
    public void addOnErrorConsumer(BiConsumer<CatalogEvent, OpenCgaObserver> onError) {
        this.onError = onError;
    }

    @Override
    public void addOnCompleteConsumer(BiConsumer<CatalogEvent, OpenCgaObserver> onComplete) {
        this.onComplete = onComplete;
    }

    @Override
    public void subscribe(String eventId, OpenCgaObserver observer) {
        if (!eventSubscriberMap.containsKey(eventId)) {
            eventSubscriberMap.put(eventId, new LinkedList<>());
        }
        eventSubscriberMap.get(eventId).add(observer);
        String uniqueObserverId = getUniqueObserverId(eventId, observer);
        observerMap.put(uniqueObserverId, observer);
    }

    private String getUniqueObserverId(String eventId, OpenCgaObserver observer) {
        return observer.getResource().name() + ":" + eventId;
    }

    @Override
    public void notify(CatalogEvent event) {
        List<String> possibleKeys = getPossibleKeys(event.getId());
        Map<String, OpenCgaObserver> tmpObserverMap = new HashMap<>();
        List<EventSubscriber> subscriberList = new LinkedList<>();
        for (String possibleKey : possibleKeys) {
            if (eventSubscriberMap.containsKey(possibleKey)) {
                List<OpenCgaObserver> openCgaObservers = eventSubscriberMap.get(possibleKey);
                for (OpenCgaObserver openCgaObserver : openCgaObservers) {
                    String uniqueObserverId = getUniqueObserverId(event.getId(), openCgaObserver);
                    if (tmpObserverMap.containsKey(uniqueObserverId)) {
                        logger.warn("Observer '{}' is already subscribed to event '{}'. It will be notified just once.",
                                openCgaObserver.getResource(), event.getId());
                    } else {
                        tmpObserverMap.put(uniqueObserverId, openCgaObserver);
                        subscriberList.add(new EventSubscriber(openCgaObserver.getResource().name(), false, 0));
                    }
                }
            }
        }
        // Fill subscribers in CatalogEvent
        event.setSubscribers(subscriberList);

        CompositeDisposable compositeDisposable = new CompositeDisposable();
        Single<CatalogEvent> single = Single.just(event);

        if (preEventObserver != null) {
            compositeDisposable.add(single.subscribe(preEventObserver));
        }

        Queue<Map.Entry<OpenCgaObserver, Integer>> queue = new LinkedList<>();
        for (OpenCgaObserver value : tmpObserverMap.values()) {
            queue.add(new AbstractMap.SimpleEntry<>(value, 1));
        }

        if (!tmpObserverMap.isEmpty()) {
            while (!queue.isEmpty()) {
                Map.Entry<OpenCgaObserver, Integer> poll = queue.poll();
                OpenCgaObserver observer = poll.getKey();
                Integer numAttempts = poll.getValue();

                compositeDisposable.add(single.subscribe(onSuccess -> {
                            try {
                                observer.getOnNext().accept(onSuccess.getEvent());
                            } catch (RuntimeException e) {
                                logger.error("Error notifying observer '{}'", observer.getResource(), e);
                                if (onError != null) {
                                    onError.accept(event, observer);
                                }

                                // Retry 3 times
                                if (numAttempts < MAX_NUM_ATTEMPTS) {
                                    queue.add(new AbstractMap.SimpleEntry<>(observer, numAttempts + 1));
                                }

                                return;
                            }
                            if (onComplete != null) {
                                onComplete.accept(event, observer);
                            }
                        },
                        throwable -> {
                            if (onError != null) {
                                onError.accept(event, observer);
                            }
                            observer.getOnError().accept(throwable, event.getEvent());
                        }));
            }
        }
        if (postEventObserver != null) {
            compositeDisposable.add(single.subscribe(postEventObserver));
        }
        compositeDisposable.dispose();
    }

    private List<String> getPossibleKeys(String eventId) {
        List<String> splitted = Arrays.asList(eventId.split("\\."));
        if (splitted.size() > 1) {
            List<String> keys = new LinkedList<>();
            Queue<MyKeys> keyQueue = new LinkedList<>();
            keyQueue.add(new MyKeys(splitted.get(0), splitted.subList(1, splitted.size())));
            keyQueue.add(new MyKeys("*", splitted.subList(1, splitted.size())));
            while (!keyQueue.isEmpty()) {
                MyKeys myKeys = keyQueue.remove();
                if (myKeys.getRemainingKeys().size() == 1) {
                    keys.add(myKeys.getPrefixKey() + "." + myKeys.getRemainingKeys().get(0));
                    keys.add(myKeys.getPrefixKey() + ".*");
                } else {
                    List<String> sublist = myKeys.getRemainingKeys().subList(1, myKeys.getRemainingKeys().size());
                    keyQueue.add(new MyKeys(myKeys.getPrefixKey() + "." + myKeys.getRemainingKeys().get(0), sublist));
                    keyQueue.add(new MyKeys(myKeys.getPrefixKey() + ".*", sublist));
                }
            }
            return keys;
        } else {
            return Arrays.asList(eventId, "*");
        }
    }

    private static class MyKeys {
        private final String prefixKey;
        private final List<String> remainingKeys;

        public MyKeys(String prefixKey, List<String> remainingKeys) {
            this.prefixKey = prefixKey;
            this.remainingKeys = remainingKeys;
        }

        public String getPrefixKey() {
            return prefixKey;
        }

        public List<String> getRemainingKeys() {
            return remainingKeys;
        }
    }

    @Override
    public void close() throws IOException {
    }
}
