package org.opencb.opencga.core.events;

import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.functions.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.core.models.event.CatalogEvent;
import org.opencb.opencga.core.models.event.EventSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class ReactiveXEventHandler implements IEventHandler {

    private Consumer<CatalogEvent>  preEventObserver;
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

//    @Override
//    public void notify(CatalogEvent event) {
//        List<OpenCgaObserver> eventObservers = getEventObservers(event);
//        List<EventSubscriber> eventSubscriberList = eventObservers.stream()
//                .filter(o -> !o.isEphemeral())
//                .map(o -> new EventSubscriber(o.getResource().name(), false, 0))
//                .collect(Collectors.toList());
//
//        // Fill subscribers in CatalogEvent
//        event.setSubscribers(eventSubscriberList);
//
//        CompositeDisposable compositeDisposable = new CompositeDisposable();
//        Single<CatalogEvent> single = Single.just(event);
//
//        // Process preEventObserver
//        if (preEventObserver != null) {
//            compositeDisposable.add(single.subscribe(preEventObserver));
//        }
//        notifyListeners(event, eventObservers, compositeDisposable, single);
//
//        compositeDisposable.dispose();
//    }

    @Override
    public void notify(CatalogEvent event, Exception e) {
        List<OpenCgaObserver> eventObservers = getEventObservers(event);
        List<EventSubscriber> eventSubscriberList = eventObservers.stream()
                .filter(o -> !o.isEphemeral())
                .map(o -> new EventSubscriber(o.getResource().name(), false, 0))
                .collect(Collectors.toList());

        // Fill subscribers in CatalogEvent
        event.setSubscribers(eventSubscriberList);

        CompositeDisposable compositeDisposable = new CompositeDisposable();
        Single<CatalogEvent> single = e != null
                ? Single.error(e)
                : Single.just(event);

        // Process preEventObserver
        if (preEventObserver != null) {
            compositeDisposable.add(single.subscribe(preEventObserver, throwable -> {}));
        }
        notifyListeners(event, eventObservers, compositeDisposable, single);

        compositeDisposable.dispose();
    }

    @Override
    public void retry(CatalogEvent event) {
        List<OpenCgaObserver> eventObservers = getEventObservers(event);

        Set<String> uniqueObserverIds = new HashSet<>();
        for (EventSubscriber subscriber : event.getSubscribers()) {
            if (!subscriber.isSuccessful()) {
                uniqueObserverIds.add(subscriber.getId());
            }
        }

        if (uniqueObserverIds.isEmpty()) {
            logger.warn("No subscribers to retry for event '{}'", event.getId());
            return;
        }

        List<OpenCgaObserver> filteredObservers = eventObservers.stream()
                .filter(o -> uniqueObserverIds.contains(o.getResource().name()))
                .collect(Collectors.toList());
        if (filteredObservers.isEmpty()) {
            logger.warn("No subscribers to retry for event '{}'. Could not find subscribers '{}'.", event.getId(),
                    StringUtils.join(uniqueObserverIds, ", "));
            return;
        }

        CompositeDisposable compositeDisposable = new CompositeDisposable();
        Single<CatalogEvent> single = Single.just(event);

        notifyListeners(event, filteredObservers, compositeDisposable, single);
        compositeDisposable.dispose();
    }

    private List<OpenCgaObserver> getEventObservers(CatalogEvent event) {
        Set<String> uniqueObserverIds = new HashSet<>();
        List<String> possibleKeys = getPossibleKeys(event.getId());
        List<OpenCgaObserver> observerList = new LinkedList<>();
        for (String possibleKey : possibleKeys) {
            if (eventSubscriberMap.containsKey(possibleKey)) {
                List<OpenCgaObserver> openCgaObservers = eventSubscriberMap.get(possibleKey);
                for (OpenCgaObserver openCgaObserver : openCgaObservers) {
                    String uniqueObserverId = getUniqueObserverId(event.getId(), openCgaObserver);
                    if (uniqueObserverIds.contains(uniqueObserverId)) {
                        logger.warn("Observer '{}' is already subscribed to event '{}'. It will be notified just once.",
                                openCgaObserver.getResource(), event.getId());
                    } else {
                        uniqueObserverIds.add(uniqueObserverId);
                        observerList.add(openCgaObserver);
                    }
                }
            }
        }
        return observerList;
    }

    private void notifyListeners(CatalogEvent event, List<OpenCgaObserver> observerList, CompositeDisposable compositeDisposable,
                                 Single<CatalogEvent> single) {
        // Process subscribers
        for (OpenCgaObserver observer : observerList) {
            compositeDisposable.add(single.subscribe(onSuccess -> {
                        try {
                            logger.info("Notifying observer '{}' of event '{}'", observer.getResource(), event.getId());
                            observer.getOnNext().accept(onSuccess.getEvent());
                            logger.info("Observer '{}' successfully reacted to event '{}'", observer.getResource(), event.getId());
                        } catch (RuntimeException e) {
                            logger.error("Observer '{}' did not react successfully to event '{}'", observer.getResource(), event.getId());
                            if (!observer.isEphemeral() && onError != null) {
                                onError.accept(event, observer);
                            }
                            return;
                        }
                        if (!observer.isEphemeral() && onComplete != null) {
                            onComplete.accept(event, observer);
                        }
                    },
                    throwable -> {
                        if (!observer.isEphemeral() && onError != null) {
                            onError.accept(event, observer);
                        }
                        logger.info("Notifying observer '{}' of error in event '{}'", observer.getResource(), event.getId());
                        observer.getOnError().accept(throwable, event.getEvent());
                        logger.info("Observer '{}' successfully reacted to error in event '{}'", observer.getResource(), event.getId());
                    }));
        }

        // Process postEventObserver
        if (postEventObserver != null) {
            compositeDisposable.add(single.subscribe(postEventObserver, throwable -> {}));
        }
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
