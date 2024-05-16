package org.opencb.opencga.core.events;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.functions.Consumer;

import java.util.*;

public class ReactiveXEventHandler implements IEventHandler {

    private Consumer<OpencgaEvent> preEventObserver;
    private Consumer<? super OpencgaEvent> postEventObserver;

    private final Map<String, List<OpenCgaObserver>> eventSubscriberMap;

    public ReactiveXEventHandler() {
        this.eventSubscriberMap = new HashMap<>();
    }

    @Override
    public void addPreEventObserver(java.util.function.Consumer<OpencgaEvent> eventObserver) {
        this.preEventObserver = eventObserver != null ? eventObserver::accept : null;
    }

    @Override
    public void addPostEventObserver(java.util.function.Consumer<OpencgaEvent> eventObserver) {
        this.postEventObserver = eventObserver != null ? eventObserver::accept : null;
    }

    @Override
    public void subscribe(String eventId, OpenCgaObserver observer) {
        if (!eventSubscriberMap.containsKey(eventId)) {
            eventSubscriberMap.put(eventId, new LinkedList<>());
        }
        eventSubscriberMap.get(eventId).add(observer);
    }

    @Override
    public void notify(String eventId, OpencgaEvent event) {
        List<String> possibleKeys = getPossibleKeys(eventId);
        for (String possibleKey : possibleKeys) {
            if (eventSubscriberMap.containsKey(possibleKey)) {
                CompositeDisposable compositeDisposable = new CompositeDisposable();

                Observable<OpencgaEvent> observable = Observable.just(event);
//            observable = observable.subscribeOn(Schedulers.io());
//            observable = observable.observeOn(Schedulers.io());
                if (preEventObserver != null) {
                    compositeDisposable.add(observable.subscribe(preEventObserver));
                }
                for (OpenCgaObserver openCgaObserver : eventSubscriberMap.get(possibleKey)) {
                    observable.subscribe(openCgaObserver.getOnNext()::accept,
                            throwable ->  openCgaObserver.getOnError().accept(throwable, event),
                            () -> openCgaObserver.getOnComplete().accept(event),
                            compositeDisposable);
                }
                if (postEventObserver != null) {
                    compositeDisposable.add(observable.subscribe(postEventObserver));
                }
                compositeDisposable.dispose();
            }
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
    public void close() throws Exception {
    }
}
