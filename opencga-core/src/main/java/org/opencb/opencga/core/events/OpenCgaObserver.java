package org.opencb.opencga.core.events;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class OpenCgaObserver {

    private Consumer<OpencgaEvent> onNext;
    private BiConsumer<Throwable, OpencgaEvent> onError;
    private Consumer<OpencgaEvent> onComplete;

    public OpenCgaObserver() {
    }

    public OpenCgaObserver(Consumer<OpencgaEvent> onNext, BiConsumer<Throwable, OpencgaEvent> onError, Consumer<OpencgaEvent> onComplete) {
        this.onNext = onNext;
        this.onError = onError;
        this.onComplete = onComplete;
    }

    public Consumer<OpencgaEvent> getOnNext() {
        return onNext;
    }

    public OpenCgaObserver setOnNext(Consumer<OpencgaEvent> onNext) {
        this.onNext = onNext;
        return this;
    }

    public BiConsumer<Throwable, OpencgaEvent> getOnError() {
        return onError;
    }

    public OpenCgaObserver setOnError(BiConsumer<Throwable, OpencgaEvent> onError) {
        this.onError = onError;
        return this;
    }

    public Consumer<OpencgaEvent> getOnComplete() {
        return onComplete;
    }

    public OpenCgaObserver setOnComplete(Consumer<OpencgaEvent> onComplete) {
        this.onComplete = onComplete;
        return this;
    }
}
