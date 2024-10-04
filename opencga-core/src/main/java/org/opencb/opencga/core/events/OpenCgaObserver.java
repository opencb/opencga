package org.opencb.opencga.core.events;

import org.opencb.opencga.core.models.common.Enums;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class OpenCgaObserver {

    private Enums.Resource resource;
    private Consumer<OpencgaEvent> onNext;
    private BiConsumer<Throwable, OpencgaEvent> onError;

    public OpenCgaObserver() {
    }

    public OpenCgaObserver(Enums.Resource resource, Consumer<OpencgaEvent> onSuccess) {
        this(resource, onSuccess, (throwable, opencgaEvent) -> {});
    }

    public OpenCgaObserver(Enums.Resource resource, Consumer<OpencgaEvent> onNext, BiConsumer<Throwable, OpencgaEvent> onError) {
        this.resource = resource;
        this.onNext = onNext;
        this.onError = onError;
    }

    public Enums.Resource getResource() {
        return resource;
    }

    public OpenCgaObserver setResource(Enums.Resource resource) {
        this.resource = resource;
        return this;
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

}
