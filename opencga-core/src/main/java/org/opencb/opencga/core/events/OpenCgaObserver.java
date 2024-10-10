package org.opencb.opencga.core.events;

import org.opencb.opencga.core.models.common.Enums;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class OpenCgaObserver {

    private Enums.Resource resource;
    private Consumer<OpencgaEvent> onNext;
    private BiConsumer<Throwable, OpencgaEvent> onError;

    // Flag indicating whether the observer execution requires to be controlled or not.
    private boolean ephemeral;

    private static final Logger logger = LoggerFactory.getLogger(OpenCgaObserver.class);

    public OpenCgaObserver() {
    }

    public OpenCgaObserver(Enums.Resource resource, Consumer<OpencgaEvent> onSuccess) {
        this(resource, onSuccess, (throwable, opencgaEvent) -> logger.warn("Not implemented error handling for observer '{}'.", resource),
                false);
    }

    public OpenCgaObserver(Enums.Resource resource, Consumer<OpencgaEvent> onSuccess, boolean ephemeral) {
        this(resource, onSuccess, (throwable, opencgaEvent) -> logger.warn("Not implemented error handling for observer '{}'.", resource),
                ephemeral);
    }

    public OpenCgaObserver(Enums.Resource resource, Consumer<OpencgaEvent> onNext, BiConsumer<Throwable, OpencgaEvent> onError) {
        this(resource, onNext, onError, false);
    }

    public OpenCgaObserver(Enums.Resource resource, Consumer<OpencgaEvent> onNext, BiConsumer<Throwable, OpencgaEvent> onError,
                           boolean ephemeral) {
        this.resource = resource;
        this.onNext = onNext;
        this.onError = onError;
        this.ephemeral = ephemeral;
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

    public boolean isEphemeral() {
        return ephemeral;
    }

    public OpenCgaObserver setEphemeral(boolean ephemeral) {
        this.ephemeral = ephemeral;
        return this;
    }
}
