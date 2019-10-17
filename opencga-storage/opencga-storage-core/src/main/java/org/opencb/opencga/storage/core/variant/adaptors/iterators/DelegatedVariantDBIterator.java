package org.opencb.opencga.storage.core.variant.adaptors.iterators;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.opencga.core.results.VariantQueryResult;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Created by jacobo on 28/03/19.
 */
public class DelegatedVariantDBIterator extends VariantDBIterator {

    private final VariantDBIterator delegated;

    public DelegatedVariantDBIterator(VariantDBIterator delegated) {
        this.delegated = delegated;
    }

    @Override
    public VariantDBIterator addCloseable(AutoCloseable closeable) {
        return delegated.addCloseable(closeable);
    }

    public VariantDBIterator getDelegated() {
        return delegated;
    }

    @Override
    public long getTimeConverting() {
        return delegated.getTimeConverting();
    }

    @Override
    public long getTimeFetching() {
        return delegated.getTimeFetching();
    }

    @Override
    public int getCount() {
        return delegated.getCount();
    }

    @Override
    public boolean hasNext() {
        return delegated.hasNext();
    }

    @Override
    public Variant next() {
        return delegated.next();
    }

    @Override
    public void forEachRemaining(Consumer<? super Variant> action) {
        delegated.forEachRemaining(action);
    }

    @Override
    public DataResult<Variant> toDataResult() {
        return delegated.toDataResult();
    }

    @Override
    public VariantQueryResult<Variant> toDataResult(Map<String, List<String>> samples) {
        return delegated.toDataResult(samples);
    }

    @Override
    protected <R, E extends Exception> R convert(TimeFunction<R, E> converter) throws E {
        return delegated.convert(converter);
    }

    @Override
    protected <R, E extends Exception> R fetch(TimeFunction<R, E> fetcher) throws E {
        return delegated.fetch(fetcher);
    }

    @Override
    public void close() throws Exception {
        delegated.close();
    }

    @Override
    public int hashCode() {
        return delegated.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return delegated.equals(obj);
    }

    @Override
    public String toString() {
        return delegated.toString();
    }
}
