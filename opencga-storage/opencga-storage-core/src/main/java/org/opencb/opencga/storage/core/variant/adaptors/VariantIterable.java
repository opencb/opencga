package org.opencb.opencga.storage.core.variant.adaptors;

import com.google.common.base.Throwables;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created on 13/03/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface VariantIterable extends Iterable<Variant> {

    @Override
    default VariantDBIterator iterator() {
        return iterator(new Query(), new QueryOptions());
    }

    VariantDBIterator iterator(Query query, QueryOptions options);

    default Stream<Variant> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    @Override
    default void forEach(Consumer<? super Variant> action) {
        forEach(new Query(), action, new QueryOptions());
    }

    default void forEach(Query query, Consumer<? super Variant> action, QueryOptions options) {
        Objects.requireNonNull(action);
        try (VariantDBIterator variantDBIterator = iterator(query, options)) {
            while (variantDBIterator.hasNext()) {
                action.accept(variantDBIterator.next());
            }
        } catch (Exception e) {
            Throwables.propagate(e);
        }
    }
}
