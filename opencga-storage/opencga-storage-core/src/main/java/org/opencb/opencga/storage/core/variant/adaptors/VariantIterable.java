/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.core.variant.adaptors;

import com.google.common.base.Throwables;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.MultiVariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;

import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
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

    default VariantDBIterator iterator(Iterator<?> variants, Query query, QueryOptions options) {
        return iterator(variants, query, options, 100);
    }

    default VariantDBIterator iterator(Iterator<?> variants, Query query, QueryOptions options, int batchSize) {
        return new MultiVariantDBIterator(variants, batchSize, query, options, this::iterator);
    }

    VariantDBIterator iterator(Query query, QueryOptions options);

    default Stream<Variant> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    default Stream<Variant> stream(Query query, QueryOptions options) {
        return StreamSupport.stream(spliterator(query, options), false);
    }

    default Spliterator<Variant> spliterator(Query query, QueryOptions options) {
        return Spliterators.spliteratorUnknownSize(iterator(query, options), 0);
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
