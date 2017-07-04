package org.opencb.opencga.storage.core.variant.adaptors;

import com.google.common.base.Throwables;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Iterates multiple variant iterators. Every time that voids one iterator, creates a new one using the iteratorFactory.
 *
 * Created on 04/07/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MultiVariantDBIterator extends VariantDBIterator {

    private final Iterator<Query> queryIterator;
    private final QueryOptions options;
    private final BiFunction<Query, QueryOptions, VariantDBIterator> iteratorFactory;
    private VariantDBIterator variantDBIterator;

    /**
     * Creates a multi iterator given a iterator of variants. It will apply the query (if any) to all the variants in the iterator.
     * The iterator will group the variants in batches to split the query.
     *
     * @param variantsIterator  Iterator with all the variants to filter.
     * @param batchSize         Number of variants to use in each query
     * @param query             Base query.
     * @param options           Query options to be used with the iterator factory
     * @param iteratorFactory   Iterator factory. See {@link VariantDBAdaptor#iterator()}
     */
    public MultiVariantDBIterator(Iterator<?> variantsIterator, int batchSize,
                                  Query query, QueryOptions options,
                                  BiFunction<Query, QueryOptions, VariantDBIterator> iteratorFactory) {
        Objects.requireNonNull(variantsIterator);
        this.queryIterator = new Iterator<Query>() {
            @Override
            public boolean hasNext() {
                return variantsIterator.hasNext();
            }

            @Override
            public Query next() {
                Query newQuery;
                if (query == null) {
                    newQuery = new Query();
                } else {
                    newQuery = new Query(query);
                }
                List<Object> variants = new ArrayList<>(batchSize);
                do {
                    // Always execute "next" over variantsIterator, to fail if empty
                    variants.add(variantsIterator.next());
                } while (variantsIterator.hasNext() && variants.size() < batchSize);
                newQuery.append(VariantQueryParam.ID.key(), variants);
                return newQuery;
            }
        };
        this.options = options == null ? new QueryOptions() : options;
        this.iteratorFactory = Objects.requireNonNull(iteratorFactory);
        variantDBIterator = emptyIterator();
    }

    /**
     * @param queryIterator   Query iterator. Provides queries to execute
     * @param options         Query options to be used with the iterator factory
     * @param iteratorFactory Iterator factory. See {@link VariantDBAdaptor#iterator()}
     */
    public MultiVariantDBIterator(Iterator<Query> queryIterator, QueryOptions options,
                                  BiFunction<Query, QueryOptions, VariantDBIterator> iteratorFactory) {
        this.queryIterator = Objects.requireNonNull(queryIterator);
        this.options = options == null ? new QueryOptions() : options;
        this.iteratorFactory = Objects.requireNonNull(iteratorFactory);
        variantDBIterator = emptyIterator();
    }

    @Override
    public boolean hasNext() {
        if (!variantDBIterator.hasNext()) {
            nextVariantIterator();
            return variantDBIterator.hasNext();
        } else {
            return true;
        }
    }

    private void nextVariantIterator() {
        if (queryIterator.hasNext()) {
            // Accumulate statistics from previous iterator.
            timeFetching += variantDBIterator.timeFetching;
            timeConverting += variantDBIterator.timeConverting;
            try {
                variantDBIterator.close();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }

            Query query = queryIterator.next();
            variantDBIterator = iteratorFactory.apply(query, options);
        } else {
            variantDBIterator = emptyIterator();
        }
    }

    @Override
    public Variant next() {
        return variantDBIterator.next();
    }
}
