package org.opencb.opencga.storage.core.variant.adaptors;

import com.google.common.base.Throwables;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;

import java.util.*;
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
    // limit + skip
    private final int maxResults;
    private final int skip;
    // Skip elements first time that hasNext or next is called.
    private boolean pendingSkip;
    // Current number of results
    private int numResults;

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
        this(buildQueryIterator(variantsIterator, batchSize, query), options, iteratorFactory);
    }

    /**
     * @param queryIterator   Query iterator. Provides queries to execute
     * @param options         Query options to be used with the iterator factory
     * @param iteratorFactory Iterator factory. See {@link VariantDBAdaptor#iterator()}
     */
    public MultiVariantDBIterator(Iterator<Query> queryIterator, QueryOptions options,
                                  BiFunction<Query, QueryOptions, VariantDBIterator> iteratorFactory) {
        this.queryIterator = Objects.requireNonNull(queryIterator);
        this.options = options == null ? new QueryOptions() : new QueryOptions(options);
        this.iteratorFactory = Objects.requireNonNull(iteratorFactory);
        variantDBIterator = emptyIterator();

        int limit = this.options.getInt(QueryOptions.LIMIT, 0);
        skip = Math.max(0, this.options.getInt(QueryOptions.SKIP, 0));

        if (limit <= 0) {
            limit = Integer.MAX_VALUE;
            maxResults = limit;
        } else {
            maxResults = limit + skip;
        }

        // Client side limit+skip. Remove from QueryOptions
        this.options.remove(QueryOptions.LIMIT);
        this.options.remove(QueryOptions.SKIP);

        pendingSkip = true;
    }

    @Override
    public boolean hasNext() {
        init();
        if (numResults >= maxResults) {
            return false;
        } else if (!variantDBIterator.hasNext()) {
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
        if (hasNext()) {
            numResults++;
            return variantDBIterator.next();
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * Client side skip.
     */
    private void init() {
        if (pendingSkip) {
            pendingSkip = false;
            int skip = this.skip;
            while (skip > 0 && hasNext()) {
                next();
                skip--;
            }
        }
    }

    private static Iterator<Query> buildQueryIterator(Iterator<?> variantsIterator, int batchSize, Query query) {
        Objects.requireNonNull(variantsIterator);
        return new Iterator<Query>() {
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
    }

}
