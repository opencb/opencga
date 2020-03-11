package org.opencb.opencga.storage.core.variant.query.executors;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantIterable;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.*;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.ALL;

/**
 * Created on 09/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CompoundHeterozygousQueryExecutorTest {

    private VariantIterable iterable;
    private CompoundHeterozygousQueryExecutor ch;

    @Before
    public void setUp() throws Exception {
        iterable = Mockito.mock(VariantIterable.class);
        Mockito.doAnswer(invocation -> VariantDBIterator.emptyIterator()).when(iterable).iterator(Mockito.any(), Mockito.any());

        ch = new CompoundHeterozygousQueryExecutor(null, null, null, iterable);
    }

    @Test
    public void testBuildQueryOptions() {
        QueryOptions options = ch.buildQueryOptions(new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(ID))
                .append(QueryOptions.LIMIT, 100)
                .append(QueryOptions.SKIP, 100)
        );
        assertFalse(options.containsKey(QueryOptions.LIMIT));
        assertFalse(options.containsKey(QueryOptions.SKIP));

        Set<VariantField> includeFields = getIncludeFields(options);
        assertEquals(new HashSet<>(
                Arrays.asList(ID,
                        ANNOTATION, ANNOTATION_CONSEQUENCE_TYPES,
                        STUDIES, STUDIES_SAMPLES_DATA)), includeFields);


        options = ch.buildQueryOptions(new QueryOptions()
                .append(QueryOptions.EXCLUDE, Arrays.asList(ANNOTATION, STUDIES))
        );
        includeFields = getIncludeFields(options);
        assertEquals(new HashSet<>(
                Arrays.asList(ID, CHROMOSOME, START, END, REFERENCE, ALTERNATE, TYPE, LENGTH, HGVS, SV,
                        ANNOTATION, ANNOTATION_CONSEQUENCE_TYPES,
                        STUDIES, STUDIES_SAMPLES_DATA)), includeFields);
    }

    @Test
    public void testGetAndCheckIncludeSample() {

        assertEquals(Arrays.asList("proband", "father", "mother"),
                ch.getAndCheckIncludeSample(new Query(), "proband", "father", "mother"));

        assertEquals(Arrays.asList("proband", "father", "mother"),
                ch.getAndCheckIncludeSample(new Query(VariantQueryParam.INCLUDE_SAMPLE.key(), ALL), "proband", "father", "mother"));

        assertEquals(Arrays.asList("proband", "father", "mother"),
                ch.getAndCheckIncludeSample(new Query(VariantQueryParam.INCLUDE_SAMPLE.key(), "proband,father,mother"), "proband", "father", "mother"));

        assertEquals(Arrays.asList("mother", "father", "proband"),
                ch.getAndCheckIncludeSample(new Query(VariantQueryParam.INCLUDE_SAMPLE.key(), "mother,father,proband"), "proband", "father", "mother"));

        assertEquals(Arrays.asList("mother", "other1", "father", "proband", "other2"),
                ch.getAndCheckIncludeSample(new Query(VariantQueryParam.INCLUDE_SAMPLE.key(), "mother,other1,father,proband,other2"), "proband", "father", "mother"));

        assertEquals(Arrays.asList("proband", "mother"),
                ch.getAndCheckIncludeSample(new Query(), "proband", "-", "mother"));

        assertEquals(Arrays.asList("proband", "father"),
                ch.getAndCheckIncludeSample(new Query(), "proband", "father", "-"));

    }

    @Test(expected = VariantQueryException.class)
    public void testGetAndCheckIncludeSampleFailMissingParents() {
        ch.getAndCheckIncludeSample(new Query(), "proband", "-", "-");
    }

    @Test(expected = VariantQueryException.class)
    public void testGetAndCheckIncludeSampleFailMissingFather() {
        ch.getAndCheckIncludeSample(new Query(VariantQueryParam.INCLUDE_SAMPLE.key(), "mother,other1,proband,other2"), "proband", "father", "mother");
    }

}