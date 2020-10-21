package org.opencb.opencga.analysis.variant.knockout;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.collect.Iterators;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.knockout.result.KnockoutByGene;
import org.opencb.opencga.analysis.variant.knockout.result.KnockoutByIndividual;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.job.Job;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public class KnockoutAnalysisResultReader {

    private final CatalogManager catalogManager;

    public KnockoutAnalysisResultReader(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    public DataResult<KnockoutByIndividual> readKnockoutByIndividualFromJob(String study, String jobId, int limit, int skip,
            Predicate<KnockoutByIndividual> filter, String token) throws IOException, CatalogException {
        return readKnockoutObjectFromJob( study, jobId, limit, skip, filter, token,
                KnockoutAnalysis.KNOCKOUT_INDIVIDUALS_JSON, KnockoutByIndividual.class);
    }

    public DataResult<KnockoutByGene> readKnockoutByGeneFromJob(String study, String jobId, int limit, int skip,
            Predicate<KnockoutByGene> filter, String token) throws IOException, CatalogException {
        return readKnockoutObjectFromJob(study, jobId, limit, skip, filter, token,
                KnockoutAnalysis.KNOCKOUT_GENES_JSON, KnockoutByGene.class);
    }

    private <T> DataResult<T> readKnockoutObjectFromJob(String study, String jobId, int limit, int skip, Predicate<T> filter, String token,
                                                               String fileName, Class<T> c)
            throws IOException, CatalogException {
        StopWatch started = StopWatch.createStarted();
        Job job = catalogManager.getJobManager().get(study, jobId, new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                JobDBAdaptor.QueryParams.OUTPUT.key(),
                JobDBAdaptor.QueryParams.TOOL.key(),
                JobDBAdaptor.QueryParams.INTERNAL_STATUS.key())), token).first();

        if (!job.getTool().getId().equals(KnockoutAnalysis.ID)) {
            throw new IllegalArgumentException("Input job '" + job.getId() + "' was not produced by Knockout Analysis Tool");
        }
        if (!job.getInternal().getStatus().getName().equals(Enums.ExecutionStatus.DONE)) {
            throw new IllegalArgumentException("Unable to query by job with status '" + job.getInternal().getStatus().getName() + "'");
        }
        for (org.opencb.opencga.core.models.file.File file : job.getOutput()) {
            if (file.getName().equals(fileName)) {
                return readKnockoutObjectFromFile(study, file.getId(), filter, limit, skip, c, token)
                        .setTime(((int) started.getTime(TimeUnit.MILLISECONDS)));
            }
        }
        throw new IllegalArgumentException("File '" + fileName + "' not found in job '" + job.getId() + "'");
    }

    private <T> DataResult<T> readKnockoutObjectFromFile(String study, String fileId, Predicate<T> filter,
                                                                int limit, int skip, Class<T> c, String token)
            throws IOException, CatalogException {
        StopWatch started = StopWatch.createStarted();
        if (limit < 0) {
            limit = Integer.MAX_VALUE;
        }
        if (skip < 0) {
            skip = 0;
        }
        ObjectReader reader = JacksonUtils.getDefaultObjectMapper().reader().forType(c);
        try (InputStream is = catalogManager.getFileManager().download(study, fileId, token);
             MappingIterator<T> iterator = reader.readValues(is)) {
            Iterator<T> filtered = Iterators.filter(iterator, filter::test);
            int numMatches = Iterators.advance(iterator, skip);
            List<T> values = new ArrayList<>(Math.min(limit, 1000));
            for (int i = 0; i < limit && filtered.hasNext(); i++) {
                values.add(filtered.next());
                numMatches++;
            }
            numMatches += Iterators.size(filtered);
            return new DataResult<>(((int) started.getTime(TimeUnit.MILLISECONDS)), Collections.emptyList(), values.size(), values,
                    numMatches);
        }
    }
}
