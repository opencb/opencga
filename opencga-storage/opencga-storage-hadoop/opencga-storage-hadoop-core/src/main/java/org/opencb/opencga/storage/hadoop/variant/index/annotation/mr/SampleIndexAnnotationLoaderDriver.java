package org.opencb.opencga.storage.hadoop.variant.index.annotation.mr;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexAnnotationLoader;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantAlignedInputFormat;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created on 25/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexAnnotationLoaderDriver extends AbstractVariantsTableDriver {

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleIndexAnnotationLoaderDriver.class);
    public static final String OUTPUT = "output-table";
    public static final String SAMPLES = "samples";
    public static final String SAMPLE_IDS = "sampleIds";

    private List<Integer> sampleIds;
    private boolean hasGenotype;
    private String region;
    private String outputTable;
    private int sampleIndexVersion;

    @Override
    protected Class<SampleIndexAnnotationLoaderMapper> getMapperClass() {
        return SampleIndexAnnotationLoaderMapper.class;
    }

    @Override
    protected Map<String, String> getParams() {
        Map<String, String> params = new HashMap<>();
        params.put("--" + SAMPLES, "<samples>");
        params.put("--" + SAMPLE_IDS, "<sample-ids>");
        params.put("--" + OUTPUT, "<output-table>");
        params.put("--" + VariantQueryParam.REGION.key(), "<region>");
        return params;
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();

        outputTable = getParam(OUTPUT);
        if (StringUtils.isEmpty(outputTable)) {
            throw new IllegalArgumentIOException("Missing output table");
        }

        VariantStorageMetadataManager metadataManager = getMetadataManager();
        String samples = getParam(SAMPLES);
        String sampleIdsStr = getParam(SAMPLE_IDS);
        if (StringUtils.isNotEmpty(samples) && StringUtils.isNotEmpty(sampleIdsStr)) {
            throw new IllegalArgumentException("Incompatible params " + SAMPLES + " and " + SAMPLE_IDS);
        }
        if (StringUtils.isNotEmpty(samples) && !samples.equals(VariantQueryUtils.ALL)) {
            sampleIds = new LinkedList<>();
            for (String sample : samples.split(",")) {
                Integer sampleId = metadataManager.getSampleId(getStudyId(), sample);
                if (sampleId == null) {
                    throw new IllegalArgumentException("Sample '" + sample + "' not found.");
                }
                sampleIds.add(sampleId);
            }
        } else if (StringUtils.isNotEmpty(sampleIdsStr)) {
            sampleIds = new LinkedList<>();
            for (String sample : sampleIdsStr.split(",")) {
                sampleIds.add(Integer.valueOf(sample));
            }
        } else {
            sampleIds = metadataManager.getIndexedSamples(getStudyId());
        }

        if (sampleIds.isEmpty()) {
            throw new IllegalArgumentException("No samples to update!");
        } else {
            LOGGER.info("Update sample index annotation to " + sampleIds.size() + " samples");
        }

        List<String> fixedFormat = HBaseToVariantConverter.getFixedFormat(metadataManager.getStudyMetadata(getStudyId()));
        hasGenotype = fixedFormat.contains(VCFConstants.GENOTYPE_KEY);

        if (hasGenotype) {
            LOGGER.info("Study with genotypes. Study fixed format: " + fixedFormat);
        } else {
            LOGGER.info("Study without genotypes. Study fixed format: " + fixedFormat);
        }

        region = getParam(VariantQueryParam.REGION.key(), "");
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable) throws IOException {

        Scan scan = new Scan();

        if (StringUtils.isNotEmpty(region)) {
            VariantHBaseQueryParser.addRegionFilter(scan, new Region(region));
        }

        VariantMapReduceUtil.configureMapReduceScan(scan, getConf());

        boolean multiFileSamples = false;
        for (Integer sampleId : sampleIds) {
            SampleMetadata sampleMetadata = getMetadataManager().getSampleMetadata(getStudyId(), sampleId);
            List<PhoenixHelper.Column> sampleColumns = VariantPhoenixSchema.getSampleColumns(sampleMetadata);
            if (sampleColumns.size() > 1) {
                multiFileSamples = true;
            }
            for (PhoenixHelper.Column column : sampleColumns) {
                scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, column.bytes());
            }
        }
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantPhoenixSchema.VariantColumn.FULL_ANNOTATION.bytes());

        SampleIndexAnnotationLoaderMapper.setHasGenotype(job, hasGenotype);
        SampleIndexAnnotationLoaderMapper.setMultiFileSamples(job, multiFileSamples);
        SampleIndexAnnotationLoaderMapper.setSampleIdRange(job, sampleIds);

        VariantMapReduceUtil.initTableMapperJob(job, variantTable,
                scan, getMapperClass(), VariantAlignedInputFormat.class);
        VariantAlignedInputFormat.setDelegatedInputFormat(job, TableInputFormat.class);
        VariantAlignedInputFormat.setBatchSize(job, SampleIndexSchema.BATCH_SIZE);

        VariantMapReduceUtil.setOutputHBaseTable(job, outputTable);

        VariantMapReduceUtil.setNoneReduce(job);

        StudyMetadata.SampleIndexConfigurationVersioned versioned = getMetadataManager()
                .getStudyMetadata(getStudyId())
                .getSampleIndexConfigurationLatest();
        SampleIndexConfiguration configuration = versioned.getConfiguration();
        sampleIndexVersion = versioned.getVersion();
        VariantMapReduceUtil.setSampleIndexConfiguration(job, configuration, sampleIndexVersion);

        return job;
    }

    @Override
    protected String getJobOperationName() {
        return "sample_index_annotation_loader";
    }

    @Override
    protected void postExecution(boolean succeed) throws IOException, StorageEngineException {
        super.postExecution(succeed);
        if (succeed && StringUtils.isEmpty(region)) {
            SampleIndexAnnotationLoader.postAnnotationLoad(getStudyId(), sampleIds, getMetadataManager(), sampleIndexVersion);
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(new SampleIndexAnnotationLoaderDriver().privateMain(args, null));
        } catch (Exception e) {
            LOGGER.error("Error executing " + SampleIndexAnnotationLoaderDriver.class, e);
            System.exit(1);
        }
    }

    public static String[] buildArgs(String archiveTable, String variantsTable, int studyId, Collection<?> sampleIds, ObjectMap other) {
        ObjectMap params = new ObjectMap(other);
        params.put(SAMPLE_IDS, sampleIds);
        return AbstractVariantsTableDriver.buildArgs(archiveTable, variantsTable, studyId, null, params);
    }

}
