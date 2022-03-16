package org.opencb.opencga.storage.hadoop.variant.mr;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.config.ConfigurationOption;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.utils.AbstractHBaseDriver;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantSqlQueryParser;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseVariantConverterConfiguration;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQueryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created on 27/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMapReduceUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(VariantMapReduceUtil.class);


    public static void initTableMapperJob(Job job, String inTable, String outTable, Scan scan, Class<? extends TableMapper> mapperClass)
            throws IOException {
        initTableMapperJob(job, inTable, scan, mapperClass);
        setOutputHBaseTable(job, outTable);
        setNoneReduce(job);
    }

    public static void initTableMapperJob(Job job, String inTable, String outTable, List<Scan> scans,
                                          Class<? extends TableMapper> mapperClass)
            throws IOException {
        initTableMapperJob(job, inTable, scans, mapperClass);
        setOutputHBaseTable(job, outTable);
        setNoneReduce(job);
    }

    public static void initTableMapperMultiOutputJob(Job job, String inTable, List<Scan> scans, Class<? extends TableMapper> mapperClass)
            throws IOException {
        initTableMapperJob(job, inTable, scans, mapperClass);
        setMultiTableOutput(job);
        setNoneReduce(job);
    }

    public static void initPartialResultTableMapperJob(Job job, String inTable, String outTable, Scan scan,
                                                       Class<? extends TableMapper> mapperClass)
            throws IOException {
        LOGGER.info("Allow partial results from HBase");
        initTableMapperJob(job, inTable, scan, mapperClass, PartialResultTableInputFormat.class);
        setOutputHBaseTable(job, outTable);
        setNoneReduce(job);
    }

    public static void initTableMapperJob(Job job, String inTable, Scan scan, Class<? extends TableMapper> mapperClass)
            throws IOException {
        initTableMapperJob(job, inTable, scan, mapperClass, TableInputFormat.class);
    }

    public static void initTableMapperJob(Job job, String inTable, Scan scan, Class<? extends TableMapper> mapperClass,
                                          Class<? extends InputFormat> inputFormatClass)
            throws IOException {
        boolean addDependencyJar = job.getConfiguration().getBoolean(
                HadoopVariantStorageOptions.MR_ADD_DEPENDENCY_JARS.key(),
                HadoopVariantStorageOptions.MR_ADD_DEPENDENCY_JARS.defaultValue());
        LOGGER.info("Use table {} as input", inTable);
        TableMapReduceUtil.initTableMapperJob(
                inTable,      // input table
                scan,             // Scan instance to control CF and attribute selection
                mapperClass,   // mapper class
                null,             // mapper output key
                null,             // mapper output value
                job,
                addDependencyJar,
                inputFormatClass);
    }

    public static void initTableMapperJob(Job job, String inTable, List<Scan> scans, Class<? extends TableMapper> mapperClass)
            throws IOException {
        if (scans.isEmpty()) {
            throw new IllegalArgumentException("There must be at least one scan! Error creating TableMapperJob '" + job.getJobName() + "'");
        } else if (scans.size() == 1) {
            initTableMapperJob(job, inTable, scans.get(0), mapperClass);
        } else {
            boolean addDependencyJar = job.getConfiguration().getBoolean(
                    HadoopVariantStorageOptions.MR_ADD_DEPENDENCY_JARS.key(),
                    HadoopVariantStorageOptions.MR_ADD_DEPENDENCY_JARS.defaultValue());
            LOGGER.info("Use table {} as input", inTable);
            for (Scan scan : scans) {
                scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(inTable));
            }
            TableMapReduceUtil.initTableMapperJob(
                    scans,            // Scan instance to control CF and attribute selection
                    mapperClass,      // mapper class
                    null,             // mapper output key
                    null,             // mapper output value
                    job,
                    addDependencyJar);
        }
    }

    public static void initTableMapperJobFromPhoenix(Job job, String variantTable, String sql,
                                                     Class<? extends Mapper> mapper) {
        job.setInputFormatClass(CustomPhoenixInputFormat.class);

        LOGGER.info(sql);
        PhoenixMapReduceUtil.setInput(job, ExposedResultSetDBWritable.class, variantTable,  sql);
        job.setMapperClass(mapper);

    }

    public static void initVariantMapperJob(Job job, Class<? extends VariantMapper> mapperClass, String variantTable,
                                               VariantStorageMetadataManager metadataManager, Query query, QueryOptions queryOptions,
                                               boolean skipSampleIndex) throws IOException {
        query = new VariantQueryParser(null, metadataManager).preProcessQuery(query, queryOptions);

        setQuery(job, query);
        setQueryOptions(job, queryOptions);
        if (VariantHBaseQueryParser.isSupportedQuery(query)) {
            LOGGER.info("Init MapReduce job reading from HBase");
            boolean useSampleIndex = !skipSampleIndex
                    && SampleIndexQueryParser.validSampleIndexQuery(query);
            if (useSampleIndex) {
                Object regions = query.get(VariantQueryParam.REGION.key());
                Object geneRegions = query.get(VariantQueryUtils.ANNOT_GENE_REGIONS.key());
                // Remove extra fields from the query
                SampleIndexQuery sampleIndexQuery = new SampleIndexDBAdaptor(null, null, metadataManager).parseSampleIndexQuery(query);
                setSampleIndexConfiguration(job, sampleIndexQuery.getSchema().getConfiguration());

                // Preserve regions and gene_regions
                query.put(VariantQueryParam.REGION.key(), regions);
                query.put(VariantQueryUtils.ANNOT_GENE_REGIONS.key(), geneRegions);
                LOGGER.info("Use sample index to read from HBase");
            }

            VariantHBaseQueryParser parser = new VariantHBaseQueryParser(metadataManager);
            List<Scan> scans = parser.parseQueryMultiRegion(query, queryOptions);
            configureMapReduceScans(scans, job.getConfiguration());

            initVariantMapperJobFromHBase(job, variantTable, scans, mapperClass, useSampleIndex);

            int i = 0;
            for (Scan scan : scans) {
                LOGGER.info("[" + ++i + "]Scan: " + scan.toString());
            }
        } else {
            LOGGER.info("Init MapReduce job reading from Phoenix");
            String sql = new VariantSqlQueryParser(variantTable, metadataManager, job.getConfiguration())
                    .parse(query, queryOptions);

            initVariantMapperJobFromPhoenix(job, variantTable, sql, mapperClass);
        }
    }

    public static void initVariantMapperJobFromHBase(Job job, String variantTableName, Scan scan,
                                                     Class<? extends VariantMapper> variantMapperClass)
            throws IOException {
        initVariantMapperJobFromHBase(job, variantTableName, scan, variantMapperClass, false);
    }

    public static void initVariantMapperJobFromHBase(Job job, String variantTableName, Scan scan,
                                                     Class<? extends VariantMapper> variantMapperClass, boolean useSampleIndex)
            throws IOException {
        initVariantMapperJobFromHBase(job, variantTableName, Collections.singletonList(scan), variantMapperClass, useSampleIndex);
    }

    public static void initVariantMapperJobFromHBase(Job job, String variantTableName, List<Scan> scans,
                                                     Class<? extends VariantMapper> variantMapperClass, boolean useSampleIndex)
            throws IOException {
        initTableMapperJob(job, variantTableName, scans, TableMapper.class);

        job.setMapperClass(variantMapperClass);

//        job.getConfiguration().set(TableInputFormat.INPUT_TABLE, variantTableName);
        job.setInputFormatClass(HBaseVariantTableInputFormat.class);
        job.getConfiguration().setBoolean(HBaseVariantTableInputFormat.MULTI_SCANS, scans.size() > 1);
        job.getConfiguration().setBoolean(HBaseVariantTableInputFormat.USE_SAMPLE_INDEX_TABLE_INPUT_FORMAT, useSampleIndex);
    }

    public static void initVariantMapperJobFromPhoenix(Job job, VariantHadoopDBAdaptor dbAdaptor,
                                                       Class<? extends VariantMapper> variantMapperClass)
            throws IOException {
        initVariantMapperJobFromPhoenix(job, dbAdaptor, new Query(), new QueryOptions(), variantMapperClass);
    }

    public static void initVariantMapperJobFromPhoenix(Job job, VariantHadoopDBAdaptor dbAdaptor, Query query, QueryOptions queryOptions,
                                                       Class<? extends VariantMapper> variantMapperClass)
            throws IOException {
        String variantTableName = dbAdaptor.getVariantTable();
        VariantStorageMetadataManager scm = dbAdaptor.getMetadataManager();
        VariantSqlQueryParser variantSqlQueryParser = new VariantSqlQueryParser(variantTableName, scm, false, dbAdaptor.getConfiguration());

        String sql = variantSqlQueryParser.parse(query, queryOptions);

        initVariantMapperJobFromPhoenix(job, variantTableName, sql, variantMapperClass);
    }

    public static void initVariantMapperJobFromPhoenix(Job job, String variantTableName, String sqlQuery,
                                                       Class<? extends VariantMapper> variantMapperClass)
            throws IOException {
        // VariantDBWritable is the DBWritable class that enables us to process the Result of the query
        PhoenixMapReduceUtil.setInput(job, PhoenixVariantTableInputFormat.VariantDBWritable.class, variantTableName,  sqlQuery);

        LOGGER.info(sqlQuery);
        job.setMapperClass(variantMapperClass);

        job.setInputFormatClass(PhoenixVariantTableInputFormat.class);
    }

    public static void initVariantRowMapperJob(Job job, Class<? extends VariantRowMapper> mapperClass, String variantTable,
                                               VariantStorageMetadataManager metadataManager)
            throws IOException {
        initVariantRowMapperJob(job, mapperClass, variantTable, metadataManager, new Query(), new QueryOptions());
    }

    public static void initVariantRowMapperJob(Job job, Class<? extends VariantRowMapper> mapperClass, String variantTable,
                                               VariantStorageMetadataManager metadataManager, Query query, QueryOptions queryOptions)
            throws IOException {
        initVariantRowMapperJob(job, mapperClass, variantTable, metadataManager, query, queryOptions,
                job.getConfiguration().getBoolean("skipSampleIndex", false));
    }

    public static void initVariantRowMapperJob(Job job, Class<? extends VariantRowMapper> mapperClass, String variantTable,
                                               VariantStorageMetadataManager metadataManager, Query query, QueryOptions queryOptions,
                                               boolean skipSampleIndex) throws IOException {
        query = new VariantQueryParser(null, metadataManager).preProcessQuery(query, queryOptions);

        setQuery(job, query);
        setQueryOptions(job, queryOptions);
        if (VariantHBaseQueryParser.isSupportedQuery(query)) {
            LOGGER.info("Init MapReduce job reading from HBase");
            boolean useSampleIndex = !skipSampleIndex
                    && SampleIndexQueryParser.validSampleIndexQuery(query);
            if (useSampleIndex) {
                // Remove extra fields from the query
                SampleIndexQuery sampleIndexQuery = new SampleIndexDBAdaptor(null, null, metadataManager).parseSampleIndexQuery(query);
                setSampleIndexConfiguration(job, sampleIndexQuery.getSchema().getConfiguration());

                LOGGER.info("Use sample index to read from HBase");
            }

            VariantHBaseQueryParser parser = new VariantHBaseQueryParser(metadataManager);
            List<Scan> scans = parser.parseQueryMultiRegion(query, queryOptions);
            configureMapReduceScans(scans, job.getConfiguration());

            initVariantRowMapperJobFromHBase(job, variantTable, scans, mapperClass, useSampleIndex);

            int i = 0;
            for (Scan scan : scans) {
                LOGGER.info("[" + ++i + "]Scan: " + scan.toString());
            }
        } else {
            LOGGER.info("Init MapReduce job reading from Phoenix");
            String sql = new VariantSqlQueryParser(variantTable, metadataManager, job.getConfiguration())
                    .parse(query, queryOptions);

            initVariantRowMapperJobFromPhoenix(job, variantTable, sql, mapperClass);
        }
    }

    public static void setQueryOptions(Job job, QueryOptions queryOptions) {
        setObjectMap(job, queryOptions);
    }

    public static void setQuery(Job job, Query query) {
        setObjectMap(job, query);
    }

    public static void setObjectMap(Job job, ObjectMap objectMap) {
        for (String key : objectMap.keySet()) {
            String value = objectMap.getString(key);
            if (value != null) {
                job.getConfiguration().set(key, value);
            }
        }
    }

    public static void initVariantRowMapperJobFromHBase(Job job, String variantTableName, Scan scan,
                                                     Class<? extends VariantRowMapper> variantMapperClass)
            throws IOException {
        initVariantRowMapperJobFromHBase(job, variantTableName, scan, variantMapperClass, false);
    }

    public static void initVariantRowMapperJobFromHBase(Job job, String variantTableName, Scan scan,
                                                           Class<? extends VariantRowMapper> variantMapperClass, boolean useSampleIndex)
            throws IOException {
        initVariantRowMapperJobFromHBase(job, variantTableName, Collections.singletonList(scan), variantMapperClass, useSampleIndex);
    }

    public static void initVariantRowMapperJobFromHBase(Job job, String variantTableName, List<Scan> scans,
                                                           Class<? extends VariantRowMapper> variantMapperClass, boolean useSampleIndex)
            throws IOException {
        initTableMapperJob(job, variantTableName, scans, TableMapper.class);

        job.setMapperClass(variantMapperClass);

//        job.getConfiguration().set(TableInputFormat.INPUT_TABLE, variantTableName);
        job.setInputFormatClass(HBaseVariantRowTableInputFormat.class);
        job.getConfiguration().setBoolean(HBaseVariantRowTableInputFormat.MULTI_SCANS, scans.size() > 1);
        job.getConfiguration().setBoolean(HBaseVariantRowTableInputFormat.USE_SAMPLE_INDEX_TABLE_INPUT_FORMAT, useSampleIndex);
    }

    public static void initVariantRowMapperJobFromPhoenix(Job job, VariantHadoopDBAdaptor dbAdaptor,
                                                       Class<? extends VariantRowMapper> variantMapperClass)
            throws IOException {
        initVariantRowMapperJobFromPhoenix(job, dbAdaptor, new Query(), new QueryOptions(), variantMapperClass);
    }

    public static void initVariantRowMapperJobFromPhoenix(Job job, VariantHadoopDBAdaptor dbAdaptor, Query query, QueryOptions queryOptions,
                                                       Class<? extends VariantRowMapper> variantMapperClass)
            throws IOException {
        String variantTableName = dbAdaptor.getVariantTable();
        VariantStorageMetadataManager mm = dbAdaptor.getMetadataManager();
        VariantSqlQueryParser variantSqlQueryParser = new VariantSqlQueryParser(variantTableName, mm, false, dbAdaptor.getConfiguration());

        String sql = variantSqlQueryParser.parse(query, queryOptions);

        initVariantRowMapperJobFromPhoenix(job, variantTableName, sql, variantMapperClass);
    }

    public static void initVariantRowMapperJobFromPhoenix(Job job, String variantTableName, String sqlQuery,
                                                       Class<? extends Mapper> variantMapperClass)
            throws IOException {

        LOGGER.info(sqlQuery);
        // VariantDBWritable is the DBWritable class that enables us to process the Result of the query
        PhoenixMapReduceUtil.setInput(job, ExposedResultSetDBWritable.class, variantTableName,  sqlQuery);

        job.setMapperClass(variantMapperClass);
        job.setInputFormatClass(PhoenixVariantRowTableInputFormat.class);
    }

    public static void setNoneReduce(Job job) throws IOException {
        setNumReduceTasks(job, 0);
    }

    public static void setNumReduceTasks(Job job, int numReducers) throws IOException {
        // Don't use "job.getNumReduceTasks" so the default is -1 instead of 1
        int currentReducers = job.getConfiguration().getInt(JobContext.NUM_REDUCES, -1);
        if (currentReducers != numReducers) {
            if (numReducers == 0) {
                LOGGER.info("Set none reduce task");
            } else {
                LOGGER.info("Set {} reduce tasks", numReducers);
            }
        }
        job.setNumReduceTasks(numReducers);
    }

    public static void setNoneTimestamp(Job job) throws IOException {
        job.getConfiguration().set(AbstractVariantsTableDriver.TIMESTAMP, AbstractVariantsTableDriver.NONE_TIMESTAMP);
    }

    public static void setSampleIndexTableInputFormat(Job job) {
        job.setInputFormatClass(SampleIndexTableInputFormat.class);
    }

    public static void setOutputHBaseTable(Job job, String outTable) throws IOException {
        boolean addDependencyJar = job.getConfiguration().getBoolean(
                HadoopVariantStorageOptions.MR_ADD_DEPENDENCY_JARS.key(),
                HadoopVariantStorageOptions.MR_ADD_DEPENDENCY_JARS.defaultValue());
        LOGGER.info("Use table {} as output", outTable);
        TableMapReduceUtil.initTableReducerJob(
                outTable,      // output table
                null,             // reducer class
                job,
                null, null, null, null,
                addDependencyJar);
    }

    public static void setMultiTableOutput(Job job) throws IOException {
        job.setOutputFormatClass(MultiTableOutputFormat.class);
        LOGGER.info("Use multi-table as output");

        boolean addDependencyJars = job.getConfiguration().getBoolean(
                HadoopVariantStorageOptions.MR_ADD_DEPENDENCY_JARS.key(),
                HadoopVariantStorageOptions.MR_ADD_DEPENDENCY_JARS.defaultValue());

        if (addDependencyJars) {
            TableMapReduceUtil.addDependencyJars(job);
        }
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Mutation.class);

        TableMapReduceUtil.initCredentials(job);
    }

    public static void configureVariantConverter(Configuration configuration,
                                                 boolean mutableSamplesPosition,
                                                 boolean studyNameAsStudyId,
                                                 boolean simpleGenotypes,
                                                 String unknownGenotype) {
        configuration.setBoolean(HBaseVariantConverterConfiguration.MUTABLE_SAMPLES_POSITION, mutableSamplesPosition);
        configuration.setBoolean(HBaseVariantConverterConfiguration.STUDY_NAME_AS_STUDY_ID, studyNameAsStudyId);
        configuration.setBoolean(HBaseVariantConverterConfiguration.SIMPLE_GENOTYPES, simpleGenotypes);
        configuration.set(VariantQueryParam.UNKNOWN_GENOTYPE.key(), unknownGenotype);
    }

    public static void configureVCores(Configuration conf) {
//        mapreduce.map.cpu.vcores: 1
//        mapreduce.map.memory.mb: 2560
//        opencga.variant.table.mapreduce.map.java.opts: -Xmx2048m,-XX:+UseG1GC,-Djava.util.concurrent.ForkJoinPool.common.parallelism=1

        // Set parallel pool size
        String fjpKey = "java.util.concurrent.ForkJoinPool.common.parallelism";
        boolean hasForkJoinPool = false;
        Integer vCores = conf.getInt("mapreduce.map.cpu.vcores", 1);
        Collection<String> opts = conf.getStringCollection("opencga.variant.table.mapreduce.map.java.opts");
        String optString = StringUtils.EMPTY;
        if (!opts.isEmpty()) {
            for (String opt : opts) {
                if (opt.contains(fjpKey)) {
                    hasForkJoinPool = true;
                }
                optString += opt + " ";
            }
        }
        if (!hasForkJoinPool && vCores > 1) {
            optString += " -D" + fjpKey + "=" + vCores;
            LOGGER.warn("Force ForkJoinPool to {}", vCores);
        }
        if (StringUtils.isNotBlank(optString)) {
            LOGGER.info("Set mapreduce java opts: {}", optString);
            conf.set("mapreduce.map.java.opts", optString);
        }
    }

    public static QueryOptions getQueryOptionsFromConfig(Configuration conf) {
        QueryOptions options = new QueryOptions();
        getQueryOptionsFromConfig(options, conf);
        return options;
    }

    public static void getQueryOptionsFromConfig(QueryOptions options, Configuration conf) {
        options.put(QueryOptions.INCLUDE, conf.get(QueryOptions.INCLUDE));
        options.put(QueryOptions.EXCLUDE, conf.get(QueryOptions.EXCLUDE));
    }

    public static Query getQueryFromConfig(Configuration conf) {
        Query query = new Query();
        getQueryFromConfig(query, conf);
        return query;
    }

    public static void getQueryFromConfig(Query query, Configuration conf) {
        for (VariantQueryParam param : VariantQueryParam.values()) {
            addParam(query, conf, param);
        }
        for (QueryParam param : VariantQueryUtils.INTERNAL_VARIANT_QUERY_PARAMS) {
            addParam(query, conf, param);
        }
    }

    public static void setSampleIndexConfiguration(Job job, SampleIndexConfiguration configuration) {
        try {
            String str = JacksonUtils.getDefaultNonNullObjectMapper().writeValueAsString(configuration);
            job.getConfiguration().set(SampleIndexConfiguration.class.getName(), str);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static SampleIndexConfiguration getSampleIndexConfiguration(Configuration conf) {
        String value = conf.get(SampleIndexConfiguration.class.getName());
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException("Missing " + SampleIndexConfiguration.class.getName());
        } else {
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                return objectMapper.readValue(value, SampleIndexConfiguration.class);
            } catch (JsonProcessingException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static void addParam(Query query, Configuration conf, QueryParam param) {
        String value = conf.get(param.key(), conf.get("--" + param.key()));
        if (value != null && !value.isEmpty()) {
            query.put(param.key(), value);
        }
    }

    public static Scan configureMapReduceScan(Scan scan, Configuration conf) {
        configureMapReduceScans(Collections.singletonList(scan), conf);
        return scan;
    }

    public static List<Scan> configureMapReduceScans(List<Scan> scans, Configuration conf) {
        int caching = Integer.parseInt(getParam(conf, HadoopVariantStorageOptions.MR_HBASE_SCAN_CACHING));
        int maxColumns = Integer.parseInt(getParam(conf, HadoopVariantStorageOptions.MR_HBASE_SCAN_MAX_COLUMNS));
        int maxFilters = Integer.parseInt(getParam(conf, HadoopVariantStorageOptions.MR_HBASE_SCAN_MAX_FILTERS));

        LOGGER.info("Scan set Caching to " + caching);
        int actualColumns = scans.get(0).getFamilyMap()
                .getOrDefault(GenomeHelper.COLUMN_FAMILY_BYTES, Collections.emptyNavigableSet()).size();
        boolean removeColumns = actualColumns > maxColumns;
        if (removeColumns) {
            LOGGER.info("Scan with {} columns exceeds the max threshold of {} columns. Returning all columns",
                    scans.get(0).getFamilyMap().get(GenomeHelper.COLUMN_FAMILY_BYTES).size(),
                    maxColumns);
        }
        Filter f = scans.get(0).getFilter();
        int numFilters = getNumFilters(f);
        if (numFilters > maxFilters) {
            LOGGER.info("Scan with {} filters exceeds the max threshold of {} filters. Do not apply filters",
                    numFilters,
                    maxFilters);
        }

        for (Scan scan : scans) {
            scan.setCaching(caching);        // 1 is the default in Scan
            scan.setCacheBlocks(false);  // don't set to true for MR jobs
            if (removeColumns) {
                scan.getFamilyMap().get(GenomeHelper.COLUMN_FAMILY_BYTES).clear();
            }
            if (numFilters > maxColumns) {
                scan.setFilter(null);
            }
        }

        return scans;
    }

    private static int getNumFilters(Filter f) {
        if (f == null) {
            return 0;
        } else if (f instanceof FilterList) {
            List<Filter> filters = ((FilterList) f).getFilters();
            int i = 1;
            for (Filter filter : filters) {
                i += getNumFilters(filter);
            }
            return i;
        } else {
            return 1;
        }
    }

    public static String getParam(Configuration conf, ConfigurationOption key) {
        Object defaultValue = key.defaultValue();
        return getParam(conf, key.key(), defaultValue == null ? null : defaultValue.toString());
    }
    public static String getParam(Configuration conf, String key) {
        return getParam(conf, key, null);
    }

    public static String getParam(Configuration conf, String key, String defaultValue) {
        return getParam(conf, key, defaultValue, null);
    }

    /**
     * Reads a param that might come in different forms. It will take the the first value in this order:
     * - "--{key}"
     * - "packageName.className.{key}"
     * - "className.{key}"
     * - "{key}"
     * - "{defaultvalue}"
     *
     * @param conf          Configuration from where to read the value.
     * @param key           Key to read
     * @param defaultValue  Default value
     * @param aClass        Optional class
     * @return              The value
     */
    public static String getParam(Configuration conf, String key, String defaultValue, Class<? extends AbstractHBaseDriver> aClass) {
        String value = conf.get("--" + key);
        if (aClass != null) {
            if (StringUtils.isEmpty(value)) {
                value = conf.get(aClass.getName() + "." + key);
            }
            if (StringUtils.isEmpty(value)) {
                value = conf.get(aClass.getSimpleName() + "." + key);
            }
        }
        if (StringUtils.isEmpty(value)) {
            value = conf.get(key);
        }
        if (StringUtils.isEmpty(value)) {
            value = defaultValue;
        }
        return value;
    }
}
