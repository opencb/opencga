package org.opencb.opencga.storage.hadoop.variant.exporters;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver;
import org.opencb.opencga.storage.hadoop.variant.annotation.PhoenixVariantAnnotationWritable;

import java.io.IOException;
import java.util.Objects;

/**
 * Created by mh719 on 21/11/2016.
 * @author Matthias Haimel
 */
public class VariantTableExportDriver extends AbstractAnalysisTableDriver {
    @Deprecated
    public static final String CONFIG_VARIANT_TABLE_EXPORT_AVRO_PATH = "opencga.variant.table.export.avro.path";
    @Deprecated
    public static final String CONFIG_VARIANT_TABLE_EXPORT_AVRO_GENOTYPE = "opencga.variant.table.export.avro.genotype";

    public static final String CONFIG_VARIANT_TABLE_EXPORT_PATH = "opencga.variant.table.export.path";
    public static final String CONFIG_VARIANT_TABLE_EXPORT_TYPE = "opencga.variant.table.export.type";
    public static final String CONFIG_VARIANT_TABLE_EXPORT_GENOTYPE = "opencga.variant.table.export.genotype";
    private String outFile;
    private ExportType type;

    public enum ExportType {AVRO, VCF};

    public VariantTableExportDriver() { /* nothing */ }

    public VariantTableExportDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() {
        outFile = null;
        if (!Objects.isNull(getConf().get(CONFIG_VARIANT_TABLE_EXPORT_AVRO_PATH, null))) {
            outFile = getConf().get(CONFIG_VARIANT_TABLE_EXPORT_AVRO_PATH);
        }

        String typeString = getConf().get(CONFIG_VARIANT_TABLE_EXPORT_TYPE, ExportType.AVRO.name());
        this.type = ExportType.valueOf(typeString);

        outFile = getConf().get(CONFIG_VARIANT_TABLE_EXPORT_PATH, outFile);
        if (StringUtils.isEmpty(outFile)) {
            throw new IllegalArgumentException("No output file specified!!!");
        }
    }

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return null;
    }

    private Class<? extends Mapper> getPhoenixMapperClass() {
        return AnalysisToFileMapper.class;
    }

    @Override
    protected void initMapReduceJob(String inTable, Job job, Scan scan, boolean addDependencyJar) throws IOException {
        String phoenixInputTable = SchemaUtil.getEscapedFullTableName(inTable);
//        final String selectQuery = "SELECT * FROM " + phoenixInputTable; // default -> retrieve all data.

        PhoenixMapReduceUtil.setInput(job, PhoenixVariantAnnotationWritable.class, phoenixInputTable, StringUtils.EMPTY);
        job.setMapperClass(getPhoenixMapperClass());

        FileOutputFormat.setOutputPath(job, new Path(this.outFile)); // set Path
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class); // compression
        switch (this.type) {
            case AVRO:
                job.setOutputFormatClass(AvroKeyOutputFormat.class);
                AvroJob.setOutputKeySchema(job, VariantAvro.getClassSchema()); // Set schema
                break;
            case VCF:
                job.setOutputFormatClass(HadoopVcfOutputFormat.class);
                break;
            default:
                throw new IllegalStateException("Type not known: " + this.type);
        }
        job.setNumReduceTasks(0);
        TableMapReduceUtil.addDependencyJars(job);
    }

    @Override
    protected void postExecution(boolean succeed) throws IOException, StorageManagerException {
        super.postExecution(succeed);
        StudyConfiguration studyConfiguration = loadStudyConfiguration();
        writeMetadata(studyConfiguration, this.outFile + ".studyConfiguration");
    }

    protected void writeMetadata(StudyConfiguration studyConfiguration, String output) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Path path = new Path(output);
        FileSystem fs = FileSystem.get(getConf());
        try (FSDataOutputStream fos = fs.create(path)) {
            objectMapper.writeValue(fos, studyConfiguration);
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, null, new VariantTableExportDriver()));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}
