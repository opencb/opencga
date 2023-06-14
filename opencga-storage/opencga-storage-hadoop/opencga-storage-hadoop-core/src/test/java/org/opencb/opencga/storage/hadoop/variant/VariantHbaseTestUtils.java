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

/**
 * 
 */
package org.opencb.opencga.storage.hadoop.variant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import org.apache.commons.lang3.CharUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.types.*;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.utils.CompressionUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.family.MendelianErrorSampleIndexConverter;
import org.opencb.opencga.storage.hadoop.variant.index.sample.*;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;

import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.DataFormatException;

import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.getTmpRootDir;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest.configuration;

/**
 *  Utility class for VariantStorage hadoop tests
 *
 *  @author Matthias Haimel mh719+git@cam.ac.uk
 */
public class VariantHbaseTestUtils {

    public static VariantHadoopDBAdaptor printVariantsFromArchiveTable(VariantHadoopDBAdaptor dbAdaptor,
                                                                       StudyMetadata studyMetadata) throws Exception {
        return printVariantsFromArchiveTable(dbAdaptor, studyMetadata, System.out);
    }

    public static VariantHadoopDBAdaptor printVariantsFromArchiveTable(VariantHadoopDBAdaptor dbAdaptor,
                                                                       StudyMetadata studyMetadata, Path output)
            throws Exception {
        if (output.toFile().isDirectory()) {
            String archiveTableName = dbAdaptor.getArchiveTableName(studyMetadata.getId());
            output = output.resolve("archive._V." + archiveTableName + "." + TimeUtils.getTimeMillis() + ".txt");
        }

        try (FileOutputStream out = new FileOutputStream(output.toFile())) {
            return printVariantsFromArchiveTable(dbAdaptor, studyMetadata, new PrintStream(out));
        }
    }

    public static VariantHadoopDBAdaptor printVariantsFromArchiveTable(VariantHadoopDBAdaptor dbAdaptor,
                                                                       StudyMetadata studyMetadata, PrintStream out)
            throws Exception {
        String archiveTableName = dbAdaptor.getArchiveTableName(studyMetadata.getId());
        if (!dbAdaptor.getHBaseManager().tableExists(archiveTableName)) {
            return dbAdaptor;
        }
        dbAdaptor.getHBaseManager().act(archiveTableName, table -> {
            for (Result result : table.getScanner(GenomeHelper.COLUMN_FAMILY_BYTES)) {
                out.println("-----------------");
                out.println(Bytes.toString(result.getRow()));
                for (Cell c : result.rawCells()) {
                    out.println('\t' + Bytes.toString(CellUtil.cloneQualifier(c)));
                }
//                GenomeHelper.getVariantColumns(result.rawCells()).stream()
//                        .filter(c -> Bytes.startsWith(CellUtil.cloneFamily(c), helper.getColumnFamily()))
//                        .forEach(c -> {
//                            out.println("-----------------");
//                            out.println(Bytes.toString(CellUtil.cloneRow(c)) + " : " + Bytes.toString(CellUtil.cloneQualifier(c)));
//                            try {
//                                byte[] value = CellUtil.cloneValue(c);
//                                out.println(VariantTableStudyRowsProto.parseFrom(value));
//                            } catch (Exception e) {
//                                e.printStackTrace(out);
//                            }
//                        });

            }
            return 0;
        });
        return dbAdaptor;
    }

    public static void printVariantsFromVariantsTable(VariantHadoopDBAdaptor dbAdaptor) throws IOException {
        printVariantsFromVariantsTable(dbAdaptor, getTmpRootDir());
    }

    public static void printVariantsFromVariantsTable(VariantHadoopDBAdaptor dbAdaptor, Path dir) throws IOException {
        String tableName = HBaseVariantTableNameGenerator.getVariantTableName(VariantStorageBaseTest.DB_NAME, dbAdaptor.getConfiguration());
        HBaseManager hm = dbAdaptor.getHBaseManager();
        if (!hm.tableExists(tableName)) {
            System.out.println("Table " + tableName + " does not exist!");
            return;
        }
        System.out.println("Query from HBase : " + tableName);
        Path outputFile;
        if (dir.toFile().isDirectory()) {
            outputFile = dir.resolve("variant." + tableName + "." + TimeUtils.getTimeMillis() + ".txt");
        } else {
            outputFile = dir;
        }
        System.out.println("Variant table file = " + outputFile);
        PrintStream os = new PrintStream(new FileOutputStream(outputFile.toFile()));
        int numVariants = hm.act(tableName, table -> {
            int num = 0;
            byte[] family = GenomeHelper.COLUMN_FAMILY_BYTES;
            ResultScanner resultScanner = table.getScanner(family);
            for (Result result : resultScanner) {
                Variant variant;
                try {
                    variant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(result.getRow());
                } catch (RuntimeException e) {
                    os.println(Arrays.toString(result.getRow()));
                    os.println("--------------------");
                    continue;
                }
                os.println("Variant = " + variant + "  " + Bytes.toStringBinary(result.getRow()));
                for (Map.Entry<byte[], byte[]> entry : result.getFamilyMap(family).entrySet()) {
                    String key = Bytes.toString(entry.getKey());
                    PhoenixHelper.Column column = VariantPhoenixSchema.VariantColumn.getColumn(key);
                    if (column != null) {
                        os.println("\t" + key + " = " + length(entry.getValue()) + ", "
                                + column.getPDataType().toObject(entry.getValue())
                         + ", ts:" + result.getColumnLatestCell(family, column.bytes()).getTimestamp());
                    } else if (key.endsWith(VariantPhoenixSchema.COHORT_STATS_PROTOBUF_SUFFIX)) {
//                        ComplexFilter complexFilter = ComplexFilter.parseFrom(entry.getValue());
                        os.println("\t" + key + " = " + length(entry.getValue()) + ", " + Arrays.toString(entry.getValue()));
                    } else if (key.startsWith(VariantPhoenixSchema.POPULATION_FREQUENCY_PREFIX)
                            || key.endsWith(VariantPhoenixSchema.COHORT_STATS_FREQ_SUFFIX)
                            || key.endsWith(VariantPhoenixSchema.VARIANT_SCORE_SUFIX)) {
                        os.println("\t" + key + " = " + length(entry.getValue()) + ", " + PFloatArray.INSTANCE.toObject(entry.getValue()));
                    } else if (key.endsWith(VariantPhoenixSchema.STUDY_SUFIX)) {
                        os.println("\t" + key + " = " + PUnsignedInt.INSTANCE.toObject(entry.getValue()));
                    } else if (key.endsWith(VariantPhoenixSchema.SAMPLE_DATA_SUFIX) || key.endsWith(VariantPhoenixSchema.FILE_SUFIX)) {
                        os.println("\t" + key + " = " + result.getColumnLatestCell(GenomeHelper.COLUMN_FAMILY_BYTES, entry.getKey()).getTimestamp()+", " + PVarcharArray.INSTANCE.toObject(entry.getValue()));
                    } else if (key.endsWith(VariantPhoenixSchema.COHORT_STATS_MAF_SUFFIX)
                            || key.endsWith(VariantPhoenixSchema.COHORT_STATS_MGF_SUFFIX)) {
                        os.println("\t" + key + " = " + PFloat.INSTANCE.toObject(entry.getValue()));
                    } else if (key.startsWith(VariantPhoenixSchema.RELEASE_PREFIX)) {
                        os.println("\t" + key + " = " + PBoolean.INSTANCE.toObject(entry.getValue()));
                    } else if (key.endsWith(VariantPhoenixSchema.FILL_MISSING_SUFIX)) {
                        os.println("\t" + key + " = " + PInteger.INSTANCE.toObject(entry.getValue()));
                    } else if (entry.getValue().length == 4) {
                        Object o = null;
                        try {
                            o = PUnsignedInt.INSTANCE.toObject(entry.getValue());
                        } catch (IllegalDataException ignore) {
                        }
                        os.println("\t" + key + " = "
                                + PInteger.INSTANCE.toObject(entry.getValue()) + " , "
                                + o + " , "
                                + PFloat.INSTANCE.toObject(entry.getValue()) + " , ");
                    } else {
                        os.println("\t" + key + " ~ " + length(entry.getValue()) + ", "
                                + Bytes.toString(entry.getValue()));
                    }

                }
                os.println("--------------------");
                num++;
            }
            os.close();
            resultScanner.close();
            return num;
        });
    }

    private static String length(byte[] array) {
        return "(" + array.length + " B)";
    }

    private static void printVariantsFromDBAdaptor(VariantHadoopDBAdaptor dbAdaptor, Path dir) throws IOException {
        String tableName = HBaseVariantTableNameGenerator.getVariantTableName(VariantStorageBaseTest.DB_NAME, dbAdaptor.getConfiguration());
        Path outputFile;
        if (dir.toFile().isDirectory()) {
            outputFile = dir.resolve("variant." + tableName + "." + TimeUtils.getTimeMillis() + ".json");
        } else {
            outputFile = dir;
        }
        System.out.println("Variant table file = " + outputFile);
        try (OutputStream os = new BufferedOutputStream(new FileOutputStream(outputFile.toFile()))) {
            PrintStream out = new PrintStream(os);
            printVariantsFromDBAdaptor(dbAdaptor, out);
        }
    }

    private static void printVariantsFromDBAdaptor(VariantHadoopDBAdaptor dbAdaptor, PrintStream out) {
        VariantDBIterator iterator = dbAdaptor.iterator(new Query(VariantQueryParam.INCLUDE_SAMPLE_DATA.key(), "all,SAMPLE_ID")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), ParamConstants.ALL),
                new QueryOptions("simpleGenotypes", true));
        ObjectMapper mapper = new ObjectMapper().configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        while (iterator.hasNext()) {
            Variant variant = iterator.next();
            try {
                out.println(mapper.writeValueAsString(variant));
            } catch (JsonProcessingException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    public static void printArchiveTable(StudyMetadata studyMetadata, VariantHadoopDBAdaptor dbAdaptor, Path outDir)
            throws Exception {
        String archiveTableName = dbAdaptor.getArchiveTableName(studyMetadata.getId());
        for (Integer fileId : dbAdaptor.getMetadataManager().getIndexedFiles(studyMetadata.getId())) {
            if (dbAdaptor.getMetadataManager().getFileMetadata(studyMetadata.getId(), fileId).getType() == FileMetadata.Type.VIRTUAL) {
                continue;
            }
            String fileName = "archive." + fileId + "." + archiveTableName + "." + TimeUtils.getTimeMillis() + ".txt";
            try (PrintStream os = new PrintStream(new FileOutputStream(outDir.resolve(fileName).toFile()))) {
                printArchiveTable(dbAdaptor, studyMetadata, fileId, os);
            }
        }
    }

    public static void printArchiveTable(VariantHadoopDBAdaptor dbAdaptor,
                                                           StudyMetadata studyMetadata, int fileId, PrintStream os)
            throws Exception {
//        VariantHadoopArchiveDBIterator archive = (VariantHadoopArchiveDBIterator) dbAdaptor.iterator(
//                new Query()
//                        .append(VariantQueryParam.STUDY.key(), studyMetadata.getStudyId())
//                        .append(VariantQueryParam.FILE.key(), fileId),
//                new QueryOptions("archive", true));

        String tableName = dbAdaptor.getArchiveTableName(studyMetadata.getId());
        if (!dbAdaptor.getHBaseManager().tableExists(tableName)) {
            os.println("Table " + tableName + " does not exist");
            return;
        }

        ArchiveTableHelper archiveHelper = dbAdaptor.getArchiveHelper(studyMetadata.getId(), fileId);
        Scan scan = new Scan();
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, archiveHelper.getNonRefColumnName());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, archiveHelper.getRefColumnName());
        VariantHBaseQueryParser.addArchiveRegionFilter(scan, null, archiveHelper);

        dbAdaptor.getHBaseManager().act(tableName, (table) -> {
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                os.println("--------------------");
                os.println(Bytes.toString(result.getRow()));

                os.println("\t" + Bytes.toString(archiveHelper.getNonRefColumnName()));
                byte[] value = result.getValue(GenomeHelper.COLUMN_FAMILY_BYTES, archiveHelper.getNonRefColumnName());
                if (value != null) {
                    VcfSliceProtos.VcfSlice vcfSlice = VcfSliceProtos.VcfSlice.parseFrom(value);
                    for (String s : vcfSlice.toString().split("\n")) {
                        os.println("\t\t" + s);
                    }
                } else {
                    os.println("\t\tNULL");
                }

                os.println("\t" + Bytes.toString(archiveHelper.getRefColumnName()));
                value = result.getValue(GenomeHelper.COLUMN_FAMILY_BYTES, archiveHelper.getRefColumnName());
                if (value != null) {
                    VcfSliceProtos.VcfSlice vcfSlice = VcfSliceProtos.VcfSlice.parseFrom(value);
                    for (String s : vcfSlice.toString().split("\n")) {
                        os.println("\t\t" + s);
                    }
                } else {
                    os.println("\t\tNULL");
                }
            }
        });
    }

    public static void printMetaTable(VariantHadoopDBAdaptor dbAdaptor, Path outDir) throws IOException {
//        HBaseFileMetadataDBAdaptor fileDBAdaptor = dbAdaptor.getVariantFileMetadataDBAdaptor();
        String metaTableName = dbAdaptor.getTableNameGenerator().getMetaTableName();
        if (!dbAdaptor.getHBaseManager().tableExists(metaTableName)) {
            return;
        }
        Path fileName = outDir.resolve("meta." + metaTableName + '.' + TimeUtils.getTimeMillis() + ".txt");
        try (
                FileOutputStream fos = new FileOutputStream(fileName.toFile()); PrintStream out = new PrintStream(fos)
        ) {
            dbAdaptor.getHBaseManager().act(metaTableName, (table) -> {
                ResultScanner scanner = table.getScanner(new Scan());
                for (Result result : scanner) {
                    out.println(Bytes.toString(result.getRow()));
                    for (Cell cell : result.rawCells()) {
                        String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                        byte[] value = CellUtil.cloneValue(cell);
                        for (byte c : value) {
                            if (!CharUtils.isAsciiPrintable((char) c)) {
                                try {
                                    value = CompressionUtils.decompress(value);
                                    break;
                                } catch (DataFormatException ignore) { }
                            }
                        }
                        if (column.startsWith("COUNTER_")) {
                            out.println("\t" + column + "\t" + Bytes.toLong(value));
                        } else {
                            out.println("\t" + column + "\t" + Bytes.toString(value));
                        }
                    }
                }
            });
        }
    }

    public static void printTables(Configuration conf) throws IOException {
        System.out.println("Print tables!");
        System.out.println("conf.get(HConstants.ZOOKEEPER_QUORUM) = " + conf.get(HConstants.ZOOKEEPER_QUORUM));
        try (Connection con = ConnectionFactory.createConnection(conf)) {
            HBaseManager.act(con, "all", (table, admin) -> {
                for (NamespaceDescriptor ns : admin.listNamespaceDescriptors()) {
                    System.out.println(ns.getName());
                    for (TableName tableName : admin.listTableNamesByNamespace(ns.getName())) {
                        System.out.println("      " + tableName);
                    }
                    System.out.println("---");
                }
                return null;
            });
        }
    }

    public static void printVariants(VariantHadoopDBAdaptor dbAdaptor, URI outDir) throws Exception {
        VariantStorageMetadataManager scm = dbAdaptor.getMetadataManager();
        List<StudyMetadata> studies = scm.getStudyNames()
                .stream()
                .map(scm::getStudyMetadata)
                .collect(Collectors.toList());
        printVariants(studies, dbAdaptor, Paths.get(outDir));
    }

    public static void printVariants(StudyMetadata studyMetadata, VariantHadoopDBAdaptor dbAdaptor, URI outDir) throws Exception {
        printVariants(studyMetadata, dbAdaptor, Paths.get(outDir));
    }

    public static void printVariants(StudyMetadata studyMetadata, VariantHadoopDBAdaptor dbAdaptor, Path outDir)
            throws Exception {
        printVariants(Collections.singleton(studyMetadata), dbAdaptor, outDir);
    }

    public static void printVariants(Collection<StudyMetadata> studies, VariantHadoopDBAdaptor dbAdaptor, Path outDir)
            throws Exception {
        boolean old = HBaseToVariantConverter.isFailOnWrongVariants();
        HBaseToVariantConverter.setFailOnWrongVariants(false);
        printMetaTable(dbAdaptor, outDir);
        for (StudyMetadata studyMetadata : studies) {
            printVariantsFromArchiveTable(dbAdaptor, studyMetadata, outDir);
            printArchiveTable(studyMetadata, dbAdaptor, outDir);
            if (!dbAdaptor.getMetadataManager().getIndexedFiles(studyMetadata.getId()).isEmpty()) {
                printVcf(studyMetadata, dbAdaptor, outDir);
            }
        }
        printSampleIndexTable(dbAdaptor, outDir);
        printVariantsFromVariantsTable(dbAdaptor, outDir);
        printVariantsFromDBAdaptor(dbAdaptor, outDir);
        HBaseToVariantConverter.setFailOnWrongVariants(old);
    }

    private static void printVcf(StudyMetadata studyMetadata, VariantHadoopDBAdaptor dbAdaptor, Path outDir) throws IOException {
        try (OutputStream os = new FileOutputStream(outDir.resolve("variant." + studyMetadata.getName() + ".vcf").toFile())) {
            Query query = new Query(VariantQueryParam.STUDY.key(), studyMetadata.getName())
                    .append(VariantQueryParam.INCLUDE_SAMPLE.key(), ParamConstants.ALL)
                    .append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), ".");
            QueryOptions queryOptions = new QueryOptions();
            DataWriter<Variant> writer = new VariantWriterFactory(dbAdaptor).newDataWriter(VariantWriterFactory.VariantOutputFormat.VCF, os, query, queryOptions);
            writer.open();
            writer.pre();
            writer.write(dbAdaptor.get(query, queryOptions).getResults());
            writer.post();
            writer.close();
        }
    }

    public static void printSampleIndexTable(VariantHadoopDBAdaptor dbAdaptor, Path outDir) throws IOException {
        for (Integer studyId : dbAdaptor.getMetadataManager().getStudies(null).values()) {
            StudyMetadata studyMetadata = dbAdaptor.getMetadataManager().getStudyMetadata(studyId);
            List<StudyMetadata.SampleIndexConfigurationVersioned> confs = studyMetadata.getSampleIndexConfigurations();
            if (confs == null || confs.isEmpty()) {
                confs = Collections.singletonList(studyMetadata.getSampleIndexConfigurationLatest());
            }
            for (StudyMetadata.SampleIndexConfigurationVersioned c : confs) {
                int version = c.getVersion();
                String sampleGtTableName = dbAdaptor.getTableNameGenerator().getSampleIndexTableName(studyId, version);
                if (dbAdaptor.getHBaseManager().tableExists(sampleGtTableName)) {
                    printSampleIndexTable(dbAdaptor, outDir, studyId, sampleGtTableName);
                    printSampleIndexTable2(dbAdaptor, outDir, studyId, sampleGtTableName);
                }
            }
        }
    }

    public static void printSampleIndexTable2(VariantHadoopDBAdaptor dbAdaptor, Path outDir, int studyId, String sampleGtTableName) throws IOException {
        if (!dbAdaptor.getHBaseManager().tableExists(sampleGtTableName)) {
            // Skip table
            return;
        }
        Path fileName = outDir.resolve("sample." + sampleGtTableName + ".detailed.txt");
        try (
                FileOutputStream fos = new FileOutputStream(fileName.toFile()); PrintStream out = new PrintStream(fos)
        ) {
            SampleIndexDBAdaptor sampleIndexDBAdaptor = new SampleIndexDBAdaptor(dbAdaptor.getHBaseManager(), dbAdaptor.getTableNameGenerator(), dbAdaptor.getMetadataManager());
            SampleIndexSchema schema = sampleIndexDBAdaptor.getSchemaLatest(studyId);
            for (Integer sampleId : dbAdaptor.getMetadataManager().getIndexedSamples(studyId)) {
                String sampleName = dbAdaptor.getMetadataManager().getSampleName(studyId, sampleId);
                RawSingleSampleIndexVariantDBIterator it = sampleIndexDBAdaptor.rawIterator(dbAdaptor.getMetadataManager().getStudyName(studyId), sampleName);

                out.println("");
                out.println("");
                out.println("");
                out.println("SAMPLE: " + sampleName + " , " + sampleId);
                while (it.hasNext()) {
                    SampleVariantIndexEntry entry = it.next();
                    out.println("_______________________");
                    out.println(entry.toString(schema));
                }
            }
        }
    }

    public static void printSampleIndexTable(VariantHadoopDBAdaptor dbAdaptor, Path outDir, int studyId, String sampleGtTableName)
            throws IOException {
        if (!dbAdaptor.getHBaseManager().tableExists(sampleGtTableName)) {
            // Skip table
            return;
        }
        Path fileName = outDir.resolve("sample." + sampleGtTableName + ".txt");
        try (
                FileOutputStream fos = new FileOutputStream(fileName.toFile()); PrintStream out = new PrintStream(fos)
        ) {
            SampleIndexSchema schema = new SampleIndexSchemaFactory(dbAdaptor.getMetadataManager()).getSchemaLatest(studyId);
            dbAdaptor.getHBaseManager().act(sampleGtTableName, table -> {

                table.getScanner(new Scan()).iterator().forEachRemaining(result -> {

//                        Map<String, List<Variant>> map = converter.convertToMap(result);
                    Map<String, String> map = new TreeMap<>();
                    String chromosome = SampleIndexSchema.chromosomeFromRowKey(result.getRow());
                    int batchStart = SampleIndexSchema.batchStartFromRowKey(result.getRow());
                    for (Cell cell : result.rawCells()) {
                        String s = Bytes.toString(CellUtil.cloneQualifier(cell));
                        byte[] value = CellUtil.cloneValue(cell);
                        if (s.startsWith("_C_") || s.startsWith("_DC")) {
                            map.put(s, String.valueOf(Bytes.toInt(value)));
                        } else if (s.startsWith("_AC_")) {
                            map.put(s, IntStream.of(IndexUtils.countPerBitToObject(value)).mapToObj(String::valueOf).collect(Collectors.toList()).toString());
                        } else if (s.startsWith("_")) {
                            StringBuilder sb = new StringBuilder();
                            for (byte b : value) {
                                sb.append(IndexUtils.byteToString(b));
                                sb.append(" - ");
                            }
                            map.put(s, sb.toString());
                        } else if (s.startsWith(Bytes.toString(SampleIndexSchema.toMendelianErrorColumn()))) {
                            map.put(s, MendelianErrorSampleIndexConverter.toVariants(value, 0, value.length).toString());
                        } else {
                            map.put(s, new SampleIndexVariantBiConverter(schema).toVariants(chromosome, batchStart, value, 0, value.length).toString());
                        }
                    }

                    out.println("_______________________");
                    out.println(SampleIndexSchema.rowKeyToString(result.getRow()));
                    for (Map.Entry<String, ?> entry : map.entrySet()) {
                        out.println("\t" + entry.getKey() + " = " + entry.getValue());
                    }

                });

            });
        }
    }

    public static void removeFile(HadoopVariantStorageEngine variantStorageManager, String file,
                                  StudyMetadata studyMetadata, Map<? extends String, ?> otherParams, URI outputUri) throws Exception {
        ObjectMap params = new ObjectMap()
                .append(VariantStorageOptions.STUDY.key(), studyMetadata.getName());
        if (otherParams != null) {
            params.putAll(otherParams);
        }

        variantStorageManager.getOptions().putAll(params);
        variantStorageManager.removeFile(studyMetadata.getName(), file, outputUri);
//        studyMetadata.copy(
//                variantStorageManager
//                        .getDBAdaptor()
//                        .getMetadataManager()
//                        .getStudyMetadata(studyMetadata.getStudyId(), null)
//                        .first());
//        return variantStorageManager.readVariantSource(etlResult.getTransformResult(), new ObjectMap());
    }

    public static VariantFileMetadata loadFile(
            HadoopVariantStorageEngine variantStorageManager, String dbName, URI outputUri, String resourceName,
            StudyMetadata studyMetadata, Map<? extends String, ?> otherParams, boolean doTransform, boolean loadArchive,
            boolean loadVariant) throws Exception {
        URI fileInputUri = VariantStorageBaseTest.getResourceUri(resourceName);

        ObjectMap params = new ObjectMap(VariantStorageOptions.TRANSFORM_FORMAT.key(), "proto")
                .append(VariantStorageOptions.STUDY.key(), studyMetadata.getName())
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.SPECIES.key(), "hsapiens").append(VariantStorageOptions.ASSEMBLY.key(), "GRch37")
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);

        if (otherParams != null) {
            params.putAll(otherParams);
        }
//        if (fileId > 0) {
//            params.append(VariantStorageEngine.Options.FILE_ID.key(), fileId);
//        }
        StoragePipelineResult etlResult = VariantStorageBaseTest.runETL(variantStorageManager, fileInputUri, outputUri, params, doTransform, doTransform, loadArchive || loadVariant);
//        StudyMetadata updatedStudyMetadata = variantStorageManager.getDBAdaptor().getMetadataManager().getStudyMetadata(studyMetadata.getStudyId(), null).first();
//        if (updatedStudyMetadata != null) {
//            studyMetadata.copy(updatedStudyMetadata);
//        }

        return variantStorageManager.getVariantReaderUtils().readVariantFileMetadata(doTransform ? etlResult.getTransformResult() : etlResult.getInput());
    }

    public static VariantFileMetadata loadFile(HadoopVariantStorageEngine variantStorageManager, String dbName, URI outputUri,
                                               String resourceName, StudyMetadata studyMetadata) throws Exception {
        return loadFile(variantStorageManager, dbName, outputUri, resourceName, studyMetadata, null, true, true, true);
    }

    public static VariantFileMetadata loadFile(HadoopVariantStorageEngine variantStorageManager, String dbName, URI outputUri,
                                         String resourceName, StudyMetadata studyMetadata, Map<? extends String, ?> otherParams) throws Exception {
        return loadFile(variantStorageManager, dbName, outputUri, resourceName, studyMetadata, otherParams, true, true, true);
    }
}
