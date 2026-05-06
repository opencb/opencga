package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.tools.variant.VariantSorterTask;
import org.opencb.biodata.tools.variant.VariantVcfHtsjdkReader;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.run.Task;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.plain.StringDataReader;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.utils.iterators.CloseableIterator;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.gaps.VcfFileVariantIterator;
import org.opencb.opencga.storage.core.variant.gaps.VariantOverlappingStatus;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.transform.VariantNormalizerFactory;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.FILL_GAPS_GAP_LOCAL_BUFFER_SIZE;

public class FillGapsFromFile {

    public static final String OUTPUT_PROTO_GZ = "mutations.proto.gz";
    private final HBaseManager hBaseManager;
    private final VariantStorageMetadataManager metadataManager;
    private final ObjectMap options;
    private final Logger logger = LoggerFactory.getLogger(FillGapsFromFile.class);
    private int maxBufferSize = FILL_GAPS_GAP_LOCAL_BUFFER_SIZE.defaultValue();

    public FillGapsFromFile(HBaseManager hBaseManager, VariantStorageMetadataManager metadataManager, ObjectMap options) {
        this.hBaseManager = hBaseManager;
        this.metadataManager = metadataManager;
        this.options = options;
    }

    public void fillGaps(String study, List<URI> inputFiles, URI outdir, String variantTableName, String gapsGenotype)
            throws StorageEngineException {
        try {
            Path output = fillGaps(study, inputFiles, outdir, gapsGenotype);

            writeGaps(variantTableName, output);

            Files.delete(output);
        } catch (IOException e) {
            throw new StorageEngineException("Error computing aggregation family operation", e);
        }
    }

    public Path fillGaps(String study, List<URI> inputFiles, URI outdir, String gapsGenotype) throws StorageEngineException, IOException {
        StudyMetadata studyMetadata = metadataManager.getStudyMetadata(study);
        Path output = Paths.get(outdir.resolve(OUTPUT_PROTO_GZ));
        if (Files.exists(output)) {
            throw new IOException("Output file already exists! " + output);
        }
        StopWatch stopWatch = StopWatch.createStarted();

        HBaseFillGapsTask fillGapsTask = new HBaseFillGapsTask(metadataManager, studyMetadata, false, false, gapsGenotype);

        long filesLengthBytes = 0;
        List<VcfFileVariantIterator> fileIterators = new ArrayList<>();
        for (URI inputFile : inputFiles) {
            File file = Paths.get(inputFile).toFile();
            if (!file.exists()) {
                throw new FileNotFoundException("File not found: " + file);
            }
            long fileLength = file.length();
            filesLengthBytes += fileLength;
            String fileName = file.getName();
            int fileId = metadataManager.getFileId(studyMetadata.getId(), fileName);
            FileMetadata fileMetadata = metadataManager.getFileMetadata(studyMetadata.getId(), fileId);
            LinkedHashSet<Integer> sampleIds = fileMetadata.getSamples();
            VariantFileMetadata variantFileMetadata = metadataManager.getVariantFileMetadata(studyMetadata.getId(), fileId);

            InputStream inputStream = Files.newInputStream(file.toPath());
            StringDataReader.SizeInputStream sizeInputStream = new StringDataReader.SizeInputStream(inputStream, fileLength);
            inputStream = sizeInputStream;
            if (fileName.endsWith(".gz")) {
                inputStream = new GZIPInputStream(inputStream);
            }

            DataReader<Variant> reader = new VariantVcfHtsjdkReader(inputStream,
                    new VariantFileMetadata(Integer.toString(fileId), Integer.toString(fileId))
                            .toVariantStudyMetadata(studyMetadata.getName()));
            ObjectMap thisOptions = new ObjectMap(this.options);
            if (VariantReaderUtils.isGvcf(fileName)
                    || fileMetadata.getAttributes().getBoolean(VariantStorageOptions.GVCF.key(), false)) {
                logger.info("GVCF file detected: " + fileName);
                thisOptions.put(VariantStorageOptions.GVCF.key(), true);
            }
            Task<Variant, Variant> normalizer = new VariantNormalizerFactory(thisOptions).newNormalizer(variantFileMetadata);
            reader = reader.then(normalizer);
            VariantSorterTask sorter = new VariantSorterTask(100, SampleIndexSchema.VARIANT_COMPARATOR);
            reader = reader.then(sorter);
            fileIterators.add(new VcfFileVariantIterator(file, fileName, fileId, sampleIds, reader.iterator(),
                    sizeInputStream, maxBufferSize));
        }

        int numVariants = 0;
        int numPuts = 0;
        ProgressLogger progressLogger = new ProgressLogger("Processed", filesLengthBytes, 100);
        try (OutputStream os = new GZIPOutputStream(Files.newOutputStream(output))) {
            String chromosome = getNextChromosome(fileIterators);
            while (chromosome != null) {
                Result result = fillGapsChromosome(chromosome, fileIterators, fillGapsTask, progressLogger, os);
                numVariants += result.getNumVariants();
                numPuts += result.getNumPuts();
                chromosome = getNextChromosome(fileIterators);
            }
        }

        logger.info("Fill gaps finished! " + numVariants + " variants processed, " + numPuts + " mutations written to " + output);
        logger.info("Time taken: " + TimeUtils.durationToString(stopWatch));
        return output;
    }

    public FillGapsFromFile setMaxBufferSize(int maxBufferSize) {
        this.maxBufferSize = maxBufferSize;
        return this;
    }

    public static CloseableIterator<Put> putProtoIterator(Path protoFile) throws IOException {
        GZIPInputStream inputStream = new GZIPInputStream(Files.newInputStream(protoFile));
        return new PutCloseableIterator(inputStream);
    }

    public void writeGaps(String variantTableName, Path output) throws StorageEngineException {
        try (BufferedMutator bufferedMutator = hBaseManager.getConnection()
                .getBufferedMutator(new BufferedMutatorParams(TableName.valueOf(variantTableName)))) {
            try (CloseableIterator<Put> iterator = putProtoIterator(output)) {
                while (iterator.hasNext()) {
                    bufferedMutator.mutate(iterator.next());
                }
            }
            bufferedMutator.flush();
        } catch (Exception e) {
            throw new StorageEngineException("Error computing aggregation family operation", e);
        }
    }

    private static final class PutCloseableIterator extends CloseableIterator<Put> {
        private final GZIPInputStream inputStream;
        private Put next;

        private PutCloseableIterator(GZIPInputStream inputStream) {
            super(inputStream);
            this.inputStream = inputStream;
            next = null;
        }

        @Override
        public boolean hasNext() {
            if (next == null) {
                // Read next
                try {
                    ClientProtos.MutationProto proto = ClientProtos.MutationProto.parseDelimitedFrom(inputStream);
                    if (proto == null) {
                        // EOF
                        return false;
                    } else {
                        next = ProtobufUtil.toPut(proto);
                        return true;
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            } else {
                return true;
            }
        }

        @Override
        public Put next() {
            if (hasNext()) {
                Put ret = next;
                next = null;
                return ret;
            } else {
                throw new NoSuchElementException();
            }
        }
    }

    private static String getNextChromosome(List<VcfFileVariantIterator> fileIterators) {
        for (VcfFileVariantIterator fileIterator : fileIterators) {
            String chromosome = fileIterator.getNextChromosome();
            if (chromosome != null) {
                return chromosome;
            }
        }
        return null;
    }

    public static final class Result {
        private final int numVariants;
        private final int numPuts;

        public Result(int numVariants, int numPuts) {
            this.numVariants = numVariants;
            this.numPuts = numPuts;
        }

        public int getNumVariants() {
            return numVariants;
        }

        public int getNumPuts() {
            return numPuts;
        }
    }

    private Result fillGapsChromosome(String chromosome, List<VcfFileVariantIterator> fileIterators,
                                      HBaseFillGapsTask fillGapsTask, ProgressLogger progressLogger, OutputStream os)
            throws IOException {
        int numVariants = 0;
        int numPuts = 0;
        for (VcfFileVariantIterator fileIterator : fileIterators) {
            fileIterator.setChromosome(chromosome);
        }
        Variant variant = getNextVariant(fileIterators, null);
        EnumMap<VariantOverlappingStatus, Integer> overlappingStatusCount = new EnumMap<>(VariantOverlappingStatus.class);
        while (variant != null) {
            Integer start = variant.getStart();
            numVariants++;
            Put put = new Put(VariantPhoenixKeyFactory.generateVariantRowKey(variant));
            for (VcfFileVariantIterator fileIterator : fileIterators) {
                VariantOverlappingStatus overlappingStatus = fillGapsTask.fillGaps(
                        variant, fileIterator.getSampleIds(), put, fileIterator.getFileId(), fileIterator);
                overlappingStatusCount.merge(overlappingStatus, 1, Integer::sum);
                // The fillGapsTask may have consumed some variants from the iterator
                // Trim the buffer to remove the consumed variants
                fileIterator.trim();
                progressLogger.increment(fileIterator.getReadBytes(), () -> " Bytes up to position " + chromosome + ":" + start);
            }

            if (!put.isEmpty()) {
                ClientProtos.MutationProto proto = ProtobufUtil.toMutation(ClientProtos.MutationProto.MutationType.PUT, put);
                proto.writeDelimitedTo(os);
                numPuts++;
            }

            variant = getNextVariant(fileIterators, variant);
        }
        logger.info("chr " + chromosome + " , numVariants = " + numVariants + " , numPuts = " + numPuts + " " + overlappingStatusCount);
        return new Result(numVariants, numPuts);
    }

    private Variant getNextVariant(List<VcfFileVariantIterator> files, Variant prevVariant) {
        Variant nextVariant = null;
        for (VcfFileVariantIterator file : files) {
            Variant variant = file.getNextVariant(prevVariant);
            if (variant != null) {
                if (nextVariant == null) {
                    nextVariant = variant;
                } else if (SampleIndexSchema.INTRA_CHROMOSOME_VARIANT_COMPARATOR.compare(variant, nextVariant) < 0) {
                    // If the new variant is before the current next variant, update the next variant
                    nextVariant = variant;
                }
            }
        }
        return nextVariant;
    }
}
