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
import org.opencb.biodata.models.variant.avro.VariantType;
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
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.transform.VariantNormalizerFactory;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;
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

        FillGapsTask fillGapsTask = new FillGapsTask(metadataManager, studyMetadata, false, false, gapsGenotype);

        long filesLengthBytes = 0;
        List<VariantIterator> fileIterators = new ArrayList<>();
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
            fileIterators.add(new VariantIterator(file, fileName, fileId, sampleIds, reader.iterator(), sizeInputStream, maxBufferSize));
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


    private static class VariantIterator implements ListIterator<Variant> {

        private final File file;
        private final String fileName;
        private final int fileId;
        private final LinkedHashSet<Integer> sampleIds;
        private final Iterator<Variant> variantIterator;
        private final StringDataReader.SizeInputStream sizeInputStream;
        private long lastAvailable;
        private String chromosome;

        // Variants buffer from the current chromosome
        private RandomAccessDequeue<Variant> buffer;
        private ListIterator<Variant> bufferIterator;

        private final Set<String> prevChromosomes = new HashSet<>();
        // Buffer for other chromosomes
        private final Map<String, RandomAccessDequeue<Variant>> bufferByChr = new LinkedHashMap<>();
        private final int maxBufferSize;


        VariantIterator(File file, String fileName, int fileId, LinkedHashSet<Integer> sampleIds,
                        Iterator<Variant> variantIterator, StringDataReader.SizeInputStream sizeInputStream, int maxBufferSize) {
            this.file = file;
            this.fileName = fileName;
            this.fileId = fileId;
            this.sampleIds = sampleIds;
            this.variantIterator = variantIterator;
            this.sizeInputStream = sizeInputStream;
            this.maxBufferSize = maxBufferSize;
            buffer = newBuffer();
            bufferIterator = buffer.listIterator();
            this.lastAvailable = sizeInputStream.availableLong();
        }

        private RandomAccessDequeue<Variant> newBuffer() {
            return new RandomAccessDequeue<>(this.maxBufferSize * 2);
        }

        public long getReadBytes() {
            long newAvailable = sizeInputStream.availableLong();
            long readBytes = lastAvailable - newAvailable;
            lastAvailable = newAvailable;
            return readBytes;
        }

        @Override
        public boolean hasNext() {
            if (bufferIterator.hasNext()) {
                return true;
            } else {
                return addToBuffer() != null;
            }
        }

        @Override
        public Variant next() {
            if (!bufferIterator.hasNext()) {
                if (addToBuffer() == null) {
                    throw new NoSuchElementException();
                }
            }
            return bufferIterator.next();
        }

        private int getBufferSize() {
            return buffer.size() + bufferByChr.values().stream().mapToInt(RandomAccessDequeue::size).sum();
        }

        @Override
        public boolean hasPrevious() {
            return bufferIterator.hasPrevious();
        }

        @Override
        public Variant previous() {
            return bufferIterator.previous();
        }

        @Override
        public int nextIndex() {
            return bufferIterator.nextIndex();
        }

        @Override
        public int previousIndex() {
            return bufferIterator.previousIndex();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void set(Variant variant) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void add(Variant variant) {
            throw new UnsupportedOperationException();
        }

        private Variant addToBuffer() {
            if (variantIterator.hasNext()) {
                if (buffer.isEmpty() && getBufferSize() > maxBufferSize) {
                    // This chromosome is empty, and we're already reading from another chromosome
                    // Do not overflow the buffer
                    return null;
                }
                Variant variant = variantIterator.next();
                if (chromosome == null) {
                    chromosome = variant.getChromosome();
                }
                if (variant.getChromosome().equals(chromosome)) {
                    buffer.add(variant);
                    return variant;
                } else {
                    bufferByChr.computeIfAbsent(variant.getChromosome(), k -> newBuffer()).add(variant);
                    if (prevChromosomes.contains(variant.getChromosome())) {
                        throw new IllegalStateException("Chromosome \"" + variant.getChromosome() + "\" already processed!"
                                + " Unordered chromosomes in file \"" + file + "\"");
                    }
                    return null;
                }
            }
            return null;
        }

        public Variant getNextVariant(Variant prevVariant) {
            // Look for an actual variant in the buffer
            int i = nextIndex();
            Variant variant = null;
            while (i < buffer.size() && !isNextVariant(prevVariant, variant)) {
                variant = buffer.get(i);
                i++;
            }
            if (!isNextVariant(prevVariant, variant)) {
                // Look for an actual variant in the iterator
                variant = addToBuffer();
                while (variant != null && !isNextVariant(prevVariant, variant)) {
                    variant = addToBuffer();
                }
            }
            return variant;
        }

        protected static boolean isNextVariant(Variant prevVariant, Variant variant) {
            if (variant == null) {
                return false;
            } else if (isVariant(variant)) {
                if (prevVariant == null) {
                    return true;
                } else {
                    if (variant.getChromosome().equals(prevVariant.getChromosome())) {
                        int compare = SampleIndexSchema.INTRA_CHROMOSOME_VARIANT_COMPARATOR.compare(prevVariant, variant);
                        return compare < 0;
//                        if (variant.getStart() < nextVariant.getStart()) {
//                            nextVariant = variant;
//                        }
                    } else {
                        return true;
                    }
                }
            } else {
                return false;
            }
        }

        private static boolean isVariant(Variant variant) {
            return variant.getType() != VariantType.NO_VARIATION;
        }

        public void trim() {
            // Remove all variants before the current variant
            while (bufferIterator.nextIndex() > 0) {
                buffer.removeHead();
            }
        }

        public void setChromosome(String newChromosome) {
            if (this.chromosome != null && !this.chromosome.equals(newChromosome)) {
                // When changing chromosome, change the buffer
                prevChromosomes.add(this.chromosome);
                RandomAccessDequeue<Variant> chrBuffer = bufferByChr.remove(newChromosome);
                buffer = chrBuffer == null ? newBuffer() : chrBuffer;
                bufferIterator = buffer.listIterator();
            }
            this.chromosome = newChromosome;
        }

        public String getNextChromosome() {
            if (chromosome == null) {
                Variant variant = getNextVariant(null);
                if (variant == null) {
                    return null;
                } else {
                    return variant.getChromosome(); // First chromosome
                }
            } else if (bufferByChr.isEmpty()) {
                return null;
            } else {
                return bufferByChr.keySet().iterator().next();
            }
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

    private static String getNextChromosome(List<VariantIterator> fileIterators) {
        for (VariantIterator fileIterator : fileIterators) {
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

    private Result fillGapsChromosome(String chromosome, List<VariantIterator> fileIterators, FillGapsTask fillGapsTask,
                                      ProgressLogger progressLogger, OutputStream os)
            throws IOException {
        int numVariants = 0;
        int numPuts = 0;
        for (VariantIterator fileIterator : fileIterators) {
            fileIterator.setChromosome(chromosome);
        }
        Variant variant = getNextVariant(fileIterators, null);
        EnumMap<VariantOverlappingStatus, Integer> overlappingStatusCount = new EnumMap<>(VariantOverlappingStatus.class);
        while (variant != null) {
            Integer start = variant.getStart();
            numVariants++;
            Put put = new Put(VariantPhoenixKeyFactory.generateVariantRowKey(variant));
            for (VariantIterator fileIterator : fileIterators) {
//                int prevNextIndex = fileIterator.nextIndex();
                VariantOverlappingStatus overlappingStatus = fillGapsTask.fillGaps(
                        variant, fileIterator.sampleIds, put, fileIterator.fileId, fileIterator);
                overlappingStatusCount.merge(overlappingStatus, 1, Integer::sum);
//                System.out.println(fileIterator.fileName + "(" + prevNextIndex + " -> " + fileIterator.nextIndex() + ") : "
//                        + overlappingStatus);
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

    private Variant getNextVariant(List<VariantIterator> files, Variant prevVariant) {
        Variant nextVariant = null;
        for (VariantIterator file : files) {
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
