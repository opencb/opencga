package org.opencb.opencga.storage.hadoop.variant.gaps;

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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.VariantStoragePipeline;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class FillGapsFromFile {

    private final HBaseManager hBaseManager;
    private final VariantStorageMetadataManager metadataManager;
    private final VariantReaderUtils variantReaderUtils;
    private final ObjectMap options;
    private final Logger logger = LoggerFactory.getLogger(FillGapsFromFile.class);

    public FillGapsFromFile(HBaseManager hBaseManager, VariantStorageMetadataManager metadataManager,
                            VariantReaderUtils variantReaderUtils, ObjectMap options) {
        this.hBaseManager = hBaseManager;
        this.metadataManager = metadataManager;
        this.variantReaderUtils = variantReaderUtils;
        this.options = options;
    }


    private static class VariantIterator implements ListIterator<Variant> {

        private final File file;
        private final String fileName;
        private final int fileId;
        private final LinkedHashSet<Integer> sampleIds;
        private final Iterator<Variant> variantIterator;
        private String chromosome;

        // Variants buffer from the current chromosome
        private RandomAccessDequeue<Variant> buffer = new RandomAccessDequeue<>();
        private ListIterator<Variant> bufferIterator = buffer.listIterator();

        private final Set<String> prevChromosomes = new HashSet<>();
        // Buffer for other chromosomes
        private final Map<String, RandomAccessDequeue<Variant>> bufferByChr = new LinkedHashMap<>();


        VariantIterator(File file, String fileName, int fileId, LinkedHashSet<Integer> sampleIds,
                               Iterator<Variant> variantIterator) {
            this.file = file;
            this.fileName = fileName;
            this.fileId = fileId;
            this.sampleIds = sampleIds;
            this.variantIterator = variantIterator;
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
                Variant variant = variantIterator.next();
                if (chromosome == null) {
                    chromosome = variant.getChromosome();
                }
                if (variant.getChromosome().equals(chromosome)) {
                    buffer.add(variant);
                    return variant;
                } else {
                    bufferByChr.computeIfAbsent(variant.getChromosome(), k -> new RandomAccessDequeue<>()).add(variant);
                    if (prevChromosomes.contains(variant.getChromosome())) {
                        throw new IllegalStateException("Chromosome " + variant.getChromosome() + " already processed!");
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
                buffer = chrBuffer == null ? new RandomAccessDequeue<>() : chrBuffer;
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

    public void fillGaps(String studyId, List<URI> inputFiles, URI outdir, String variantTableName, String gapsGenotype)
            throws IOException, StorageEngineException {
        URI output = fillGaps(studyId, inputFiles, outdir, gapsGenotype);

        Path outputPath = Paths.get(output);

        try (BufferedMutator bufferedMutator = hBaseManager.getConnection()
                .getBufferedMutator(new BufferedMutatorParams(TableName.valueOf(variantTableName)))) {
            try (InputStream is = new GZIPInputStream(Files.newInputStream(outputPath))) {
                ClientProtos.MutationProto proto;
                while ((proto = ClientProtos.MutationProto.parseDelimitedFrom(is)) != null) {
                    Put put = ProtobufUtil.toPut(proto);
//                    Pair<String, Integer> chrpos = VariantPhoenixKeyFactory.extractChrPosFromVariantRowKey(put.getRow());
//                    CellScanner cellScanner = put.cellScanner();
//                    System.out.println(chrpos.getFirst() + ":" + chrpos.getSecond() + " put = " + put.toString(100));
//                    while (cellScanner.advance()) {
//                        Cell current = cellScanner.current();
//                        System.out.println(Bytes.toStringBinary(CellUtil.cloneQualifier(current)) + " = "
//                                + Bytes.toStringBinary(CellUtil.cloneValue(current)));
//                    }
                    bufferedMutator.mutate(put);
                }
            }
            bufferedMutator.flush();
        }
        Files.delete(outputPath);

    }

    public URI fillGaps(String studyId, List<URI> inputFiles, URI outdir, String gapsGenotype) throws StorageEngineException, IOException {
        StudyMetadata studyMetadata = metadataManager.getStudyMetadata(studyId);
        FillGapsTask fillGapsTask = new FillGapsTask(metadataManager, studyMetadata, false, false, gapsGenotype);

        List<VariantIterator> fileIterators = new ArrayList<>();
        int numVariants = 0;
        for (URI inputFile : inputFiles) {
            File file = Paths.get(inputFile).toFile();
            String fileName = file.getName();
            int fileId = metadataManager.getFileId(studyMetadata.getId(), fileName);
            FileMetadata fileMetadata = metadataManager.getFileMetadata(studyMetadata.getId(), fileId);
            LinkedHashSet<Integer> sampleIds = fileMetadata.getSamples();
            VariantFileMetadata variantFileMetadata = metadataManager.getVariantFileMetadata(studyMetadata.getId(), fileId, null).first();
            DataReader<Variant> reader = variantReaderUtils.getVariantVcfReader(inputFile,
                    new VariantFileMetadata(Integer.toString(fileId), Integer.toString(fileId))
                            .toVariantStudyMetadata(studyMetadata.getName()));
            ObjectMap thisOptions = new ObjectMap(this.options);
            if (VariantReaderUtils.isGvcf(fileName)
                    || fileMetadata.getAttributes().getBoolean(VariantStorageOptions.GVCF.key(), false)) {
                logger.info("GVCF file detected: " + fileName);
                thisOptions.put(VariantStorageOptions.GVCF.key(), true);
            }
            Task<Variant, Variant> normalizer = VariantStoragePipeline.initNormalizer(variantFileMetadata, thisOptions);
            reader = reader.then(normalizer);
            VariantSorterTask sorter = new VariantSorterTask(100, SampleIndexSchema.VARIANT_COMPARATOR);
            reader = reader.then(sorter);
            fileIterators.add(new VariantIterator(file, fileName, fileId, sampleIds, reader.iterator()));
        }

        URI output = outdir.resolve("mutations.proto.gz");
        try (OutputStream os = new GZIPOutputStream(Files.newOutputStream(Paths.get(output)))) {
            String chromosome = getNextChromosome(fileIterators);
            while (chromosome != null) {
                numVariants += fillGapsChromosome(chromosome, fileIterators, fillGapsTask, os);
                chromosome = getNextChromosome(fileIterators);
            }
        }

        logger.info("Fill gaps finished! " + numVariants + " variants written to " + output);
        return output;
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

    private int fillGapsChromosome(String chromosome, List<VariantIterator> fileIterators, FillGapsTask fillGapsTask, OutputStream os)
            throws IOException {
        int numVariants = 0;
        for (VariantIterator fileIterator : fileIterators) {
            fileIterator.setChromosome(chromosome);
        }
        Variant variant = getNextVariant(fileIterators, null);
        EnumMap<VariantOverlappingStatus, Integer> overlappingStatusCount = new EnumMap<>(VariantOverlappingStatus.class);
        while (variant != null) {
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
            }

            if (!put.isEmpty()) {
                ClientProtos.MutationProto proto = ProtobufUtil.toMutation(ClientProtos.MutationProto.MutationType.PUT, put);
                proto.writeDelimitedTo(os);
            }
//            if (table != null) {
//                table.put(put);
//            }
            variant = getNextVariant(fileIterators, variant);
        }
        logger.info("chr " + chromosome + " = " + numVariants + " (" + overlappingStatusCount + ")");
        return numVariants;
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
