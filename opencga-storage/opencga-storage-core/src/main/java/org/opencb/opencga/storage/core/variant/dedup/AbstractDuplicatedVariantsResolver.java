package org.opencb.opencga.storage.core.variant.dedup;

import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.tools.variant.VariantDeduplicationTask;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.io.managers.IOConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;

public abstract class AbstractDuplicatedVariantsResolver implements Closeable, VariantDeduplicationTask.DuplicatedVariantsResolver {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final IOConnector ioConnector;
    private final String variantFile;
    private final URI duplicatedVariantsOutputFile;
    private DataOutputStream outputStream;;
    private int duplicatedLocus;
    private int duplicatedVariants;
    private int discardedVariants;

    public AbstractDuplicatedVariantsResolver(String variantFile, IOConnector ioConnector, URI duplicatedVariantsOutputFile) {
        this.ioConnector = ioConnector;
        this.variantFile = variantFile;
        this.duplicatedVariantsOutputFile = duplicatedVariantsOutputFile;
    }

    @Override
    public final List<Variant> resolveDuplicatedVariants(List<Variant> list) {
        duplicatedLocus++;
        duplicatedVariants += list.size();
        logger.warn("Found {} duplicated variants for file {} in variant {}.", list.size(), variantFile, list.get(0));
        if (list.size() <= 1) {
            throw new IllegalStateException("Unexpected list of " + list.size() + " duplicated variants : " + list);
        }

        try {
            ensureOpenOutputStream();

            Variant selectedVariant = internalResolveDuplicatedVariants(list);
            if (selectedVariant == null) {
                discardedVariants += list.size();
            } else {
                discardedVariants += list.size() - 1;
            }

            for (Variant variant : list) {
                FileEntry file = variant.getStudies().get(0).getFile(0);
                write(variant.toString(),
                        file.getData().get(StudyEntry.QUAL),
                        file.getData().get(StudyEntry.FILTER),
                        file.getCall() == null ? variant.toString() : file.getCall().getVariantId(),
                        variant == selectedVariant ? "LOADED" : "DISCARDED");
            }
            return selectedVariant == null ? Collections.emptyList() : Collections.singletonList(selectedVariant);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public int getDuplicatedLocus() {
        return duplicatedLocus;
    }

    public int getDuplicatedVariants() {
        return duplicatedVariants;
    }

    public int getDiscardedVariants() {
        return discardedVariants;
    }

    @Override
    public void close() throws IOException {
        if (outputStream != null) {
            logger.warn("Found {} duplicated variants in {} different locations. {} variants were discarded.",
                    duplicatedVariants, duplicatedLocus, discardedVariants);
            logger.info("Check full list of duplicated variants in file {}", UriUtils.fileName(duplicatedVariantsOutputFile));
            outputStream.close();
        }
    }

    private void ensureOpenOutputStream() throws IOException {
        if (outputStream == null) {
            openOutputStream();
        }
    }

    private synchronized void openOutputStream() throws IOException {
        if (outputStream == null) {
            outputStream = new DataOutputStream(ioConnector.newOutputStream(duplicatedVariantsOutputFile));
            write("#VARIANT", "QUAL", "FILTER", "ORIGINAL_CALL", "STATUS");
        }
    }

    private void write(final String variant, final String qual, final String filter, final String originalCall, final String status)
            throws IOException {
        outputStream.writeBytes(variant + "\t" + qual + "\t" + filter + "\t" + originalCall + "\t" + status + "\n");
        outputStream.flush();
    }

    abstract Variant internalResolveDuplicatedVariants(List<Variant> list);
}
