package org.opencb.opencga.storage.variant;

import java.util.List;
import org.opencb.commons.bioformats.variant.Variant;
import org.opencb.commons.bioformats.variant.vcf4.io.writers.VariantWriter;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public abstract class VariantDBWriter implements VariantWriter {
    
    abstract boolean buildBatchRaw(List<Variant> data);
    
    abstract boolean buildStatsRaw(List<Variant> data);
    
    abstract boolean buildEffectRaw(List<Variant> variants);
    
    abstract boolean buildBatchIndex(List<Variant> data);
    
    abstract boolean writeBatch(List<Variant> data);
    
}
