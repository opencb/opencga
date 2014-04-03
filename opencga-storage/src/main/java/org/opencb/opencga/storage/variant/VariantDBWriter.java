package org.opencb.opencga.storage.variant;

import java.util.List;
import org.opencb.biodata.formats.variant.vcf4.io.VariantWriter;
import org.opencb.biodata.models.variant.Variant;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public abstract class VariantDBWriter implements VariantWriter {
    
    protected abstract boolean buildBatchRaw(List<Variant> data);
    
    protected abstract boolean buildStatsRaw(List<Variant> data);
    
    protected abstract boolean buildEffectRaw(List<Variant> variants);
    
    protected abstract boolean buildBatchIndex(List<Variant> data);
    
    protected abstract boolean writeBatch(List<Variant> data);
    
}
