package org.opencb.opencga.storage.core.variant.io;

import java.util.List;
import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.models.variant.Variant;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public abstract class VariantDBWriter implements VariantWriter {
    
    protected abstract boolean buildBatchRaw(List<Variant> data);
    
    protected abstract boolean buildEffectRaw(List<Variant> variants);
    
    protected abstract boolean buildBatchIndex(List<Variant> data);
    
    protected abstract boolean writeBatch(List<Variant> data);
    
}
