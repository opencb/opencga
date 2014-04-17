package org.opencb.opencga.storage.variant.json;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Set;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public abstract class ArchivedVariantFileJsonMixin {
    
    @JsonIgnore public abstract Set<String> getSampleNames();
            
}
