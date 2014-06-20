package org.opencb.opencga.storage.variant.json;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.List;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public abstract class VariantSourceJsonMixin {
    
    @JsonIgnore public abstract List<String> getSamples();
}
