package org.opencb.opencga.storage.core.variant.io.json;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public abstract class GenotypeJsonMixin {
    
    @JsonIgnore public abstract String getGenotype();
    
    @JsonIgnore public abstract int[] getAllelesIdx();
    
}
