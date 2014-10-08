/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.opencb.opencga.storage.core.alignment.json;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * @author Jacobo Coll Moragon <jcoll@ebi.ac.uk>
 */
public abstract class AlignmentDifferenceJsonMixin {

    @JsonIgnore
    public abstract boolean isSequenceStored();

    @JsonIgnore
    public abstract boolean isAllSequenceStored();
}
