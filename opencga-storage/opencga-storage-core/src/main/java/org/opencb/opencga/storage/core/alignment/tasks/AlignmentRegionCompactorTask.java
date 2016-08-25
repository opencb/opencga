/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.core.alignment.tasks;

import org.opencb.biodata.formats.alignment.AlignmentUtils;
import org.opencb.biodata.formats.sequence.fasta.dbadaptor.CellBaseSequenceDBAdaptor;
import org.opencb.biodata.formats.sequence.fasta.dbadaptor.SequenceDBAdaptor;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.biodata.models.alignment.exceptions.ShortReferenceSequenceException;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.run.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Date: 27/03/14
 * @author Jacobo Coll Moragon <jcoll@ebi.ac.uk>
 */

public class AlignmentRegionCompactorTask extends Task<AlignmentRegion> {

    private final SequenceDBAdaptor adaptor;
    protected static Logger logger = LoggerFactory.getLogger(AlignmentRegionCompactorTask.class);


    public AlignmentRegionCompactorTask() {
        this.adaptor = new CellBaseSequenceDBAdaptor();
    }


    public AlignmentRegionCompactorTask(SequenceDBAdaptor adaptor) {
        this.adaptor = adaptor;
    }

    @Override
    public boolean pre(){
        try {
            adaptor.open();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean post(){
        try {
            adaptor.close();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean apply(List<AlignmentRegion> batch) throws IOException {

        for(AlignmentRegion alignmentRegion : batch){
            Region region = alignmentRegion.getRegion();
            long start = region.getStart();
            if(start <= 0){
                start = 1;
                region.setStart(1);
            }
            logger.info("Asking for sequence: " + region.toString() + " size = " + (region.getEnd()-region.getStart()));
            String sequence = adaptor.getSequence(region);
            for(Alignment alignment : alignmentRegion.getAlignments()){
                try {
                    AlignmentUtils.completeDifferencesFromReference(alignment, sequence, start);
                } catch (ShortReferenceSequenceException e) {
                    logger.warn("NOT ENOUGH REFERENCE SEQUENCE. " + alignment.getChromosome()+":"+alignment.getUnclippedStart() + "-" + alignment.getUnclippedEnd(), e);
                }
            }
        }
        return true;
    }
}
