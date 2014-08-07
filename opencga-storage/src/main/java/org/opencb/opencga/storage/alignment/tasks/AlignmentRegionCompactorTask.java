package org.opencb.opencga.storage.alignment.tasks;

import org.opencb.biodata.formats.alignment.AlignmentHelper;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.biodata.models.alignment.exceptions.ShortReferenceSequenceException;
import org.opencb.biodata.models.feature.Region;
import org.opencb.commons.containers.map.QueryOptions;
import org.opencb.commons.run.Task;

import java.io.IOException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: jcoll
 * Date: 27/03/14
 * Time: 13:00
 */

public class AlignmentRegionCompactorTask extends Task<AlignmentRegion> {

    private final QueryOptions queryOptions;
    
    public AlignmentRegionCompactorTask() {
        this.queryOptions = new QueryOptions();
    }

    public AlignmentRegionCompactorTask(QueryOptions queryOptions) {
        this.queryOptions = queryOptions;
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
            System.out.println("Asking for sequence: " + region.toString() + " size = " + (region.getEnd()-region.getStart()));
            String sequence = AlignmentHelper.getSequence(region, queryOptions);
            for(Alignment alignment : alignmentRegion.getAlignments()){
                try {
                    AlignmentHelper.completeDifferencesFromReference(alignment, sequence, start);
                } catch (ShortReferenceSequenceException e) {
                    System.out.println("[ERROR] NOT ENOUGH REFERENCE SEQUENCE!!");
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }
        return true;
    }
}
