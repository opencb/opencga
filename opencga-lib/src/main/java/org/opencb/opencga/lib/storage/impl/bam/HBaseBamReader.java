package org.opencb.opencga.lib.storage.impl.bam;

import net.sf.samtools.BAMRecord;
import org.opencb.opencga.lib.data.adaptors.HBaseDBAdaptor;
import org.opencb.opencga.lib.storage.api.bam.BamReader;

import java.util.List;
import java.util.Map;

public class HBaseBamReader implements BamReader {

    private HBaseDBAdaptor dbAdaptor;

    public HBaseBamReader() {
        dbAdaptor = new HBaseDBAdaptor();
    }

    @Override
    public List<BAMRecord> query(String chromosome, int start, int end, Map<String, Object> options) {
        // TODO Auto-generated method stub
//		dbAdaptor....
        return null;
    }

    @Override
    public List<BAMRecord> queryByGene(String gene, Map<String, Object> options) {
        // TODO Auto-generated method stub
        return null;
    }


}
