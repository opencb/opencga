package org.opencb.opencga.lib.storage.api.bam;

import net.sf.samtools.BAMRecord;

import java.util.List;
import java.util.Map;

public interface BamReader {

    List<BAMRecord> query(String chromosome, int start, int end, Map<String, Object> options);

    List<BAMRecord> queryByGene(String gene, Map<String, Object> options);

}
