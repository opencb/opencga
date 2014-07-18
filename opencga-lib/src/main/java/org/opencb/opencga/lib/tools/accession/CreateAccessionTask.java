package org.opencb.opencga.lib.tools.accession;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.opencb.biodata.formats.variant.vcf4.VcfRecord;
import org.opencb.biodata.models.variant.Variant;
import static org.opencb.biodata.models.variant.Variant.SV_THRESHOLD;
import org.opencb.commons.run.Task;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class CreateAccessionTask extends Task<VcfRecord> {

    private final Character[] validCharacters = { 
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        'B', 'C', 'D', 'F', 'G', 'H', 'J', 'K', 'L', 'M',
        'N', 'P', 'Q', 'R', 'S', 'T', 'V', 'W', 'X', 'Y', 'Z'
    };
    private final int numVariantTypes = Variant.VariantType.values().length;
    
    private String globalPrefix;
    private String studyPrefix;
    
    private String currentChromosome;
    private int currentPosition;
    private String[] currentAccessions;
    
    private String lastAccession;
    
    private CombinationIterator<Character> iterator;

    public CreateAccessionTask(String globalPrefix, String studyPrefix) {
        this(globalPrefix, studyPrefix, 0);
    }

    public CreateAccessionTask(String globalPrefix, String studyPrefix, int priority) {
        super(priority);
        this.globalPrefix = globalPrefix != null ? globalPrefix : "";
        this.studyPrefix = studyPrefix;
        this.currentAccessions = new String[numVariantTypes];
        this.iterator = new CombinationIterator(7, validCharacters);
    }
    
    @Override
    public boolean apply(List<VcfRecord> batch) throws IOException {
        for (VcfRecord record : batch) {
            if (!record.getChromosome().equals(currentChromosome) || record.getPosition() != currentPosition) {
                // Record in a new genomic position
                resetAccessions(record.getChromosome(), record.getPosition());
            } 
            // Set accession for this record (be it in a new genomic position or not)
            record.addInfoField("ACC=" + globalPrefix + studyPrefix + lastAccession);
        }
        
        return true;
    }
    
    private void resetAccessions(String chromosome, int position) {
        currentChromosome = chromosome;
        currentPosition = position;
        
        Character[] next = (Character[]) iterator.next();
        StringBuilder sb = new StringBuilder(next.length);
        for (Character c : next) {
            sb.append(c);
        }
        lastAccession = sb.toString();
    }
    
    private Variant.VariantType getType(String reference, String alternate) {
        int maxLength = Math.max(reference.length(), alternate.length());
        
        if (reference.length() == alternate.length()) {
            return Variant.VariantType.SNV;
        } else if (maxLength <= SV_THRESHOLD) {
            /*
            * 3 possibilities for being an INDEL:
            * - The value of the ALT field is <DEL> or <INS>
            * - The REF allele is not . but the ALT is
            * - The REF allele is . but the ALT is not
            * - The REF field length is different than the ALT field length
            */
            return Variant.VariantType.INDEL;
        } else {
            return Variant.VariantType.SV;
        }
    }
}
