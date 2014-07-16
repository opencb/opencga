package org.opencb.opencga.lib.tools.accession;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.opencb.biodata.formats.variant.vcf4.VcfRecord;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.run.Task;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class CreateAccessionTask extends Task<VcfRecord> {

    private final char[] validCharacters = { 
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        'B', 'C', 'D', 'F', 'G', 'H', 'J', 'K', 'L', 'M',
        'N', 'P', 'Q', 'R', 'S', 'T', 'V', 'W', 'X', 'Y', 'Z'
    };
    private final int numVariantTypes = Variant.VariantType.values().length;
    
    private String prefix;
    
    private String currentChromosome;
    private int currentPosition;
    private String[] currentAccessions;
    
    private String lastAccession;

    public CreateAccessionTask(String prefix) {
        this(prefix, 0);
    }

    public CreateAccessionTask(String prefix, int priority) {
        super(priority);
        this.prefix = prefix;
        this.lastAccession = "0000000";
        this.currentAccessions = new String[numVariantTypes];
    }
    
    @Override
    public boolean apply(List<VcfRecord> batch) throws IOException {
        for (VcfRecord record : batch) {
            if (!record.getChromosome().equals(currentChromosome) || record.getPosition() != currentPosition) {
                resetAccessions(record.getChromosome(), record.getPosition());
            }
        }
        
        return true;
    }
    
    private void resetAccessions(String chromosome, int position) {
        currentChromosome = chromosome;
        currentPosition = position;
        Arrays.fill(currentAccessions, null);
    }
    
}
