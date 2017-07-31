/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.core.tools.accession;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.ArrayUtils;
import org.opencb.biodata.formats.variant.vcf4.VcfRecord;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.formats.variant.vcf4.VariantAggregatedVcfFactory;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.formats.variant.vcf4.VariantVcfFactory;
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

    private VariantSource source;
    private String globalPrefix;
    private String studyPrefix;

    /**
     * Last accessions used, in case they would need to be reused. They are 
     * grouped by chromosome, position and alternate allele.
     */
    private LRUCache<String, Map<String, String>> currentAccessions;
    private String lastAccession;

    private CombinationIterator<Character> iterator;
    private VariantVcfFactory variantFactory;

    public CreateAccessionTask(VariantSource source, String globalPrefix, String studyPrefix) {
        this(source, globalPrefix, studyPrefix, 0);
    }

    public CreateAccessionTask(VariantSource source, String globalPrefix, String studyPrefix, int priority) {
        this(source, globalPrefix, studyPrefix, null, priority);
    }

    public CreateAccessionTask(VariantSource source, String globalPrefix, String studyPrefix, String lastAccession) {
        this(source, globalPrefix, studyPrefix, lastAccession, 0);
    }
    
    public CreateAccessionTask(VariantSource source, String globalPrefix, String studyPrefix, String lastAccession, int priority) {
        super(priority);
        this.source = source;
        this.globalPrefix = globalPrefix != null ? globalPrefix : "";
        this.studyPrefix = studyPrefix;
        this.lastAccession = lastAccession;
        this.currentAccessions = new LRUCache<>(10);
        if (lastAccession != null && lastAccession.length() == 7) {
            this.iterator = new CombinationIterator(7, validCharacters, ArrayUtils.toObject(this.lastAccession.toCharArray()));
        } else {
            this.iterator = new CombinationIterator(7, validCharacters);
        }

        this.variantFactory = new VariantAggregatedVcfFactory(); // Do not even try to parse the samples, it's useless
    }

    @Override
    public boolean apply(List<VcfRecord> batch) throws IOException {
        for (VcfRecord record : batch) {
            List<Variant> variants = variantFactory.create(source, record.toString());
            StringBuilder allAccessionsInRecord = new StringBuilder();
            for (Variant v : variants) {
                Map<String, String> variantAccession = currentAccessions.get(getKey(v));
                if (variantAccession != null) {
                    String accessionGroup = variantAccession.get(getValue(v));
                    if (accessionGroup != null) {
                        allAccessionsInRecord = appendAccession(allAccessionsInRecord, accessionGroup);
                    } else {
                        resetAccessions(v);
                        allAccessionsInRecord = appendAccession(allAccessionsInRecord, lastAccession);
                    }
                } else {
                    resetAccessions(v);
                    allAccessionsInRecord = appendAccession(allAccessionsInRecord, lastAccession);
                }
            }
            
            // Set accession/s for this record (be it in a new genomic position or not)
            record.addInfoField("ACC=" + allAccessionsInRecord.toString());
        }

        return true;
    }

    private String getKey(Variant v) {
        return v.getChromosome() + "_" + v.getStart();
    }
    
    private String getValue(Variant v) {
        return v.getReference() + "_" + v.getAlternate();
    }
    
    private void resetAccessions(Variant v) {
        Character[] next = (Character[]) iterator.next();
        StringBuilder sb = new StringBuilder(next.length);
        for (Character c : next) {
            sb.append(c);
        }
        lastAccession = sb.toString();
        
        Map<String, String> variantAccession = currentAccessions.get(getKey(v));
        if (variantAccession == null) {
            variantAccession = new HashMap<>();
            variantAccession.put(getValue(v), lastAccession);
            currentAccessions.put(getKey(v), variantAccession);
        } else {
            String accessionGroup = variantAccession.get(getValue(v));
            if (accessionGroup == null) {
                variantAccession.put(getValue(v), lastAccession);
            }
        }
    }

    private StringBuilder appendAccession(StringBuilder allAccessionsInRecord, String newAccession) {
        if (allAccessionsInRecord.length() == 0) {
            return allAccessionsInRecord.append(globalPrefix).append(studyPrefix).append(newAccession);
        } else {
            return allAccessionsInRecord.append(",").append(globalPrefix).append(studyPrefix).append(newAccession);
        }
    }
    
}
