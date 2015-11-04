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

package org.opencb.opencga.storage.hadoop.variant;

import java.util.Map;
import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.protobuf.VariantStatsProtos;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.datastore.core.ComplexTypeConverter;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
@Deprecated
public class VariantStatsToHbaseConverter implements ComplexTypeConverter<VariantStats, VariantStatsProtos.VariantStats> {
    
    @Override
    public VariantStats convertToDataModelType(VariantStatsProtos.VariantStats object) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public VariantStatsProtos.VariantStats convertToStorageType(VariantStats object) {
        VariantStatsProtos.VariantStats.Builder builder = VariantStatsProtos.VariantStats.newBuilder();
        
        // Allele and genotype counts
        builder.setRefAlleleCount(object.getRefAlleleCount());
        builder.setAltAlleleCount(object.getAltAlleleCount());
        for (Map.Entry<Genotype, Integer> count : object.getGenotypesCount().entrySet()) {
            VariantStatsProtos.VariantStats.Count.Builder countBuilder = VariantStatsProtos.VariantStats.Count.newBuilder();
            countBuilder.setKey(count.getKey().toString());
            countBuilder.setCount(count.getValue());
            builder.addGenotypesCount(countBuilder.build());
        }

        // Allele and genotype frequencies
        builder.setRefAlleleFreq(object.getRefAlleleFreq());
        builder.setAltAlleleFreq(object.getAltAlleleFreq());
        for (Map.Entry<Genotype, Float> freq : object.getGenotypesFreq().entrySet()) {
            VariantStatsProtos.VariantStats.Frequency.Builder countBuilder = VariantStatsProtos.VariantStats.Frequency.newBuilder();
            countBuilder.setKey(freq.getKey().toString());
            countBuilder.setFrequency(freq.getValue());
            builder.addGenotypesFreq(countBuilder.build());
        }

        // Missing values
        builder.setMissingAlleles(object.getMissingAlleles());
        builder.setMissingGenotypes(object.getMissingGenotypes());
        
        // MAF and MGF
        builder.setMaf(object.getMaf());
        builder.setMgf(object.getMgf());
        builder.setMafAllele(object.getMafAllele());
        builder.setMgfGenotype(object.getMgfGenotype());
        
        // Miscellaneous
        builder.setPassedFilters(object.hasPassedFilters());
        
        builder.setQuality(object.getQuality());
        
        builder.setNumSamples(object.getNumSamples());
        
//        builder.setTransitionsCount(object.getTransitionsCount());
//        builder.setTransversionsCount(object.getTransversionsCount());

        // Optional fields, they require pedigree information
//        if (object.isPedigreeStatsAvailable()) {
        if (false) {
            builder.setMendelianErrors(object.getMendelianErrors());
            
            builder.setCasesPercentDominant(object.getCasesPercentDominant());
            builder.setControlsPercentDominant(object.getControlsPercentDominant());
            builder.setCasesPercentRecessive(object.getCasesPercentRecessive());
            builder.setControlsPercentRecessive(object.getControlsPercentRecessive());
            
//            builder.setHardyWeinberg(effect.getHw().getpValue());
        }
        
        Put p = new Put(builder.build().toByteArray());
        return builder.build();
    }
    
}
