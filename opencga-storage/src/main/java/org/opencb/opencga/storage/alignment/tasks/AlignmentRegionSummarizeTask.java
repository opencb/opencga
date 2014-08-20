package org.opencb.opencga.storage.alignment.tasks;

import org.opencb.commons.bioformats.alignment.Alignment;
import org.opencb.commons.bioformats.alignment.AlignmentRegion;
import org.opencb.commons.bioformats.alignment.stats.AlignmentRegionSummary;
import org.opencb.commons.run.Task;

import java.io.IOException;
import java.util.*;


/**
 * -Guardarnos todas las tags en un header maestro. Un header inmenso. Los identificadores pasan a ser más grandes.
 * -Guardar los más frecuentes en un solo header.
 * -Jerarquico
 *
 */


/**
 * Created with IntelliJ IDEA.
 * User: jmmut
 * Date: 3/31/14
 * Time: 5:55 PM
 *
 * @brief This task obtains the most repeated values in the alignments belonging to some range
 */
public class AlignmentRegionSummarizeTask extends Task<AlignmentRegion> {
    private int modeStatsSize = defaultModeStatsSize ;
    private static final int defaultModeStatsSize = 100000;


  //  private Map<String, Integer> nameMap;
    private Map<Integer, Integer> flagsMap;
    private Map<Long, Integer> startDiffMap;
    private Map<Integer, Integer> lengthMap;
    private Map<Integer, Integer> mappingQualityMap;
    private Map<Integer, Integer> mateAlignmentStartDiffMap;
    private Map<String, Integer> rNextMap;
    private List<String> keyList;
    private Map<String, Integer> keyMap;
    private Map<Map.Entry<Integer, Object>, Integer> tagsMap;


    private long start;
    private long actualEnd; //Position until which the actual ModeStats are calculated
    private boolean running = false;

    private void restart(long position){
        //nameMap = new HashMap<>();
        flagsMap = new HashMap<>();
        startDiffMap = new HashMap<>();
        lengthMap = new HashMap<>();
        mappingQualityMap = new HashMap<>();
        mateAlignmentStartDiffMap = new HashMap<>();
        rNextMap = new HashMap<>();
        keyMap = new HashMap<>();
        keyList = new ArrayList<>();
        tagsMap = new HashMap<>();

        start = (position / modeStatsSize)*modeStatsSize;
        actualEnd = start + modeStatsSize;
    }

    @Override
    public boolean apply(List<AlignmentRegion> batch) throws IOException {
        String name;
        Map.Entry<String, Integer> nameEntry;
        Integer flags;
        Map.Entry<Integer, Integer> flagsEntry;


        for (AlignmentRegion alignmentRegion : batch) {
            System.out.println("en un alignmentRegion con num alignments = " + alignmentRegion.getAlignments().size());

            if(!running){
                restart(alignmentRegion.getStart());
                running = true;
            }

            long auxl;
            int auxi;
            String auxs;
            for (Alignment alignment : alignmentRegion.getAlignments()) {


                if(alignment.getStart() > actualEnd){

                    int defaultLength = 0;
                    int defaultFlag = 0;
                    int defaultMapQ = 0;
                    String defaultRNext = "";

                    auxi = 0;
                    for (Map.Entry<Integer, Integer> entry: flagsMap.entrySet()) {
                        if(auxi < entry.getValue()){
                            defaultFlag = entry.getKey();
                        }
                    }
                    auxi = 0;
                    for (Map.Entry<Integer, Integer> entry: lengthMap.entrySet()) {
                        if(auxi < entry.getValue()){
                            defaultLength = entry.getKey();
                        }
                    }
                    auxi = 0;
                    for (Map.Entry<Integer, Integer> entry: mappingQualityMap.entrySet()) {
                        if(auxi < entry.getValue()){
                            defaultMapQ = entry.getKey();
                        }
                    }
                    auxi = 0;
                    for (Map.Entry<String, Integer> entry: rNextMap.entrySet()) {
                        if(auxi < entry.getValue()){
                            defaultRNext = entry.getKey();
                        }
                    }


                    AlignmentRegionSummary alignmentRegionSummary = new AlignmentRegionSummary(defaultLength, defaultFlag ,defaultMapQ, defaultRNext, keyMap, tagsMap);
                    restart(alignment.getStart());


                }

//                name = alignment.getName();
//                nameMap.put(name, !nameMap.containsKey(name)? 1: nameMap.get(name)+1);

                flags = alignment.getFlags();
                flagsMap.put(flags, !flagsMap.containsKey(flags)? 1: flagsMap.get(flags)+1);

                auxl = alignment.getStart()-start;
                startDiffMap.put(auxl, !startDiffMap.containsKey(auxl)? 1: startDiffMap.get(auxl)+1);
                start = alignment.getStart();

                auxi = alignment.getLength();
                lengthMap.put(auxi, !lengthMap.containsKey(auxi)? 1: lengthMap.get(auxi)+1);

                auxi = alignment.getMappingQuality();
                mappingQualityMap.put(auxi, !mappingQualityMap.containsKey(auxi)? 1: mappingQualityMap.get(auxi)+1);

                auxi = (int)(alignment.getStart()-alignment.getMateAlignmentStart());
                mateAlignmentStartDiffMap.put(auxi, !mateAlignmentStartDiffMap.containsKey(auxi)? 1: mateAlignmentStartDiffMap.get(auxi)+1);

                rNextMap.put(alignment.getMateReferenceName(), !rNextMap.containsKey(alignment.getMateReferenceName())? 1: rNextMap.get(alignment.getMateReferenceName())+1);


                for(Map.Entry<String, Object> entry : alignment.getAttributes().entrySet()){
                    auxs = entry.getKey();
                    if(keyMap.containsKey(auxs)){
                        auxi = keyMap.get(auxs);
                    } else {
                        auxi = keyList.size();
                        keyList.add(auxs);
                        keyMap.put(auxs, auxi);
                    } //auxi = IndexTag;

                    Map.Entry<Integer, Object> tag = new AbstractMap.SimpleEntry<>(auxi, entry.getValue());
                    if(tagsMap.containsKey(tag)){
                        auxi = tagsMap.get(tag) + 1;
                    } else {
                        auxi = 1;
                    }
                    tagsMap.put(tag, auxi);
                }
            }

//            nameEntry = new AbstractMap.SimpleEntry<String, Integer>("", 0);
//            for (Map.Entry<String, Integer> entry: nameMap.entrySet()) {
//                if (entry.getValue() > nameEntry.getValue()) {
//                    nameEntry = entry;
//                }
//
//            }
            flagsEntry = new AbstractMap.SimpleEntry<Integer, Integer>(0, 0);
            for (Map.Entry<Integer, Integer> entry: flagsMap.entrySet()) {
                if (entry.getValue() > flagsEntry.getValue()) {
                    flagsEntry = entry;
                }
                System.out.println("clave: " + entry.getKey() + ", valor : " + entry.getValue());
            }


            System.out.println("Start Diff:");
            for (Map.Entry<Long, Integer> entry: startDiffMap.entrySet()) {
                System.out.print("clave: " + entry.getKey() + ", valor : " + entry.getValue() + "\t");
                for(int i = 0; i < entry.getValue(); i++){
                    System.out.print('*');
                }
                System.out.println("");
            }

            System.out.println("Mapping Quality:");
            for (Map.Entry<Integer, Integer> entry: mappingQualityMap.entrySet()) {
                System.out.print("clave: " + entry.getKey() + ", valor : " + entry.getValue() + "\t");
                for(int i = 0; i < entry.getValue(); i++){
                    System.out.print('*');
                }
                System.out.println("");
            }
/*
            System.out.println("MateAlignmentStart Diff:");
            for (Map.Entry<Integer, Integer> entry: mateAlignmentStartDiffMap.entrySet()) {
                System.out.print("clave: " + entry.getKey() + ", valor : " + entry.getValue() + "\t");
                for(int i = 0; i < entry.getValue(); i++){
                    System.out.print('*');
                }
                System.out.println("");
            }

            System.out.println("MateReferenceNext Diff:");
            for (Map.Entry<String, Integer> entry: rNextMap.entrySet()) {
                System.out.print("clave: " + entry.getKey() + ", valor : " + entry.getValue() + "\t");
                for(int i = 0; i < entry.getValue(); i++){
                    System.out.print('*');
                }
                System.out.println("");
            }*/

            System.out.println("Key List:");
            for (int i = 0; i < keyList.size(); i++){
                System.out.println(i + " : " + keyList.get(i));
            }

            System.out.println("Tags Map:");
            for (Map.Entry<Map.Entry<Integer, Object>, Integer> entry: tagsMap.entrySet()) {
                System.out.print("clave: " + entry.getKey() + ", valor : " + entry.getValue() + "\t");
                for(int i = 0; i < entry.getValue(); i++){
                    System.out.print('*');
                }
                System.out.println("");
            }
        }

        return false;
    }

    public int getModeStatsSize() {
        return modeStatsSize;
    }

    public void setModeStatsSize(int modeStatsSize) {
        this.modeStatsSize = modeStatsSize;
    }
}
