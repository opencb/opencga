package org.opencb.opencga.storage.alignment;

import org.opencb.commons.bioformats.alignment.Alignment;

import java.util.*;


/**
 * Created with IntelliJ IDEA.
 * User: jcoll
 * Date: 4/7/14
 * Time: 7:36 PM
 * To change this template use File | Settings | File Templates.
 */
public class AlignmentRegionSummary {
    //Only can add an Alignment when it's opened.
    private boolean open;
    private int index;

    private int defaultFlag;
    private int defaultLen;
    private String defaultRNext;
    private int defaultOverlapped;
    private List<String> keysList;
    private Map<String, Integer> keysMap;
    private Map<Map.Entry<Integer, Object>, Integer> tagsMap;

    //Histogram for default values.
    //Null at closed
    private Map<Integer, Integer> flagsMap;
    private Map<Integer, Integer> lenMap;
    private Map<String, Integer> rnextMap;


    public AlignmentRegionSummary(int index){
        this.open = true;
        this.index = index;
        this.flagsMap = new HashMap<>();
        this.lenMap = new HashMap<>();
        this.rnextMap = new HashMap<>();
    }

    public AlignmentRegionSummary(AlignmentProto.Summary summary, int index){
        this.open = false;

        this.index = index;

        this.defaultFlag = summary.getDefaultFlag();
        this.defaultLen = summary.getDefaultLen();
        this.defaultRNext = summary.getDefaultRNext();
        this.defaultOverlapped = summary.getDefaultOverlapped();

        String keys = summary.getKeys();
        this.keysList = new ArrayList<>(keys.length()/2);
        this.keysMap = new HashMap<>();
        for(int i = 0; i < keys.length()/2; i++){
            keysList.set(i, keys.substring(i*2, i*2+2));
            keysMap.put(keysList.get(i), i);
        }

        Map.Entry<Integer, Object> tag;
        Object value = "";
        this.tagsMap = new HashMap<>();
        for (AlignmentProto.Summary.Pair pair : summary.getValuesList()) {
            if(pair.hasAvalue()) {
                value = pair.getAvalue();
            } else if(pair.hasFvalue()) {
                value = pair.getFvalue();
            } else if(pair.hasIvalue()) {
                value = pair.getIvalue();
            } else if(pair.hasZvalue()) {
                value = pair.getZvalue();
            }
            tagsMap.put(new HashMap.SimpleEntry<>(pair.getKey(), value), tagsMap.size());
        }

    }

    /**
     * This function can only be called when summary is OPEN.
     */
    public void addAlignment(Alignment alignment){
        if(!open){
            System.out.println("[ERROR] Alignmentregionsummary.addAlignment() can't be called when is closed!");
            //TODO jj: Throw exception?
            return;
        }
        //FLAG
        {
            Integer f = flagsMap.get(alignment.getFlags());
            f = f==null?1:f+1;
            flagsMap.put(alignment.getFlags(), f);
        }

        //LENGTH
        {
            Integer l = lenMap.get(alignment.getLength());
            l = l==null?1:l+1;
            lenMap.put(alignment.getLength(), l);
        }

        //MateReferenceName
        {
            Integer rn = rnextMap.get(alignment.getLength());
            rn = rn==null?1:rn+1;
            rnextMap.put(alignment.getMateReferenceName(), rn);
        }

        //Tags / Attributes
        int key;
        Map.Entry<Integer, Object> tag;
        for(Map.Entry<String, Object> entry : alignment.getAttributes().entrySet()){
            //Update key map
            if(keysMap.containsKey(entry.getKey())){
                keysMap.put(entry.getKey(), key = keysMap.size());
            } else {
                key = keysMap.get(entry.getKey());
            }

            //Add new map
            tag = new HashMap.SimpleEntry<>(key, entry.getValue());

            if(!tagsMap.containsKey(tag)){
                tagsMap.put(tag, tagsMap.size());
            }
        }

    }

    public void close(){
        int maxFlags = 0;
        for(Map.Entry<Integer, Integer> entry : flagsMap.entrySet()){
            if(entry.getValue() > maxFlags){
                maxFlags = entry.getValue();
                defaultFlag = entry.getKey();
            }
        }

        int maxLen = 0;
        for(Map.Entry<Integer, Integer> entry : lenMap.entrySet()){
            if(entry.getValue() > maxLen){
                maxLen = entry.getValue();
                defaultLen = entry.getKey();
            }
        }

        int maxRNext = 0;
        for(Map.Entry<String, Integer> entry : rnextMap.entrySet()){
            if(entry.getValue() > maxRNext){
                maxRNext = entry.getValue();
                defaultRNext = entry.getKey();
            }
        }


        keysList = new ArrayList<>(keysMap.size());

        for(Map.Entry<String, Integer> entry : keysMap.entrySet()){
            keysList.set(entry.getValue(), entry.getKey());
        }

        open = false;
        flagsMap = null;
        lenMap = null;
        rnextMap = null;
    }

    /**
     * This function can only be called when summary is CLOSED.
     */
    public AlignmentProto.Summary toProto(){

        String keys = "";

        for(String s : keysList){
            keys+=s;
        }

        ArrayList<AlignmentProto.Summary.Pair> pairArrayList = new ArrayList<>(tagsMap.size());
        for(Map.Entry<Map.Entry<Integer, Object>, Integer> entry : tagsMap.entrySet()){
            if(pairArrayList.set(entry.getValue(),
                    AlignmentProto.Summary.Pair.newBuilder()
                            .setKey(entry.getKey().getKey())
                            .setAvalue(entry.getKey().getValue().toString())
                            .build()
            ) != null){
                System.out.println("[ERROR] Duplicated tag index.");
            }
        }

        return AlignmentProto.Summary.newBuilder()
                .setDefaultFlag(defaultFlag)
                .setDefaultLen(defaultLen)
                .setDefaultOverlapped(defaultOverlapped)
                .setDefaultRNext(defaultRNext)
                .setKeys(keys)
                .addAllValues(pairArrayList)
                .build();

    }

    /**
     * This function can only be called when summary is CLOSED.
     *
     * TODO jj: Add better algorithm. For 2.0
     */
    public ArrayList<Integer> getIndexTagList(Map<String, Object> alignmentTags){
        ArrayList<Integer> indexTagList = new ArrayList<>(alignmentTags.size());
        int i = 0;

        int key;
        Map.Entry<Integer, Object> tag;
        for(Map.Entry<String, Object> entry : alignmentTags.entrySet()){
            key = keysMap.get(entry.getKey());
            tag = new HashMap.SimpleEntry<>(key, entry.getValue());
            indexTagList.set(i++, tagsMap.get(tag));
        }

        return indexTagList;
    }

    /**
     * This function can only be called when summary is CLOSED.
     */
    public int getDefaultOverlapped() {
        return defaultOverlapped;
    }

    /**
     * This function can only be called when summary is CLOSED.
     */
    public String getDefaultRNext() {
        return defaultRNext;
    }

    /**
     * This function can only be called when summary is CLOSED.
     */
    public int getDefaultLen() {
        return defaultLen;
    }

    /**
     * This function can only be called when summary is CLOSED.
     */
    public int getDefaultFlag() {
        return defaultFlag;
    }

    /**
     * This function can only be called when summary is CLOSED.
     */
    public int getIndex() {
        return index;
    }

}
