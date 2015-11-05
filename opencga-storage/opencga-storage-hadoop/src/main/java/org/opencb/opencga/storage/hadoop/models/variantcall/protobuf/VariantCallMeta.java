/**
 * 
 */
package org.opencb.opencga.storage.hadoop.models.variantcall.protobuf;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.opencb.opencga.storage.hadoop.models.variantcall.protobuf.VariantCallProtos.VariantCallMetaProt;
import org.opencb.opencga.storage.hadoop.models.variantcall.protobuf.VariantCallProtos.VariantCallMetaProt.Builder;

import com.google.protobuf.ByteString;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class VariantCallMeta {

    private final Map<String, Integer> name2id = new HashMap<>();
    private final Map<Integer, String> id2name = new HashMap<>();

    private final Map<String, Integer> col2id = new HashMap<>();
    private final Map<Integer, ByteString> id2col = new HashMap<>();
    private Builder builder;

    public VariantCallMeta(){
        builder = VariantCallMetaProt.newBuilder();
    }

    public VariantCallMeta (VariantCallMetaProt prot) {
        builder = VariantCallMetaProt.newBuilder(prot);
        init(prot);
    }

    public VariantCallMetaProt build(){
        return builder.build();
    }

    private void init(VariantCallMetaProt prot){
        int cnt = prot.getSampleNamesCount();
        for(int i = 0; i < cnt; ++i){
            int sampleId = prot.getSampleId(i);
            String sampleName = prot.getSampleNames(i);
            updateName(sampleId, sampleName);
            
            updateCol(sampleId, prot.getSampleColumn(i));
        }
    }
    
    private void updateName(Integer sampleId, String name){
        name2id.put(name, sampleId);
        id2name.put(sampleId, name);
    }
    
    private void updateCol(Integer sampleId, ByteString byteString){
        id2col.put(sampleId, byteString);
        col2id.put(byteString.toStringUtf8(), sampleId);
    }

    public void addSample(String name, Integer id, ByteString bs){
        if(name2id.containsKey(name)) throw new IllegalArgumentException(String.format("Sample name '%s' already exists",name));
        if(id2name.containsKey(id)) throw new IllegalArgumentException(String.format("Sample id '%s' already exists!",id));
        builder.addSampleNames(name);
        builder.addSampleId(id);
        builder.addSampleColumn(bs);
        
        updateName(id, name);
        updateCol(id, bs);
    }

    public Integer getIdFromName(String name){
        return name2id.get(name);
    }

    public String getNameFromId(Integer id){
        return id2name.get(id);
    }
    
    public ByteString getColumnFromId(Integer id){
        return id2col.get(id);
    }
    
    public Integer getIdFromColumn(String col){
        return col2id.get(col);
    }
    
    public Optional<Integer> maxId(){
        return id2name.keySet().parallelStream().reduce(Math::max);
    }
    
    public Integer nextId(){
        Optional<Integer> max = maxId();
        if(max.isPresent()){
            return max.get() + 1;
        }
        return 0;
    }
}
