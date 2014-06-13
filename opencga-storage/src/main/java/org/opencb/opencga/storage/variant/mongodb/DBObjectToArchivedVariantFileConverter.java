package org.opencb.opencga.storage.variant.mongodb;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.ArchivedVariantFile;
import org.opencb.datastore.core.ComplexTypeConverter;

/**
 * 
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DBObjectToArchivedVariantFileConverter implements ComplexTypeConverter<ArchivedVariantFile, DBObject> {

    private List<String> samples;
    
    private DBObjectToVariantStatsConverter statsConverter;

    /**
     * Create a converter between ArchivedVariantFile and DBObject entities when 
     * there is no need to provide a list of samples.
     */
    public DBObjectToArchivedVariantFileConverter() {
        this(null, null);
    }
    
    /**
     * Create a converter between ArchivedVariantFile and DBObject entities. A 
     * list of samples and a statistics converter may be provided in case those 
     * should be processed during the conversion.
     * 
     * @param samples The list of samples, if any
     * @param statsConverter The object used to convert the file statistics
     */
    public DBObjectToArchivedVariantFileConverter(List<String> samples, DBObjectToVariantStatsConverter statsConverter) {
        this.samples = samples;
        this.statsConverter = statsConverter;
    }
    
    
    @Override
    public ArchivedVariantFile convertToDataModelType(DBObject object) {
        ArchivedVariantFile file = new ArchivedVariantFile((String) object.get("fileName"), (String) object.get("fileId"), (String) object.get("studyId"));
        
        // Attributes
        if (object.containsField("attributes")) {
            file.setAttributes(((DBObject) object.get("attributes")).toMap());
        }
        
        // Samples
        if (object.containsField("samples")) {
            BasicDBList genotypes = (BasicDBList) object.get("samples");
            
            Iterator<String> samplesIterator = samples.iterator();
            Iterator<Object> genotypesIterator = genotypes.iterator();
            while (samplesIterator.hasNext() && genotypesIterator.hasNext()) {
                String sampleName = samplesIterator.next();
                Genotype gt = Genotype.decode((int) genotypesIterator.next());
                Map<String, String> sampleData = new HashMap<>();
                sampleData.put("GT", gt.toString());
                file.addSampleData(sampleName, sampleData);
            }
        }
        
        // Statistics
        if (statsConverter != null && object.containsField("stats")) {
            file.setStats(statsConverter.convertToDataModelType((DBObject) object.get("stats")));
        }
        return file;
    }

    @Override
    public DBObject convertToStorageType(ArchivedVariantFile object) {
        BasicDBObject mongoFile = new BasicDBObject("fileId", object.getFileId()).append("studyId", object.getStudyId());

        // Attributes
        if (object.getAttributes().size() > 0) {
            BasicDBObject attrs = null;
            for (Map.Entry<String, String> entry : object.getAttributes().entrySet()) {
                if (attrs == null) {
                    attrs = new BasicDBObject(entry.getKey(), entry.getValue());
                } else {
                    attrs.append(entry.getKey(), entry.getValue());
                }
            }

            if (attrs != null) {
                mongoFile.put("attributes", attrs);
            }
        }

        // Samples
        if (samples != null && !samples.isEmpty()) {
            mongoFile.append("format", object.getFormat()); // Useless field if genotypeCodes are not stored

            BasicDBList genotypeCodes = new BasicDBList();
            for (String sampleName : samples) {
                String genotype = object.getSampleData(sampleName, "GT");
                if (genotype != null) {
                    genotypeCodes.add(new Genotype(genotype).encode());
                }
            }
            mongoFile.put("samples", genotypeCodes);
        }
        
        // Statistics
        if (statsConverter != null) {
            mongoFile.put("stats", statsConverter.convertToStorageType(object.getStats()));
        }
        
        return mongoFile;
    }
    
}
