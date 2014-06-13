package org.opencb.opencga.storage.variant.mongodb;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.util.List;
import java.util.Map;
import org.opencb.biodata.models.variant.ArchivedVariantFile;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.utils.CryptoUtils;
import org.opencb.datastore.core.ComplexTypeConverter;
import static org.opencb.opencga.storage.variant.mongodb.VariantMongoWriter.CHUNK_SIZE_BIG;
import static org.opencb.opencga.storage.variant.mongodb.VariantMongoWriter.CHUNK_SIZE_SMALL;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DBObjectToVariantConverter implements ComplexTypeConverter<Variant, DBObject> {

    private DBObjectToArchivedVariantFileConverter archivedVariantFileConverter;

    /**
     * Create a converter between Variant and DBObject entities when there is 
     * no need to convert the files the variant was read from.
     */
    public DBObjectToVariantConverter() {
        this(null);
    }

    /**
     * Create a converter between Variant and DBObject entities. A converter for 
     * the files the variant was read from can be provided in case those 
     * should be processed during the conversion.
     * 
     * @param archivedVariantFileConverter The mongoVariant used to convert the files
     */
    public DBObjectToVariantConverter(DBObjectToArchivedVariantFileConverter archivedVariantFileConverter) {
        this.archivedVariantFileConverter = archivedVariantFileConverter;
    }
    
    
    @Override
    public Variant convertToDataModelType(DBObject object) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DBObject convertToStorageType(Variant object) {
        // Attributes easily calculated
        BasicDBObject mongoVariant = new BasicDBObject("_id", buildStorageId(object)).append("id", object.getId()).append("type", object.getType().name());
        mongoVariant.append("chr", object.getChromosome()).append("start", object.getStart()).append("end", object.getStart());
        mongoVariant.append("length", object.getLength()).append("ref", object.getReference()).append("alt", object.getAlternate());
        
        // Internal fields used for query optimization (dictionary named "_at")
        BasicDBObject _at = new BasicDBObject();
        mongoVariant.append("_at", _at);
        
        // ChunkID (1k and 10k)
        String chunkSmall = object.getChromosome() + "_" + object.getStart() / CHUNK_SIZE_SMALL + "_" + CHUNK_SIZE_SMALL / 1000 + "k";
        String chunkBig = object.getChromosome() + "_" + object.getStart() / CHUNK_SIZE_BIG + "_" + CHUNK_SIZE_BIG / 1000 + "k";
        BasicDBList chunkIds = new BasicDBList(); chunkIds.add(chunkSmall); chunkIds.add(chunkBig);
        _at.append("chunkIds", chunkIds);
        
        // Transform HGVS: Map of lists -> List of map entries
        BasicDBList hgvs = new BasicDBList();
        for (Map.Entry<String, List<String>> entry : object.getHgvs().entrySet()) {
            for (String value : entry.getValue()) {
                hgvs.add(new BasicDBObject("type", entry.getKey()).append("name", value));
            }
        }
        mongoVariant.append("hgvs", hgvs);
        
        // Files
        if (archivedVariantFileConverter != null) {
            BasicDBList mongoFiles = new BasicDBList();
            for (ArchivedVariantFile archiveFile : object.getFiles().values()) {
                mongoFiles.add(archivedVariantFileConverter.convertToStorageType(archiveFile));
            }
            mongoVariant.append("files", mongoFiles);
        }
        
        // TODO Effects
        
        return mongoVariant;
    }

    public String buildStorageId(Variant v) {
        StringBuilder builder = new StringBuilder(v.getChromosome());
        builder.append("_");
        builder.append(v.getStart());
        builder.append("_");
        if (v.getReference().length() < Variant.SV_THRESHOLD) {
            builder.append(v.getReference());
        } else {
            builder.append(new String(CryptoUtils.encryptSha1(v.getReference())));
        }
        
        builder.append("_");
        
        if (v.getAlternate().length() < Variant.SV_THRESHOLD) {
            builder.append(v.getAlternate());
        } else {
            builder.append(new String(CryptoUtils.encryptSha1(v.getAlternate())));
        }
            
        return builder.toString();
    }
}
