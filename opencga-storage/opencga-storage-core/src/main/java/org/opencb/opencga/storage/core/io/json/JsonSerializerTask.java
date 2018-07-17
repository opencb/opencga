package org.opencb.opencga.storage.core.io.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.avro.generic.GenericRecord;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.variant.io.json.mixin.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;

import java.util.ArrayList;
import java.util.List;

/**
 * Created on 18/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class JsonSerializerTask<T> implements Task<T, String> {

    protected final ObjectMapper jsonObjectMapper;
    private final Class<T> clazz;
    private ObjectWriter jsonWriter;

    public JsonSerializerTask(Class<T> clazz) {
        this.clazz = clazz;
        jsonObjectMapper = new ObjectMapper(new JsonFactory());
    }

    @Override
    public void pre() throws Exception {
        configureObjectMapper();
        jsonWriter = jsonObjectMapper.writerFor(VariantStatsWrapper.class);
    }

    public void configureObjectMapper() {
        jsonObjectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
    }

    @Override
    public List<String> apply(List<T> list) throws Exception {
        List<String> result = new ArrayList<>();
        for (T object : list) {
            String string = jsonWriter.writeValueAsString(object);
            result.add(string);
        }
        return result;
    }
}
