package org.opencb.opencga.storage.core.variant.io.json.mixin;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.util.StdConverter;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.cellbase.client.rest.ParentRestClient;

import java.util.List;

@JsonDeserialize(
        converter = ParentRestClient.ConsequenceTypeMixin.ConsequenceTypeConverter.class
)
public interface ConsequenceTypeMixin {
    class ConsequenceTypeConverter extends StdConverter<ConsequenceType, ConsequenceType> {
        public ConsequenceTypeConverter() {
        }

        public ConsequenceType convert(ConsequenceType consequenceType) {
            // Do not use JsonAlias for geneId and transcriptId, as we want to keep the old values.
            if (consequenceType != null) {
                if (consequenceType.getGeneId() == null) {
                    consequenceType.setGeneId(consequenceType.getEnsemblGeneId());
                }

                if (consequenceType.getTranscriptId() == null) {
                    consequenceType.setTranscriptId(consequenceType.getEnsemblTranscriptId());
                }
            }

            return consequenceType;
        }
    }

    @JsonAlias("transcriptAnnotationFlags")
    List<String> getTranscriptFlags();


}
