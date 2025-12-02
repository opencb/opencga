package org.opencb.opencga.core.models.common.mixins;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.util.StdConverter;
import org.opencb.biodata.models.variant.avro.ConsequenceType;

import java.util.List;

@JsonDeserialize(
        converter = ConsequenceTypeMixin.ConsequenceTypeConverter.class
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
