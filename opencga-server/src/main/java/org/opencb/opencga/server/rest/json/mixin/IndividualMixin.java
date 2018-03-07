package org.opencb.opencga.server.rest.json.mixin;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties({"id", "fatherId", "motherId", "family", "species", "release", "version", "creationDate", "status"})
public class IndividualMixin {
}
