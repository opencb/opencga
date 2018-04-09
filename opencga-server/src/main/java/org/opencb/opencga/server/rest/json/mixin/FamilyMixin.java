package org.opencb.opencga.server.rest.json.mixin;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties({"id", "creationDate", "status", "release", "version"})
public abstract class FamilyMixin {
}
