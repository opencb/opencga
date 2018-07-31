package org.opencb.opencga.server.rest.json.mixin;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties({"studyUid", "uid"})
public abstract class PrivateUidMixin {
}
