package org.opencb.opencga.core.models.federation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties({"password", "secretKey"})
public class FederationClientMixin {
}
