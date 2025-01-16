package org.opencb.opencga.core.models.federation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties({"password", "secretKey", "token"})
public class FederationClientParamsMixin {
}
