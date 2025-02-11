package org.opencb.opencga.core.models.federation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties({"password", "securityKey", "token"})
public class FederationClientParamsMixin {
}
