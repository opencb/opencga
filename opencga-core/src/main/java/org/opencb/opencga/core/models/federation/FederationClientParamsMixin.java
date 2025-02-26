package org.opencb.opencga.core.models.federation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties({"email", "url", "organizationId", "userId", "password", "securityKey", "token"})
public class FederationClientParamsMixin {
}
