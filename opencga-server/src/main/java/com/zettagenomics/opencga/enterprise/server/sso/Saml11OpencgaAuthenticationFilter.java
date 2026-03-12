package com.zettagenomics.opencga.enterprise.server.sso;

import org.jasig.cas.client.Protocol;

public class Saml11OpencgaAuthenticationFilter extends OpencgaAuthenticationFilter {
    public Saml11OpencgaAuthenticationFilter() {
        super(Protocol.SAML11);
    }
}
