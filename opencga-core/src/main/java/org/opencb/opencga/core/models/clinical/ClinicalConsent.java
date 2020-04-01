/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.core.models.clinical;

public class ClinicalConsent {

    private ConsentStatus primaryFindings;
    private ConsentStatus secondaryFindings;
    private ConsentStatus carrierFindings;
    private ConsentStatus researchFindings;

    public enum ConsentStatus {
        YES, NO, UNKNOWN
    }

    public ClinicalConsent() {
        this(ConsentStatus.UNKNOWN, ConsentStatus.UNKNOWN, ConsentStatus.UNKNOWN, ConsentStatus.UNKNOWN);
    }

    public ClinicalConsent(ConsentStatus primaryFindings, ConsentStatus secondaryFindings, ConsentStatus carrierFindings,
                           ConsentStatus researchFindings) {
        this.primaryFindings = primaryFindings != null ? primaryFindings : ConsentStatus.UNKNOWN;
        this.secondaryFindings = secondaryFindings != null ? secondaryFindings : ConsentStatus.UNKNOWN;
        this.carrierFindings = carrierFindings != null ? carrierFindings : ConsentStatus.UNKNOWN;
        this.researchFindings = researchFindings != null ? researchFindings : ConsentStatus.UNKNOWN;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalConsent{");
        sb.append("primaryFindings=").append(primaryFindings);
        sb.append(", secondaryFindings=").append(secondaryFindings);
        sb.append(", carrierFindings=").append(carrierFindings);
        sb.append(", researchFindings=").append(researchFindings);
        sb.append('}');
        return sb.toString();
    }


    public ConsentStatus getPrimaryFindings() {
        return primaryFindings;
    }

    public ClinicalConsent setPrimaryFindings(ConsentStatus primaryFindings) {
        this.primaryFindings = primaryFindings;
        return this;
    }

    public ConsentStatus getSecondaryFindings() {
        return secondaryFindings;
    }

    public ClinicalConsent setSecondaryFindings(ConsentStatus secondaryFindings) {
        this.secondaryFindings = secondaryFindings;
        return this;
    }

    public ConsentStatus getCarrierFindings() {
        return carrierFindings;
    }

    public ClinicalConsent setCarrierFindings(ConsentStatus carrierFindings) {
        this.carrierFindings = carrierFindings;
        return this;
    }

    public ConsentStatus getResearchFindings() {
        return researchFindings;
    }

    public ClinicalConsent setResearchFindings(ConsentStatus researchFindings) {
        this.researchFindings = researchFindings;
        return this;
    }
}
