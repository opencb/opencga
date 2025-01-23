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

package com.zettagenomics.opencga.enterprise.client.rest;

import com.zettagenomics.opencga.enterprise.client.rest.clients.*;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.models.user.AuthenticationResponse;


public class EnterpriseOpenCGAClient extends OpenCGAClient {

    public EnterpriseOpenCGAClient(ClientConfiguration clientConfiguration) {
        super(clientConfiguration);
    }

    public EnterpriseOpenCGAClient(String organizationId, String user, String password,
                                   ClientConfiguration clientConfiguration) throws ClientException {
        super(organizationId, user, password, clientConfiguration);
    }

    public EnterpriseOpenCGAClient(AuthenticationResponse authenticationTokens, ClientConfiguration clientConfiguration) {
        super(authenticationTokens, clientConfiguration);
    }

    public OrganizationClient getEnterpriseOrganizationClient() {
        return this.getClient(OrganizationClient.class, () -> new OrganizationClient(this.token, this.clientConfiguration));
    }

    public UserClient getEnterpriseUserClient() {
        return this.getClient(UserClient.class, () -> new UserClient(this.token, this.clientConfiguration));
    }

    public ProjectClient getEnterpriseProjectClient() {
        return this.getClient(ProjectClient.class, () -> new ProjectClient(this.token, this.clientConfiguration));
    }

    public StudyClient getEnterpriseStudyClient() {
        return this.getClient(StudyClient.class, () -> new StudyClient(this.token, this.clientConfiguration));
    }

    public FileClient getEnterpriseFileClient() {
        return this.getClient(FileClient.class, () -> new FileClient(this.token, this.clientConfiguration));
    }

    public JobClient getEnterpriseJobClient() {
        return this.getClient(JobClient.class, () -> new JobClient(this.token, this.clientConfiguration));
    }

    public IndividualClient getEnterpriseIndividualClient() {
        return this.getClient(IndividualClient.class, () -> new IndividualClient(this.token, this.clientConfiguration));
    }

    public SampleClient getEnterpriseSampleClient() {
        return this.getClient(SampleClient.class, () -> new SampleClient(this.token, this.clientConfiguration));
    }

    public AdminClient getEnterpriseAdminClient() {
        return this.getClient(AdminClient.class, () -> new AdminClient(this.token, this.clientConfiguration));
    }

    public CohortClient getEnterpriseCohortClient() {
        return this.getClient(CohortClient.class, () -> new CohortClient(this.token, this.clientConfiguration));
    }

    public ClinicalAnalysisClient getEnterpriseClinicalAnalysisClient() {
        return this.getClient(ClinicalAnalysisClient.class,
                () -> new ClinicalAnalysisClient(this.token, this.clientConfiguration));
    }

    public DiseasePanelClient getEnterpriseDiseasePanelClient() {
        return this.getClient(DiseasePanelClient.class,
                () -> new DiseasePanelClient(this.token, this.clientConfiguration));
    }

    public FamilyClient getEnterpriseFamilyClient() {
        return this.getClient(FamilyClient.class, () -> new FamilyClient(this.token, this.clientConfiguration));
    }

    public AlignmentClient getEnterpriseAlignmentClient() {
        return this.getClient(AlignmentClient.class, () -> new AlignmentClient(this.token, this.clientConfiguration));
    }

    public VariantClient getEnterpriseVariantClient() {
        return this.getClient(VariantClient.class, () -> new VariantClient(this.token, this.clientConfiguration));
    }

    public VariantOperationClient getEnterpriseVariantOperationClient() {
        return this.getClient(VariantOperationClient.class,
                () -> new VariantOperationClient(this.token, this.clientConfiguration));
    }

    public MetaClient getEnterpriseMetaClient() {
        return this.getClient(MetaClient.class, () -> new MetaClient(this.token, this.clientConfiguration));
    }

    public CVDBClient getEnterpriseCvdbAnalysisClient() {
        return getClient(CVDBClient.class, () -> new CVDBClient(token, clientConfiguration));
    }

}
