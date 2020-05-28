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

package org.opencb.opencga.core.models.panel;

import org.opencb.opencga.core.models.AclParams;

public class PanelAclUpdateParams extends AclParams {

    private String panel;

    public PanelAclUpdateParams() {
    }

    public PanelAclUpdateParams(String panel, String permissions) {
        super(permissions);
        this.panel = panel;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PanelAclUpdateParams{");
        sb.append("panel='").append(panel).append('\'');
        sb.append(", permissions='").append(permissions).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getPanel() {
        return panel;
    }

    public PanelAclUpdateParams setPanel(String panel) {
        this.panel = panel;
        return this;
    }

    public PanelAclUpdateParams setPermissions(String permissions) {
        super.setPermissions(permissions);
        return this;
    }

}
