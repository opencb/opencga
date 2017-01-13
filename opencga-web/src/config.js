/*
 * Copyright 2015-2016 OpenCB
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

var opencga = {
    host: "bioinfodev.hpc.cam.ac.uk/hgva-1.0.0",
    version: "v1",
    cookiePrefix: "opencga"
};

var application = {
    title: "Catalog",
    version: "v1.0.0",
    logo: "images/opencb-logo.png",
    menu: [
        {
            id: "browser",
            title: "Variant Browser",
            visibility: "none"
        },
        {
            id: "prioritization",
            title: "Prioritization",
            visibility: "none"
        },
        {
            id: "diagnose",
            title: "Diagnose",
            visibility: "none",
            submenu: [
                {
                    id: "diagnose:sample",
                    title: "Sample",
                    visibility: "public"
                },
                {
                    id: "diagnose:family",
                    title: "Family",
                    visibility: "public"
                }]
        },
        {
            id: "beacon",
            title: "Beacon",
            visibility: "none"
        },
        {
            id: "analysis",
            title: "Analysis",
            visibility: "none",
            submenu: [
                {
                    id: "ibs",
                    title: "IBS",
                    visibility: "public"
                },
                {
                    id: "burden",
                    title: "Burden Test",
                    visibility: "public"
                }]
        },
        {
            id: "tools",
            title: "Tools",
            visibility: "none",
            submenu: [
                {
                    id: "genomeBrowser",
                    title: "Genome Browser",
                    visibility: "public"
                },
                {
                    separator: true,
                    visibility: "public"
                },
                {
                    id: "exporter",
                    title: "Exporter",
                    visibility: "public"
                }
            ]
        }
    ],
    search: {
        placeholder: "eg. BRCA2",
        visibility: "none"
    },
    settings: {
        visibility: "public"
    },
    about: [
        {"name": "Documentation",  "url": "https://github.com/opencb/opencga/wiki/OpenCGA-Catalog", "icon": "fa fa-book"},
        {"name": "Source code", "url": "https://github.com/opencb/opencga", "icon": "fa fa-github"},
        {"name": "Contact",  "url": "", "icon": "fa fa-envelope"},
        {"name": "FAQ",  "url": "", "icon": ""}
    ],
    breadcrumb: {
        title: "Dashboard",
        visibility: "private"
    }
};