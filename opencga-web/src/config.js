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

const cellbase = {
    hosts: ["http://bioinfo.hpc.cam.ac.uk/cellbase/"],
    // hosts: ["http://cellbase.clinbioinfosspa.es/cb"],
    version: "v4",
};

var opencga = {
    host: "http://bioinfo.hpc.cam.ac.uk/hgva",
    version: "v1",

    // This allows IVA to query a OpenCGA instance being an 'anonymous' user, this means that no login is required.
    // If 'projects' is empty then all public projects and studies of 'user' will be used.
    anonymous: {
        // user: "hgvauser",
        // projects: [
        //     {
        //         id: "platinum",
        //         name: "Platinum",
        //         alias: "platinum",
        //         organism: {
        //             scientificName: "Homo sapiens",
        //             assembly: "GRCh37"
        //         },
        //         studies : [
        //             {
        //                 id: "illumina_platinum",
        //                 name: "Illumina Platinum",
        //                 alias: "illumina_platinum"
        //             }
        //         ]
        //     }
        // ]
    },
    cookie: {
        prefix: "iva",
    }
};

const application = {
    title: "Catalog",
    version: "v1.0.0",
    logo: "images/opencb-logo.png",
    // The order, title and nested submenus are respected
    menu: [
        {
            id: "dashboard",
            title: "Dashboard",
            visibility: "public"
        },
        {
            id: "browsers",
            title: "Data Model Browser",
            visibility: "public",
            submenu: [
                {
                    id: "projects",
                    title: "Projects and Studies",
                    visibility: "public",
                },
                {
                    id: "samples",
                    title: "Samples",
                    visibility: "public",
                },
                {
                    id: "cohorts",
                    title: "Cohorts",
                    visibility: "public",
                }
            ]
        },
        {
            id: "tools",
            title: "Tools",
            visibility: "public",
            submenu: [
                {
                    title: "Catalog",
                    category: true,
                    visibility: "public",
                },
                {
                    id: "panel",
                    title: "Panels",
                    visibility: "public",
                },
                {
                    separator: true,
                    visibility: "public",
                },
                {
                    title: "Other",
                    category: true,
                    visibility: "public",
                },
                {
                    id: "genomeBrowser",
                    title: "Genome Browser (Beta)",
                    visibility: "public"
                }
            ]
        },
    ],
    search: {
        placeholder: "Search",
        visible: true,
    },
    settings: {
        visibility: "public",
    },
    about: [
        {name: "Documentation", url: "http://docs.opencb.org/display/opencga/IVA+Home", icon: "fa fa-book"},
        {name: "Tutorial", url: "http://docs.opencb.org/display/opencga/Tutorials", icon: ""},
        {name: "Source code", url: "https://github.com/opencb/opencga", icon: "fa fa-github"},
        {name: "Releases", url: "https://github.com/opencb/opencga/releases", icon: ""},
        {name: "Contact", url: "http://docs.opencb.org/display/opencga/About", icon: "fa fa-envelope"},
        {name: "FAQ", url: "", icon: ""},
    ],
    login: {
        visible: true,
    },
    breadcrumb: {
        title: "Projects",
        visible: true,
    },
    notifyEventMessage: "notifymessage",
    session: {
        // 60000 ms = 1 min
        checkTime: 60000,
        // 60000 ms = 1 min
        minRemainingTime: 60000,
        // 600000 ms = 10 min = 1000(1sec) * 60(60 sec = 1min) * 10(10 min)
        maxRemainingTime: 600000
    }
};

const tools = {
    dashboard: {

    },
    sample: {
        title: "Sample Browser",
        showTitle: true,
        filter: {
            examples: [
                {
                    name: "Somatic",
                    query: {
                        somatic: "true"
                    }
                }
            ]
        },
        grid: {
            pageSize: 3,
            showSelect: true,
        }
    },
    genomeBrowser: {
        active: false,
    }
};
