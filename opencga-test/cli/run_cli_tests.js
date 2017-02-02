#!/usr/bin/env nodejs

const fs = require('fs');

var config = parseCli();
console.log(config)
console.log()

let testSuiteDir = config.path;
if (testSuiteDir.endsWith('OpenCga')) {
    testSuiteDir += '/TestsSuits';
}

// Iterate over all test suites
fs.readdir(testSuiteDir, function (err, files) {
    files.forEach(file => {
        // console.log(file)
        let suite = testSuiteDir + '/' + file;
        // console.log(suite)
        fs.readdir(suite, function (err1, dirs) {
            if (typeof dirs !== 'undefined') {
                dirs.forEach(dir => {
                    // console.log("dir: " + dir);
                    if (dir != 'SetUp' && dir != 'content.txt' && dir != 'properties.xml') {
                        // console.log(suite + '/' + dir + '/content.txt')
                        fs.readFile(suite + '/' + dir + '/content.txt', 'utf8', function (err2, content) {
                            console.log("CLI for %s", dir)

                            processSuiteTest(content);
                        });
                    }
                });
            }
        });
    });
});

function processSuiteTest(content) {
    let variables = {};
    content.split('\n').forEach(line => {
        if (line.trim() !== '') {
            if (line.indexOf('!define') != -1) {
                let define = line.split(" ");
                variables[define[1]] = define[2].replace("{", "").replace("}", "");
            }

            if (line.indexOf('!3') != -1) {
                console.log(line)
            }
            if (line.indexOf('GET') != -1 || line.indexOf('POST') != -1) {
                let fields = line.split('|');
                // console.log("fields: " + fields[2]);
                if (typeof fields[2] !== 'undefined') {
                    // Get command
                    let clifields = fields[2].split('/');
                    let command = clifields[2];

                    // Get subcommand
                    let clifields1;
                    if (clifields.length == 4) {
                        clifields1 = clifields[3].split('?');
                    } else {
                        clifields1 = clifields[4].split('?');
                    }
                    let subcommand = clifields1[0];

                    // Get options
                    let optionsStr = "";
                    if (typeof clifields1[1] !== 'undefined') {
                        let options = clifields1[1].split('&');
                        options.forEach(o => {
                            let kv = o.split('=');
                            let k = kv[0].replace(/([A-Z])/g, function($1){return "-"+$1.toLowerCase();});
                            let v = kv[1].replace("=", " ")
                                .replace("$sessionId", config.sessionId)
                                .replace("true", "")
                                .replace("false", "")
                                .replace("${study}", variables['study'])
                                .replace("${file}", variables['file'])
                                .replace("${path}", variables['path'])
                                .replace("${sift}", variables['sift']);
                            optionsStr += "--" + k + " " + v + " ";
                        });
                    }

                    console.log("opencga.sh %s %s %s", command, subcommand, optionsStr);
                    console.log()
                }
            }
        }
    })
}

function parseCli() {
    // Default CLI options
    let config = {
        user: 'test',
        password: 'user_P@ssword',
        sessionId: 'AXMAAAVAGS542GAAF',
        path: '../fitnesse/FitNesseRoot/OpenCga',
        debug: false
    };

    // First 2 elements correspond to nodejs binary and script name
    for (let i = 2; i < process.argv.length; i++) {
        // console.log(process.argv[i])
        let keyValue = process.argv[i].split('=');
        switch (keyValue[0]) {
            case 'path':
                config.path = keyValue[1];
                break;
            case 'debug':
                config.debug = (keyValue[1] === 'true');
                break;
            default:
                console.log("Parameter '%s' hot recognised", process.argv[i]);
                break
        }
    }
    return config;
}
