const JSON5 = require('json5')
const fs = require('fs')
const path = require('path')
const shell = require('shelljs');

/**
 * Find all files recursively in specific folder with specific extension, e.g:
 * findFilesInDir('./project/src', '.html') ==> ['./project/src/a.html','./project/src/build/index.html']
 * @param  {String} startPath    Path relative to this file or other file which requires this files
 * @param  {String} filter       Extension name, e.g: '.html'
 * @return {Array}               Result files with path string in an array
 */
function findFilesInDir(startPath) {

    var results = [];

    if (!fs.existsSync(startPath)) {
        console.log("no dir ", startPath);
        return;
    }

    var files = fs.readdirSync(startPath);
    for (var i = 0; i < files.length; i++) {
        var filename = path.join(startPath, files[i]);
        var stat = fs.lstatSync(filename);
        if (stat.isDirectory()) {
            results = results.concat(findFilesInDir(filename)); //recurse
        } else {
            results.push(filename);
        }

    }
    return results;
}

function processInlineTextFileFunc(fileContent, filepath) {
    let startPos = fileContent.indexOf("[local-textFile('")
    
    if (startPos !== -1) {
        startPos = startPos + "[local-textFile('".length
        let endPos = fileContent.indexOf("')]", startPos)
        console.log(endPos)
        let fileToInline = fileContent.substr(startPos, endPos - startPos)

        let dir = path.dirname(filepath)

        let inlinefileContent = fs.readFileSync(path.join(dir, fileToInline)).toString()

        return fileContent.replace(`[local-textFile('${fileToInline}')]`, inlinefileContent.replace(/\r?\n|\r/g, "\\n"))
    }
    return fileContent
}

function processInlineJsonFileFunc(fileContent, filepath) {
    let startPos = fileContent.indexOf("[local-jsonFile('")
    
    if (startPos !== -1) {
        startPos = startPos + "[local-jsonFile('".length
        let endPos = fileContent.indexOf("')]", startPos)
        console.log(endPos)
        let fileToInline = fileContent.substr(startPos, endPos - startPos)

        let dir = path.dirname(filepath)

        let inlinefileContent = fs.readFileSync(path.join(dir, fileToInline)).toString()

        let isObject = false
        try {
            JSON5.parse(inlinefileContent)
            isObject = true
        } catch (e) {
            console.log(filepath)
            console.error(e)
            throw "can't use jsonFile func on non-json file"
        }
       
        if (isObject) {
            return fileContent.replace(`"[local-jsonFile('${fileToInline}')]"`, inlinefileContent)
        }
    }
    return fileContent
}

const files = findFilesInDir('/home/lawrence/source/hacks/opencga-azure/arm')

files.forEach(function (filepath) {

    let dir = path.dirname(filepath.replace('/arm/', '/output/'))
    try {
        fs.mkdirSync(dir, { recursive: true })
    } catch { console.warn("failed creating dir: " + dir) }

    let fileContent = fs.readFileSync(filepath).toString()
    if (filepath.includes('.json5')) {
        fileContent = processInlineJsonFileFunc(fileContent, filepath)
        fileContent = processInlineTextFileFunc(fileContent, filepath)
        var parsed = JSON5.parse(fileContent)
        fileContent = JSON.stringify(parsed, undefined, 2)
    }
    fs.writeFileSync(filepath.replace('/arm/', '/output/').replace('.json5', '.json'), fileContent, { flag: 'wx' })


})


