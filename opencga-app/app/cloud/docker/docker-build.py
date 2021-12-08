#!/usr/bin/env python3

import argparse
import os
import requests
import sys
import json
import pathlib
from pathlib import Path

def error(message):
    sys.stderr.write('error: %s\n' % message)
    #parser.print_help()
    sys.exit(2)

def run(command):
    print(command)
    code = os.system(command)
    if code != 0:
        error("Error executing: " + command)

def login(loginRequired=False):
    if args.username is None or args.password is None:
        if loginRequired:
            error("Username and password are required")
        else:
            return

    code = os.system("docker login -u " + args.username + " --password " + args.password)
    if code != 0:
        error("Error executing: docker login")

def build():
    print("Building docker images " + str(images))
    for i in images:
        image = org + "/opencga-" + i
        print("*********************************************")
        print("Building " + image + ":" + tag)
        print("*********************************************")
        if i == "init" or i == "demo":
            command = ("docker build"
                + " -t " + image + ":" + tag
                + " -f " + build_folder + "/cloud/docker/opencga-" + i + "/Dockerfile"
                + " --build-arg TAG=" + tag
                + " --build-arg ORG=" + org
                + " " + args.docker_build_args + " "
                + " " + build_folder)
        else:
            command = ("docker build"
                + " -t " + image + ":" + tag
                + " -f " + build_folder + "/cloud/docker/opencga-" + i + "/Dockerfile"
                + " " + args.docker_build_args + " "
                + " " + build_folder)
        run(command)

def tag_latest(image):
    if "hdp" in tag or "dev" in tag:
        print("Don't use tag " + tag + " as latest")
        return
    if server:
        print("Don't use tag latest in server " + server)
        return

    latest_tag = os.popen(("curl -s https://registry.hub.docker.com/v1/repositories/" + org + "/opencga-" + image + "/tags"
                            + " | jq -r .[].name"
                            + " | grep -v latest"
                            + " | grep -v hdp"
                            + " | grep -v dev"
                            + " | sort -r -h"
                            + " | head"))
    if tag >= latest_tag.read():
        print("*********************************************")
        print("Pushing " + org + "/opencga-" + image + ":latest")
        print("*********************************************")
        run("docker tag " + org + "/opencga-" + image + ":" + tag + " " + org + "/opencga-" + image + ":latest")
        run("docker push " + org + "/opencga-" + image + ":latest")
    else:
        print("Don't use tag " + tag + " as latest")

def push():
    print("Pushing images to Docker hub")
    for i in images:
        image = org + "/opencga-" + i
        print("*********************************************")
        print("Pushing " + server + image + ":" + tag)
        print("*********************************************")
        if server:
            run("docker tag " + image + ":" + tag + " " +  server + image + ":" + tag)
        run("docker push " + server + image + ":" + tag)
        tag_latest(i)

def delete():
    if args.username is None or args.password is None:
        error("Username and password are required")
    headers = {
        'Content-Type': 'application/json',
    }
    data = '{"username": "' + args.username + '", "password": "' + args.password + '"}'
    response = requests.post('https://hub.docker.com/v2/users/login/', headers=headers, data=data)
    json_response = json.loads(response.content)
    if response.status_code != 200:
        error("dockerhub login failed")
    for i in images:
        print('Deleting image on Docker hub for ' + org + '/opencga-' + i + ':' + tag)
        headers = {
            'Authorization': 'JWT ' + json_response["token"]
        }
        requests.delete('https://hub.docker.com/v2/repositories/' + org + '/opencga-' + i + '/tags/' + tag + '/', headers=headers)


parser = argparse.ArgumentParser()

# build, push or delete
parser.add_argument('action', help="Action to execute", choices=["build", "push", "delete"], default="build")

parser.add_argument('--images', help="comma separated list of images to be made, e.g. base,init,demo,r,samtools")
parser.add_argument('--tag', help="the tag for this code, e.g. v2.0.0-hdp3.1")
parser.add_argument('--build-folder', help="the location of the build folder, if not default location")
parser.add_argument('--org', help="Docker organization", default="opencb")
parser.add_argument('--username', help="Username to login to the docker registry (REQUIRED if deleting from DockerHub)")
parser.add_argument('--password', help="Password to login to the docker registry (REQUIRED if deleting from DockerHub)")
parser.add_argument('--docker-build-args', help="Additional build arguments to pass to the docker build command. Usage: --docker-build-args='ARGS' e.g: --docker-build-args='--no-cache'", default="")
parser.add_argument('--server', help="Docker registry server", default="docker.io")

args = parser.parse_args()

# set build folder to default value if not set
if args.build_folder is not None:
    build_folder = args.build_folder
else:
    # root of the opencga repo
    build_folder = str(Path(__file__).resolve().parents[2])

# build_folder = os.path.abspath(build_folder)

if not os.path.isdir(build_folder):
    error("Build folder does not exist: " + build_folder)

if not os.path.isdir(build_folder + "/libs") or not os.path.isdir(build_folder + "/conf") or not os.path.isdir(build_folder + "/bin"):
    error("Not a build folder: " + build_folder)

# set tag to default value if not set
if args.tag is None:

    ## Read version
#     opencgash = build_folder + "/bin/opencga.sh"
#     if not os.path.isfile(opencgash):
#         error("Missing " + opencgash)
#
#     stream = os.popen(opencgash + " --version 2>&1 | grep Version | cut -d ' ' -f 2")
#     version = stream.read()
#     version = version.rstrip()

    git_properties = build_folder + "/misc/git/git.properties"
    if not os.path.isfile(git_properties):
        error("Missing '" + git_properties + "'")

    stream = os.popen("grep 'git.build.version' " + git_properties + " | cut -d '=' -f 2")
    version = stream.read()
    version = version.rstrip()
    print("git_properties: " + git_properties)
    print("version: " + version)

    if not version:
        error("Missing --tag")

    ## Find hadoop_flavour
    hadoop_flavour = None
    for file in os.listdir(build_folder + "/libs/"):
        if (file.startswith("opencga-storage-hadoop-deps")):
            if hadoop_flavour is not None:
                exit("Error. Multiple libs/opencga-storage-hadoop-deps*.jar found")
            hadoop_flavour = file.split("-")[4]

    ## Mount tag
    tag = version
    if hadoop_flavour:
        tag = tag + "-" + hadoop_flavour

else:
    tag = args.tag

org = args.org
if args.server != "docker.io":
    server = args.server + "/"
else:
    server = ""

# get a list with all images
if not args.images:
    images = ["base", "init", "demo", "r", "samtools"]
else:
    imagesUnsorted = args.images.split(",")
    images = []
    if "base" in imagesUnsorted:
        imagesUnsorted.remove("base")
        images += ["base"]
    if "init" in imagesUnsorted:
        imagesUnsorted.remove("init")
        images += ["init"]
    images += imagesUnsorted

if args.action == "build":
    login(loginRequired=False)
    build()
elif args.action == "push":
    login(loginRequired=False)
    build()
    push()
elif args.action == "delete":
    delete()
else:
    error("Unknown action: " + args.action)