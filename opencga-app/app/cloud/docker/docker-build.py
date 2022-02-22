#!/usr/bin/env python3

import argparse
import json
import os
import requests
import sys
# import pathlib
from pathlib import Path

## Configure command-line options
parser = argparse.ArgumentParser()
parser.add_argument('action', help="Action to execute", choices=["build", "push", "delete"], default="build")
parser.add_argument('--images',
                    help="comma separated list of images to be made, e.g. base,init,demo,r,samtools,ext-tools")
parser.add_argument('--tag', help="the tag for this code, e.g. v2.0.0-hdp3.1")
parser.add_argument('--build-folder', help="the location of the build folder, if not default location")
parser.add_argument('--org', help="Docker organization", default="opencb")
parser.add_argument('--username', help="Username to login to the docker registry (REQUIRED if deleting from DockerHub)")
parser.add_argument('--password', help="Password to login to the docker registry (REQUIRED if deleting from DockerHub)")
parser.add_argument('--docker-build-args',
                    help="Additional build arguments to pass to the docker build command. Usage: --docker-build-args='ARGS' e.g: --docker-build-args='--no-cache'",
                    default="")
parser.add_argument('--server', help="Docker registry server", default="docker.io")

## Some ANSI colors to print shell output
shell_colors = {
    'red': '\033[91m',
    'green': '\033[92m',
    'blue': '\033[94m',
    'magenta': '\033[95m',
    'bold': '\033[1m',
    'reset': '\033[0m'
}


def error(message):
    sys.stderr.write(shell_colors['red'] + 'ERROR: %s\n' % message + shell_colors['reset'])
    sys.exit(2)


def print_header(str):
    print(shell_colors['magenta'] + "*************************************************" + shell_colors['reset'])
    print(shell_colors['magenta'] + str + shell_colors['reset'])
    print(shell_colors['magenta'] + "*************************************************" + shell_colors['reset'])


def run(command):
    print(shell_colors['bold'] + command + shell_colors['reset'])
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
    print_header('Building docker images: ' + ', '.join(images))

    for i in images:
        image = org + "/opencga-" + i + ":" + tag
        print("\n" + shell_colors['blue'] + "Building " + image + " ..." + shell_colors['reset'])

        if i == "init" or i == "demo":
            command = ("docker build"
                       + " -t " + image
                       + " -f " + build_folder + "/cloud/docker/opencga-" + i + "/Dockerfile"
                       + " --build-arg TAG=" + tag
                       + " --build-arg ORG=" + org
                       + " " + args.docker_build_args + " "
                       + " " + build_folder)
        else:
            command = ("docker build"
                       + " -t " + image
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

    latest_tag = os.popen(
        ("curl -s https://registry.hub.docker.com/v1/repositories/" + org + "/opencga-" + image + "/tags"
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
            run("docker tag " + image + ":" + tag + " " + server + image + ":" + tag)
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
        requests.delete('https://hub.docker.com/v2/repositories/' + org + '/opencga-' + i + '/tags/' + tag + '/',
                        headers=headers)


## Parse command-line parameters and init basedir, tag and build_folder
args = parser.parse_args()

# 1. Set build folder to default value if not set
if args.build_folder is not None:
    build_folder = args.build_folder
    if not os.path.isdir(build_folder):
        error("Build folder does not exist: " + build_folder)
    if not os.path.isdir(build_folder + "/libs") or not os.path.isdir(build_folder + "/conf") or not os.path.isdir(
            build_folder + "/bin"):
        error("Not a build folder: " + build_folder)
else:
    # root of the opencga repo
    build_folder = str(Path(__file__).resolve().parents[2])

# 2. Set docker tag to default value if not set
if args.tag is not None:
    tag = args.tag
else:
    # Read OpenCGA version from git.properties
    git_properties = build_folder + "/misc/git/git.properties"
    if not os.path.isfile(git_properties):
        error("Missing '" + git_properties + "'")

    stream = os.popen("grep 'git.build.version' " + git_properties + " | cut -d '=' -f 2")
    version = stream.read()
    version = version.rstrip()
    if not version:
        error("Missing --tag")

    # Get hadoop_flavour from JAR library file name
    hadoop_flavour = None
    for file in os.listdir(build_folder + "/libs/"):
        if (file.startswith("opencga-storage-hadoop-deps")):
            if hadoop_flavour is not None:
                exit("Error. Multiple libs/opencga-storage-hadoop-deps*.jar found")
            hadoop_flavour = file.split("-")[4]

    # Create docker tag
    tag = version
    if hadoop_flavour:
        tag = tag + "-" + hadoop_flavour

# 3. Set docker org to default value if not set
org = args.org

# 4. Set docker server
if args.server != "docker.io":
    server = args.server + "/"
else:
    server = ""

# 5. Get a list with all images
if args.images is None:
    images = ["base", "init", "demo", "r", "samtools", "ext-tools"]
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

## Execute the action
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
