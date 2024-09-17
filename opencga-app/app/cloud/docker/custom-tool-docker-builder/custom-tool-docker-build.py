#!/usr/bin/env python3

import argparse
import os
import sys

## Configure command-line options
parser = argparse.ArgumentParser()
parser.add_argument("action", help="Action to execute", choices=["dockerfile", "build", "push"], default="dockerfile")
parser.add_argument("-t", "--custom-tool-dir", help="Path to the tool folder, this MUST contain a requirement.txt file")
parser.add_argument("--apt-get", help="List of apt-get packages to install")
parser.add_argument("-o", "--organisation", help="Organisation of the Docker image", default="opencb")
parser.add_argument("-n", "--name", help="Name of the docker image, e.g. my-tool")
parser.add_argument("-v", "--version", help="Tag of the docker image, e.g. v1.0.0", default="1.0.0")
parser.add_argument("-l", "--latest", help="Make the docker image latest, e.g. v1.0.0", default="False")
parser.add_argument("-u", "--username", help="Username to login to the docker registry")
parser.add_argument("-p", "--password", help="Password to login to the docker registry",)
# parser.add_argument('--docker-build-args',
#                     help="Additional build arguments to pass to the docker build command. Usage: --docker-build-args='ARGS' e.g: --docker-build-args='--no-cache'",
#                     default="")
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
        error("Error executing: '" + "docker login -u " + args.username + " --password " + args.password + "'")

def get_docker_image_id():
    print_header('Getting docker image ID: ...')

    ## Check if the required parameters are set
    if args.organisation is None or args.organisation == "" or args.name is None or args.name == "" or args.version is None or args.version == "":
        error("Docker organisation, name and version are all required")

    return args.organisation + "/" + args.name + ":" + args.version


def dockerfile():
    # print_header('Creating Dockerfile ...')
    print(shell_colors['blue'] + "Creating Dockerfile  ..." + shell_colors['reset'])

    with open(custom_build_folder + "/Dockerfile", "w") as f:
        f.write("FROM ubuntu:24.04\n\n")
        f.write("COPY . /app\n\n")

        # Update Ubuntu and install base libraries
        f.write("RUN apt update && apt -y upgrade && apt install -y python3 python3-pip r-base && \\ \n")

        # Install user's apt dependencies
        if args.apt_get is not None and not args.apt_get == "":
            f.write("apt install -y " + args.apt_get.replace(",", " ") + " && \\ \n")

        # Install Python dependencies
        if os.path.isfile(custom_build_folder + "/requirements.txt"):
            f.write("pip3 install -r /app/requirements.txt --break-system-packages && \\ \n")

        # Install R dependencies
        if os.path.isfile(custom_build_folder + "/install.r"):
            f.write("Rscript /app/install.r && \\ \n")

        f.write("rm -rf /var/lib/apt/lists/* \n\n")

        f.write("USER opencga\n\n")
        f.write("WORKDIR /app\n\n")
        f.write("CMD bash\n")

def build():
    image = get_docker_image_id()
    print(shell_colors['blue'] + "Building " + image + " ..." + shell_colors['reset'])

    command = ("docker build"
               + " -t " + image
               + " -f " + custom_build_folder +  "/Dockerfile"
               # + " " + args.docker_build_args + " "
               + " " + custom_build_folder)
    run(command)

def tag_latest(image):
    # if server:
    #     print("Don't use tag latest in server " + server)
    #     return
    latest_tag = os.popen(("curl -s https://registry.hub.docker.com/v1/repositories/" + args.organisation + "/" + args.name + "/tags"
                           + " | jq -r .[].name"
                           + " | grep -v latest"
                           + " | grep -v hdp"
                           + " | grep -v dev"
                           + " | sort -r -h"
                           + " | head"))
    if args.version >= latest_tag.read():
        print_header("Pushing latest tag to Docker Hub...")
        run("docker tag " + image + " " + args.organisation + "/" + args.name + ":latest")
        run("docker push " + args.organisation + "/" + args.name + ":latest")
    else:
        print("Don't use tag " + args.version + " as 'latest'")

def push():
    image = get_docker_image_id()
    print(shell_colors['blue'] + "Pushing to Docker Hub image: " + image + shell_colors['reset'])

    # if server:
    #     run("docker tag " + image + ":" + tag + " " + server + image + ":" + tag)
    # run("docker push " + server + image + ":" + tag)
    run("docker push " + image)
    if args.latest == True:
        tag_latest(image)


## Parse command-line parameters and init basedir, tag and build_folder
args = parser.parse_args()

# 1. Set build folder to default value if not set
if args.custom_tool_dir is not None and not args.custom_tool_dir == "":
    custom_build_folder = args.custom_tool_dir
    if not os.path.isdir(custom_build_folder):
        error("Custom tool folder does not exist: " + custom_build_folder)
else:
    error("Custom tool folder is required")

# 2. Set docker server
# if args.server != "docker.io":
#     server = args.server + "/"
# else:
#     server = ""

## Execute the action
if args.action == "dockerfile":
    dockerfile()
elif args.action == "build":
    dockerfile()
    build()
elif args.action == "push":
    login(loginRequired=True)
    dockerfile()
    build()
    push()
else:
    error("Unknown action: " + args.action)
