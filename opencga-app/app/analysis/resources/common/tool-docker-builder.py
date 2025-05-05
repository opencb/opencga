#!/usr/bin/env python3

import argparse
import os
import sys

## Configure command-line options
parser = argparse.ArgumentParser()
parser.add_argument("action", help="Action to execute", choices=["dockerfile", "build", "push"], default="dockerfile")
parser.add_argument("-b", "--base-image", help="Dockerfile FROM image", default="ubuntu:24.04")
parser.add_argument("-t", "--custom-tool-dir", help="Path to the tool folder, this can contain a requirement.txt and/or install.r files", required=True)
parser.add_argument("--update-os", help="Update OS using 'apt update'", action='store_true')
parser.add_argument("--install-r", help="Install R", action='store_true')
parser.add_argument("--apt-get", help="List of apt-get packages to install")
parser.add_argument("-o", "--organisation", help="Organisation (or namespace) of the Docker image", default="opencb")
parser.add_argument("-n", "--name", help="Name of the docker image, e.g. my-tool")
parser.add_argument("-v", "--version", help="Tag of the docker image, e.g. v1.0.0", default="1.0.0")
parser.add_argument("-l", "--latest", help="Make the docker image latest, e.g. v1.0.0", default="False")
parser.add_argument("-u", "--username", help="Username to login to the docker registry")
parser.add_argument("-p", "--password", help="Password or Personal Access Token (PAT) to login to the docker registry",)
# parser.add_argument('--server', help="Docker registry server", default="docker.io")

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

    code = os.system(f"docker login -u \"{args.username}\" --password \"{args.password}\"")
    if code != 0:
        error("Error executing: '" + "docker login -u " + args.username + " --password xxxxxxx'")

def get_docker_image_id():
    print_header('Getting docker image ID: ...')

    ## Check if the required parameters are set
    if args.organisation is None or args.organisation == "" or args.name is None or args.name == "" or args.version is None or args.version == "":
        error("Docker organisation, name and version are all required")

    return args.organisation + "/" + args.name + ":" + args.version


def dockerfile():
    print(shell_colors['blue'] + "Creating Dockerfile  ..." + shell_colors['reset'])

    with open(tool_builder_folder + "/Dockerfile", "w") as f:
        ## Set base image and copy the custom tool folder
        f.write(f"FROM {args.base_image}\n\n")
        f.write("COPY . /opt/app\n\n")

        run = "RUN"
        if args.update_os is True:
            ## Install Ubuntu OS dependencies with 'apt'
            # 1. Update Ubuntu and install base libraries
            f.write(f"{run} apt update && apt -y upgrade && apt install -y python3 python3-pip ca-certificates curl && \\ \n")

            # 2. Install Docker
            f.write("install -m 0755 -d /etc/apt/keyrings && \\ \n")
            f.write("curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc && \\ \n")
            f.write("chmod a+r /etc/apt/keyrings/docker.asc && \\ \n")
            f.write("echo \"deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo \"$VERSION_CODENAME\") stable\" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \\ \n")
            f.write("apt update && apt install -y docker-ce docker-ce-cli containerd.io && \\ \n")
            run = ""

        # 3. Install R
        if args.install_r is True or os.path.isfile(tool_builder_folder + "/install.r"):
            f.write(f"{run} apt install -y r-base && \\ \n")
            run = ""

        # 4. Install user's apt dependencies
        if args.apt_get is not None and not args.apt_get == "":
            f.write(f"{run} apt install -y " + args.apt_get.replace(",", " ") + " && \\ \n")
            run = ""

        # 5. Check and build C/C++ tools
        if os.path.isfile(tool_builder_folder + "/makefile") or os.path.isfile(tool_builder_folder + "/Makefile"):
            f.write(f"{run}apt install -y build-essential && \\ \n")
            f.write("make -C /opt/app && \\ \n")
            run = ""

        ## Install application dependencies, only Python and R supported
        # 1. Install Python dependencies
        if os.path.isfile(tool_builder_folder + "/requirements.txt"):
            f.write(f"{run} pip3 install -r /opt/app/requirements.txt --break-system-packages && \\ \n")
            run = ""

        # 2. Install R dependencies
        if args.install_r is True and os.path.isfile(tool_builder_folder + "/install.r"):
            f.write(f"{run} Rscript /opt/app/install.r && \\ \n")
            run = ""

        ## Clean up and set working directory
        f.write(f"{run} rm -rf /var/lib/apt/lists/* \n\n")
        f.write("WORKDIR /opt/app\n\n")
        f.write("CMD bash\n")

def build():
    image = get_docker_image_id()
    print(shell_colors['blue'] + "Building " + image + " ..." + shell_colors['reset'])

    if os.path.isfile("/var/run/docker.sock"):
        command = ("docker build"
                   + " -t " + image
                   + " -f " + tool_builder_folder +  "/Dockerfile"
                   + " -v /var/run/docker.sock:/var/run/docker.sock"
                   + " --env DOCKER_HOST='tcp://localhost:2375'"
                   + " --network host"
                   + " " + tool_builder_folder)
    else:
        command = ("docker build"
                   + " -t " + image
                   + " -f " + tool_builder_folder +  "/Dockerfile"
                   + " " + tool_builder_folder)
    print(f"Build command: {command}")
    run(command)

def tag_latest(image):
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

    run("docker push " + image)
    if args.latest == True:
        tag_latest(image)


## Parse command-line parameters and init basedir, tag and build_folder
args = parser.parse_args()

# 1. Set build folder to default value if not set
if args.custom_tool_dir is not None and not args.custom_tool_dir == "":
    if args.custom_tool_dir.startswith("git@") or args.custom_tool_dir.startswith("https://"):
        tool_builder_folder = "/tmp/tool-docker-builder"
        os.system("rm -rf " + tool_builder_folder)
        os.system("git clone " + args.custom_tool_dir + " " + tool_builder_folder)
    else:
        tool_builder_folder = args.custom_tool_dir

    if not os.path.isdir(tool_builder_folder):
        error("Custom tool folder does not exist: " + tool_builder_folder)
else:
    error("Custom tool folder is required")

## 2. Execute the action
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
