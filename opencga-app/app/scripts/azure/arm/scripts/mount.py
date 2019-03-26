import sys
import socket
import fcntl
import struct
import random
import os
import shutil
import subprocess
import time
import csv
import ipaddress

# Run `python3 -m unittest discover` in this dir to execute tests

default_mount_options_nfs = "nfs hard,nointr,proto=tcp,mountproto=tcp,retry=30 0 0"
default_mount_options_cifs = "dir_mode=0777,file_mode=0777,serverino,nofail,uid=1001,gid=1001,vers=3.0"

def get_ip_address():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        try:
            # doesn't even have to be reachable
            s.connect(("10.255.255.255", 1))
            return s.getsockname()[0]
        except:
            return "127.0.0.1"

def ip_as_int(ip):
    return int(ipaddress.ip_address(ip))


def remove_lines_containing(file, contains):
    with open(file, "r+") as file:
        d = file.readlines()
        file.seek(0)
        for i in d:
            if contains not in i and i != "\n":
                file.write(i)
        file.truncate()


def print_help():
    print("For example 'sudo python mount.py nfs '10.20.0.1:/folder1/nfsfolder2,10.20.0.1:/folder1/nfsfolder2'")
    print(
        "or 'sudo python mount.py azurefiles <storage-account-name>,<share-name>,<storage-account-key>'"
    )


def install_apt_package(package):
    try:
        print("Attempt to install {}".format(package))
        subprocess.check_call(["apt", "install", package, "-y"])
        print("Install completed successfully")
    except subprocess.CalledProcessError as e:
        print("Failed install {} error: {}".format(package, e))
        raise


# main allows the the mount script to be executable
def main():
    if len(sys.argv) < 3:
        print("Expected arg1: 'mount_type' and arg2 'mount_data'")
        print_help()
        exit(1)

    mount_type = str(sys.argv[1])
    mount_data = str(sys.argv[2])

    mount_share(mount_type, mount_data)

# mount_share allows it to be invoked from other python scripts
def mount_share(mount_type, mount_data):

    if mount_type.lower() != "nfs" and mount_type.lower() != "azurefiles":
        print("Expected first arg to be either 'nfs' or 'azurefiles'")
        print_help()
        exit(1)

    if mount_data == "":
        print(
            """Expected second arg to be the mounting data. For NFS, this should be a CSV of IPs/FQDNS for the NFS servers with NFSExported dirs. 
                For example, '10.20.0.1:/folder1/nfsfolder2,10.20.0.1:/folder1/nfsfolder2'
            For azure files this should be the azure files connection details."""
        )
        print_help()
        exit(2)

    print("Mounting type: {}".format(sys.argv[1]))
    print("Mounting data: {}".format(sys.argv[2]))

    mount_point_permissions = 0o0777  # Todo: What permissions does this really need?
    primary_mount_folder = "/media/primarynfs"
    seconday_mount_folder_prefix = "/media/secondarynfs"
    fstab_file_path = "/etc/fstab"

    try:
        # Create folder to mount to
        if not os.path.exists(primary_mount_folder):
            os.makedirs(primary_mount_folder)
            os.chmod(primary_mount_folder, mount_point_permissions)

        # Make a backup of the fstab config incase we go wrong
        shutil.copy(fstab_file_path, "/etc/fstab-mountscriptbackup")

        # Clear existing NFS mount data to make script idempotent
        remove_lines_containing(fstab_file_path, primary_mount_folder)
        remove_lines_containing(fstab_file_path, seconday_mount_folder_prefix)

        if mount_type.lower() == "azurefiles":
            mount_azurefiles(fstab_file_path, mount_data, primary_mount_folder)

        if mount_type.lower() == "nfs":
            mount_nfs(fstab_file_path, mount_data, primary_mount_folder, mount_point_permissions)

    except IOError as e:
        print("I/O error({0})".format(e))
        exit(1)
    except:
        print("Unexpected error:{0}".format, sys.exc_info())
        raise

    print("Done editing fstab ... attempting mount")

    def mount_all():
        subprocess.check_call(["mount", "-a"])

    retryFunc("mount shares", mount_all, 100)

def retryFunc(desc, funcToRetry, maxRetries):
    # Retry mounting for a while to handle race where VM exists before storage
    # or temporary issue with storage
    print("Attempting, with retries, to: {}".format(desc))
    retryExponentialFactor = 3
    for i in range(1, maxRetries):
        if i == maxRetries:
            print("Failed after max retries")
            exit(3)

        try:
            print("Attempt #{}".format(str(i)))
            funcToRetry()
        except subprocess.CalledProcessError as e:
            print("Failed:{0}".format(e))
            retry_in = i * retryExponentialFactor
            print("retrying in {0}secs".format(retry_in))
            time.sleep(retry_in)
            continue
        else:
            print("Succeeded to: {0} after {1} retries".format(desc, i))
            break


def mount_nfs(fstab_file_path, mount_data, primary_mount_folder, mount_point_permissions):
    # # Other apt instances on the machine may be doing an install 
    # # this means ours will fail so we retry to ensure success
    def install_nfs():
        install_apt_package("nfs-common")

    retryFunc("install nfs-common", install_nfs, 20)

    ips = mount_data.split(",")
    print("Found ips:{}".format(",".join(ips)))

    # Deterministically select a primary node from the available
    # servers for this vm to use. By using the ip as a seed this ensures
    # re-running will get the same node as primary.
    # This enables spreading the load across multiple storage servers in a cluster
    # like `Avere` or `Gluster` for higher throughput.
    current_ip = get_ip_address()
    current_ip_int = ip_as_int(current_ip)
    print("Using ip as int: {0} for random seed".format((current_ip_int)))
    random.seed(current_ip_int)
    random_node = random.randint(0, len(ips) - 1)

    primary = ips[random_node]
    ips.remove(primary)
    secondarys = ips

    print("Primary node selected: {}".format(primary))
    print("Secondary nodes selected: {}".format(",".join(secondarys)))

    with open(fstab_file_path, "a") as file:

        print("Mounting primary")
        file.write(
            "\n{} {} {}".format(
                primary.strip(), primary_mount_folder, default_mount_options_nfs
            )
        )

        print("Mounting secondarys")
        number = 0
        for ip in secondarys:
            number = number + 1
            folder = "/media/secondarynfs" + str(number)
            if not os.path.exists(folder):
                os.makedirs(folder)
                os.chmod(folder, mount_point_permissions)

            file.write(
                "\n{} {} {}".format(ip.strip(), folder, default_mount_options_nfs)
            )


def mount_azurefiles(fstab_file_path, mount_data, primary_mount_folder):
    # Other apt instances on the machine may be doing an install 
    # this means ours will fail so we retry to ensure success
    def install_cifs():
         install_apt_package("cifs-utils")

    retryFunc("install cifs-utils", install_cifs, 20)

    params = mount_data.split(",")
    if len(params) != 3:
        print("Wrong params for azure files mount, expected 3 as CSV")
        print_help()
        exit(1)

    account_name = params[0]
    share_name = params[1]
    account_key = params[2]

    with open(fstab_file_path, "a") as file:
        print("Mounting primary")
        file.write(
            "\n//{0}.file.core.windows.net/{1} {2} cifs username={0},password={3},{4}".format(
                account_name,
                share_name,
                primary_mount_folder,
                account_key,
                default_mount_options_cifs,
            )
        )


if __name__ == "__main__":
    main()

