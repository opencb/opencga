import sys
import socket
import fcntl
import struct
import random
import os
import shutil

default_mount_options_nfs = "nfs hard,nointr,proto=tcp,mountproto=tcp,retry=30 0 0"


def get_avere_ips(vserver_string):
    vserver_range = vserver_string.split("-")
    if len(vserver_range) < 2:
        print("Expect vserver ip range to be in the format 'startip-endip' got:" + vserver_string)
        exit(3)

    all_ips = []
    start_ip = vserver_range[0]
    end_ip = vserver_range[1]

    start_ip_parts = start_ip.split(".")
    start_ip_last_digit = int(start_ip_parts[3])
    ip_prefix = start_ip_parts[0] + "." + start_ip_parts[1] + "." + start_ip_parts[2] + "."
    end_ip_last_digit = int(end_ip.split(".")[3])

    for ip in range(start_ip_last_digit, end_ip_last_digit + 1): 
        all_ips.append(ip_prefix + str(ip))
    
    return all_ips

def get_ip_address():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

def ip_as_int(ip):
    o = map(int, ip.split('.'))
    res = (16777216 * o[0]) + (65536 * o[1]) + (256 * o[2]) + o[3]
    return res

def main():  
    if len(sys.argv) < 3:
        print "Expected arg1: 'mount_type' and arg2 'mount_data'"
        print "For example 'sudo python mount.py avere 10.0.1.10-10.0.1.14'"
        exit(1)

    mount_type = sys.argv[1]
    mount_data = sys.argv[2]

    if str(mount_type).lower() != "avere" and str(mount_type).lower() != "azurefiles": 
        print "Expected first arg to be either 'avere' or 'azurefiles'"
        print "For example 'sudo python mount.py avere 10.0.1.10-10.0.1.14'"
        exit(1)

    if str(mount_data) == "":
        print "Expected second arg to be the mounting data. For avere this is the vserver iprange. Fo azure files this should be the azure files connection details."
        print "For example 'sudo python mount.py avere 10.0.1.10-10.0.1.14'"
        exit(2)


    print 'Mounting type:' + sys.argv[1]
    print 'Mounting data:' + sys.argv[2]
    try:
        if str(mount_type).lower() == "avere":
            ips = get_avere_ips(mount_data)
            print "Found ips:" + ",".join(ips)

            
            # Deterministically select a primary node from the available
            # servers for this vm to use. By using the ip as a seed this ensures
            # re-running will get the same node as primary
            current_ip = get_ip_address()
            current_ip_int = ip_as_int(current_ip)
            print "Using ip as int: {0} for random seed".format(current_ip_int)
            random.seed(current_ip_int)
            random_node = random.randint(0, len(ips))

            primary = ips[random_node]
            ips.remove(primary)
            secondarys = ips

            print "Primary node selected:" + primary
            print "Secondary nodes selected:" + ",".join(secondarys)

            shutil.copy("/etc/fstab", "/etc/fstab-averescriptbackup")

            # Remove existing entried in the fstab file for avere
            with open("/etc/fstab","r+") as file:
                d = file.readlines()
                file.seek(0)
                for i in d:
                    if "avere" not in i:
                        file.write(i)
                file.truncate()

            with open('/etc/fstab', 'a') as file:

                print "Mounting primary"

                primary_mount_folder = "/media/avere/primary"
                if not os.path.exists(primary_mount_folder):
                    os.makedirs(primary_mount_folder)
                    os.chmod(primary_mount_folder, 0o0777) #Todo: What permissions does this really need?

                file.write("\n"+ primary +":/msazure"+ primary_mount_folder + " "+ default_mount_options_nfs + "\n")
                
                print "Mounting secondarys"

                number = 0
                for ip in secondarys:
                    number = number+1
                    folder = "/media/avere/secondary" + str(number)
                    if not os.path.exists(folder):
                        os.makedirs(folder)
                        os.chmod(folder, 0o0777) #Todo: What permissions does this really need?
                    
                    file.write("\n"+ ip +":/msazure"+ folder + " "+ default_mount_options_nfs + "\n")
    except IOError as (errno, strerror):
        print "I/O error({0}): {1}".format(errno, strerror)
        exit(1)
    except ValueError:
        print "Could not convert data to an integer."
        exit(1)
    except:
        print "Unexpected error:", sys.exc_info()[0]
        exit(1)

    print "Done"

if __name__== "__main__":
  main()
