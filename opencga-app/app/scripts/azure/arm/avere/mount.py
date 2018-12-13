import sys

def main():  
    if len(sys.argv) < 3:
        print("Expected arg1: 'mount_type' and arg2 'mount_data'")
        exit(1)

    mount_type = sys.argv[1]
    mount_data = sys.argv[2]

    if str(mount_type).lower() != "avere" and str(mount_type).lower() != "azurefiles": 
        print("Expected first arg to be either 'avere' or 'azurefiles'")
        exit(1)

    if str(mount_data) == "":
        print("Expected second arg to be the mounting data. For avere this is the vserver iprange. Fo azure files this should be the azure files connection details.")
        exit(2)


    print('Mounting type:', sys.argv[1])
    print('Mounting data:', sys.argv[2])

    print(",".join(get_avere_ips(mount_data)))
  
if __name__== "__main__":
  main()



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
