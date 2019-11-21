import unittest
import tempfile
from mount import mount_nfs, mount_azurefiles, get_ip_address, ip_as_int

example_fstab_content = """# /etc/fstab: static file system information.
#
# Use 'blkid' to print the universally unique identifier for a
# device; this may be used with UUID= as a more robust way to name devices
# that works even if disks are added and removed. See fstab(5).
#
# <file system> <mount point>   <type>  <options>       <dump>  <pass>
/dev/mapper/sda4_crypt /               ext4    errors=remount-ro 0       1
# /boot was on /dev/sda3 during installation
UUID=abbf117a-b9c3-4459-b5f0-98467ccbf52c /boot           ext4    defaults        0       2
# /boot/efi was on /dev/sda1 during installation
UUID=45E3-17ED  /boot/efi       vfat    umask=0077      0       1
/swapfile                                 none            swap    sw              0       0
//nas/windowsbackup  /media/windowsbackup  cifs  username=shares,password=something  0  0"""

example_fstab_length = 14
class TestAvere(unittest.TestCase):
    def test_get_ip(self):
        res = get_ip_address()
        self.assertIsNotNone(res)

    
    def test_ip2int(self):
        res = ip_as_int("10.0.1.174")
        self.assertEqual(res, 167772590)

    def test_nfs_end2end(self):
        expected_fstab_content = example_fstab_content + """
server:/opencga /media/primarynfs nfs hard,nointr,proto=tcp,mountproto=tcp,retry=30 0 0
server:/opencga /media/secondarynfs1 nfs hard,nointr,proto=tcp,mountproto=tcp,retry=30 0 0
server:/opencga /media/secondarynfs2 nfs hard,nointr,proto=tcp,mountproto=tcp,retry=30 0 0"""

        file, file_path = tempfile.mkstemp(prefix="nfsmounttest_")
        
        with open(file, "w") as fw:
            fw.write(example_fstab_content)
        
        mount_nfs(file_path, "server:/opencga, server:/opencga , server:/opencga", "/media/primarynfs", 777)
        with open(file_path, 'r') as fr:
            content = fr.read()
            self.assertMultiLineEqual(content, expected_fstab_content)

    def test_azurefiles_end2end(self):
        expected_fstab_content = example_fstab_content + """
//storage-account-name.file.core.windows.net/share-name /media/primarynfs cifs username=storage-account-name,password=storage-account-key,dir_mode=0777,file_mode=0777,serverino,nofail,uid=1001,gid=1001,vers=3.0"""

        file, file_path = tempfile.mkstemp(prefix="nfsmounttest_")
        
        with open(file, "w") as fw:
            fw.write(example_fstab_content)
        
        mount_azurefiles(file_path, "storage-account-name,share-name,storage-account-key", "/media/primarynfs")
        with open(file_path, 'r') as fr:
            content = fr.read()
            self.assertMultiLineEqual(content, expected_fstab_content)