# Users and Groups

## Users in OpenCGA

Any study has intrinsically defined three different membership levels or user groups:

### Owner <a id="SharingandPermissions-Owner"></a>

The owner of an study is the user that creates the study. Because of that an study will always have one and only one owner, who will be able to perform any action over the data contained in the study. There are two actions that are only possible for the owner user: _**delete the study**_ and _**assign or remove users to/from admins**_ **groups** _\(see next section\)._

### Administrative groups <a id="SharingandPermissions-Administrativegroups"></a>

OpenCGA defines two reserved groups that will have some special behaviour.

#### Admins <a id="SharingandPermissions-Admins"></a>

Every _Study_ in OpenCGA contains a special group called _**admins**_. This group will contain a list of users that will be able to do most of the administrative work the owner might want other users to do. Users belonging to this group will be able to perform almost any action except for the two ones that are only allowed for the owner of the study. Special operations that only these users will be able to perform are _**create/update/delete groups of** **users**_, _**create/update/delete variable sets**_ and _**assign/remove permissions to other users/groups.**_

#### Members <a id="SharingandPermissions-Members"></a>

Apart from _admins,_ there is also an special group called _members_. Any user with any kind of granted access to the study will automatically belong to this group. The main aim of this group is to keep track of the users with any access to the study, but it also has other advantages such as:

* The _admin_ users might want to predefined some permissions any _member_ of a study will have. In such a case, _admin_ users will just add new users to that group and those users will automatically be granted the permissions the group has.
* If an _admin_ user wants to completely revoke any permission to one user, by removing that user from the _members_ group, OpenCGA will automatically search for any permissions set for that user in any entity and remove it.

### Decision Algorithm

The next schema provides a visual explanation of the algorithm implemented in Catalog for deciding whether the user has or not access to the data in the context of a study.

![Decision Algorithm for granting permissions ](../../../.gitbook/assets/image%20%282%29.png)

There are two circumstances under which the algorithm behaves as follows:

* If the user and any of the groups where the user belongs to have permissions defined for one entry, the permissions that will be actually used will be the user's.
* In case the user belongs to more than one group and those groups are assigned different permissions for one concrete entry, the effective permissions that will be used will be the union of the permissions found in all those groups.

