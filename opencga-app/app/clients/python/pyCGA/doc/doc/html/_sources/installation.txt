Installation
============

Here is a step by step guide on how to install pyCGA. It will get you the library ready to be load from a python script and all the scripts we provide installed in your system.

First, obtain Python if you do not already have it. Then you will ned to install pip_

.. _pip: https://pip.pypa.io/en/latest/installing.html

Once you have these you will be able to install the requirements

Requirements
------------
+------------+------------+
| Packages   | Version    |
+============+============+
| requests   |    2.7     |
+------------+------------+

Although all the packages, The dependencies are specified in the file pyCGA/pip_requirements.txt
You can install them using::

    cd pyCGA
    pip install -r pip_requirements.txt

Install
-------
Next step to complete the installation is execute the file setup.py, you can use::

    sudo python setup.py install

or::

    sudo pip install .

.. note::

    If you are using pip and you would like to reinstall the packages you should use::

        sudo pip install . --upgrade

