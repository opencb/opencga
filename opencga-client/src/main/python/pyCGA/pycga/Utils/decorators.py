import getpass
import json
import os

from pyCGA.opencgarestclients import OpenCGAClient


def catalog_login(func):
    def log_method(sid=None, configuration=None, *args, **kwargs):
        opencga_dir = os.path.join(os.getenv("HOME"), ".opencga")

        if not os.path.exists(opencga_dir):
            os.system("mkdir " + opencga_dir)
        if not configuration:
            if os.path.exists(os.path.join(opencga_dir, 'opencga_configuration.json')):
                configuration = json.load(open(os.path.join(opencga_dir, 'opencga_configuration.json')))
            else:
                configuration = create_configuration_file(opencga_dir)
        else:
            configuration = create_configuration_file(opencga_dir, configuration)

        if not sid:
            if os.path.exists(os.path.join(opencga_dir, 'session_python.json')):
                try:
                    session = json.load(open(os.path.join(opencga_dir, 'session_python.json')))
                    sid = session['sessionId']
                except ValueError:
                    create_session_file(configuration, opencga_dir)

            else:
                sid = create_session_file(configuration, opencga_dir)
        return func(sid=sid, configuration=configuration, *args, **kwargs)

    def create_session_file(configuration, opencga_dir):
        fdw = open(os.path.join(opencga_dir, 'session_python.json'), 'w')
        user = raw_input("User: ")
        pwd = getpass.getpass()
        o = OpenCGAClient(configuration=configuration, user=user, pwd=pwd)
        session = {'userId': user, 'sessionId': o.session_id}
        json.dump(session, fdw)
        fdw.close()
        sid = session['sessionId']
        return sid

    def create_configuration_file(opencga_dir, configuration=None):

        fdw = open(os.path.join(opencga_dir, 'opencga_configuration.json'), 'w')
        if not configuration:
            host = raw_input("Please provide OpenCGA Host name: ")
            conf_json = {'version': 'v1', 'rest': {'hosts': [host]}}
            json.dump(conf_json, fdw)
        if isinstance(configuration, dict):
            conf_json = configuration
        else:
            conf_json = json.load(open(configuration, 'r'))
        json.dump(conf_json, fdw)

        fdw.close()
        return conf_json

    return log_method
