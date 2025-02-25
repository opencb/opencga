#!/usr/bin/env python

import json
import os
import sys
import signal
import logging
import time
import webbrowser
import yaml
import argparse
from flask import Flask, request
from multiprocessing import Process


app = Flask(__name__)
app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'
SERVER = None
TIMEOUT = 60


@app.route('/secure')
def secure():
    """Retrieve cookies and writes them in a session file"""
    # Retrieving cookies from query params
    cookies = {}
    session_info = {'cookies': cookies}
    for key in request.args.keys():
        if key != 'user' and key != 'token':
            cookies[key] = request.args.get(key)
        else:
            session_info[key] = request.args.get(key)
    # Create session file with cookies
    create_session_file(session_info)

    # Stopping server
    global SERVER
    p = Process(target=kill_process, args=(SERVER.pid,))
    p.start()

    return '<p>You are <b>logged in</b>. You can now close this tab.</p>'


def create_session_file(session_info):
    """Create session file with cookies"""
    out_dir = os.path.join(os.path.expanduser('~'), '.opencga')
    os.makedirs(out_dir, exist_ok=True)
    out_fpath = open(os.path.join(out_dir, 'session.json'), 'w')
    out_fpath.write(json.dumps(session_info))
    out_fpath.close()


def kill_process(pid):
    """Kill process by ID"""
    time.sleep(0.1)      # Sleep 0.1 seconds so the server has time to give the response
    os.kill(pid, signal.SIGKILL)


def get_host(config_fpath, host_name):
    # Reading config file
    config_dict = None
    config_fhand = open(config_fpath, 'r')
    if config_fpath.endswith('.yml') or config_fpath.endswith('.yaml'):
        config_dict = yaml.safe_load(config_fhand)
    elif config_fpath.endswith('.json'):
        config_dict = json.loads(config_fhand.read())

    # Getting hosts
    hosts = None
    if config_dict:
        if 'rest' in config_dict and config_dict['rest']:
            if 'hosts' in config_dict['rest'] and config_dict['rest']['hosts']:
                hosts = config_dict['rest']['hosts']

    # Getting a particular host
    host = None
    if hosts:
        if len(hosts) == 1:
            host = hosts[0]['url']
        else:
            if host_name:
                for h in hosts:
                    if h['name'] == host_name:
                        host = h['url']
                if not host:
                    msg = 'Host "{}" not found in "{}".'
                    raise ValueError(msg.format(host_name, config_fpath))
            else:
                msg = 'Multiple hosts found in "{}". Please use "--host_name" to specify one.'
                raise ValueError(msg.format(config_fpath))

    return host


def parse_arguments():
    """Parse input arguments"""
    desc = 'This script creates a "~/.opencga/session.json" file with OpenCGA session info'
    parser = argparse.ArgumentParser(description=desc,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--client_config_file', help='Client configuration file')
    parser.add_argument('--host_name',
                        help='Host name; required when multiple hosts exist in client config file')
    parser.add_argument('--host_url',
                        help='Host URL; overrides client configuration file info;'
                             ' e.g. "https://demo.app.zettagenomics.com/opencga"')
    args = parser.parse_args()
    return args


def main():
    # Getting input arguments
    args = parse_arguments()
    client_config_file = args.client_config_file
    host_name = args.host_name
    host_url = args.host_url
    if client_config_file is None and host_url is None:
        msg = 'Please, use "--client_config_file" or "--host_url" to specify the OpenCGA host'
        raise ValueError(msg)

    # Getting host
    if not host_url:
        host = get_host(client_config_file, host_name)
    else:
        host = host_url

    # Disabling Flask startup warnings for not using a WSGI server
    sys.modules['flask.cli'].show_server_banner = lambda *_: None
    logging.getLogger('werkzeug').setLevel(logging.ERROR)

    # Starting server
    global SERVER
    SERVER = Process(target=app.run)
    sys.stdout.write('You have now {} seconds to authenticate\n'.format(TIMEOUT))
    SERVER.start()

    # Opening browser
    url = host.strip('/') + '/webservices/rest/v2/meta/sso/login?url=http://localhost:5000/secure'
    webbrowser.open(url, new=2)

    # Wait for some time and then kill the server
    SERVER.join(TIMEOUT)
    if SERVER.is_alive():
        SERVER.kill()


if __name__ == '__main__':
    sys.exit(main())
