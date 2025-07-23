#!/usr/bin/env python
import base64
import datetime
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
HOST = {}
SERVER = None
TIMEOUT = 60


@app.route('/secure')
def secure():
    """Retrieve cookies and writes them in a session file"""
    # Retrieving cookies from query params
    global HOST
    cookies = {}
    session_info = {'host': HOST['url'], 'attributes': {'cookies': cookies}}
    for key in request.args.keys():
        if key != 'user' and key != 'token':
            cookies[key] = request.args.get(key)
        else:
            session_info[key] = request.args.get(key)

    session_info["login"] = time.strftime("%Y-%m-%d %H:%M:%S")
    session_info["expirationTime"] = get_jwt_expiration(session_info.get('token'))

    # Create session file with cookies
    create_session_file(HOST['name'], session_info)

    # Stopping server
    global SERVER
    p = Process(target=kill_process, args=(SERVER.pid,))
    p.start()

    return '<p>You are <b>logged in</b>. You can now close this tab.</p>'


def get_jwt_expiration(jwt_token):
    # JWT format: header.payload.signature
    try:
        payload_b64 = jwt_token.split('.')[1]
        # Add padding if necessary
        padding = '=' * (-len(payload_b64) % 4)
        payload_b64 += padding
        payload_json = base64.urlsafe_b64decode(payload_b64)
        payload = json.loads(payload_json)
        exp = payload.get('exp')
        if exp is None:
            return None
        dt = datetime.datetime.fromtimestamp(exp)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def create_session_file(hostname, session_info):
    """Create session file with cookies"""
    out_dir = os.path.join(os.path.expanduser('~'), '.opencga')
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, hostname + '_session.json'), 'w') as out_fpath:
        json.dump(session_info, out_fpath, indent=4)


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
            host = hosts[0]
        else:
            if host_name:
                for h in hosts:
                    if h['name'] == host_name:
                        host = h
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

    global HOST
    # Getting host
    if not host_url:
        HOST = get_host(client_config_file, host_name)
    else:
        HOST = {'name': 'opencga', 'url': host_url}

    # Disabling Flask startup warnings for not using a WSGI server
    sys.modules['flask.cli'].show_server_banner = lambda *_: None
    logging.getLogger('werkzeug').setLevel(logging.ERROR)

    # Starting server
    global SERVER
    SERVER = Process(target=app.run)
    sys.stdout.write('You have now {} seconds to authenticate\n'.format(TIMEOUT))
    SERVER.start()

    # Opening browser
    url = HOST['url'].strip('/') + '/webservices/rest/v2/meta/sso/login?url=http://localhost:5000/secure'
    webbrowser.open(url, new=2)

    # Wait for some time and then kill the server
    SERVER.join(TIMEOUT)
    if SERVER.is_alive():
        SERVER.kill()


if __name__ == '__main__':
    sys.exit(main())
