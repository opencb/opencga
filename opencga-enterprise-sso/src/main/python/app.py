import os
import signal
import time
import webbrowser
from multiprocessing import Process

from flask import Flask, render_template, request

app = Flask(__name__)
app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'


def kill_execution(pid):
    time.sleep(0.1)
    os.kill(pid, signal.SIGKILL)


@app.route("/secure")
def secure():
    # cookies = {}
    for key in request.args.keys():
        print(key + " - " + request.args.get(key))
        # cookies[key] = request.args.get(key)

    # headers = {'User-Agent': 'Mozilla/5.0'}
    # response = requests.get('http://localhost:9090/opencga/webservices/rest/v2/meta/about', cookies=cookies, headers=headers)
    # webpage = response.text
    # print(webpage)

    p = Process(target=kill_execution, args=(server.pid,))
    p.start()

    return render_template('successful.html')


server = Process(target=app.run)
server.start()

webbrowser.open('http://localhost:9090/opencga/webservices/rest/v2/meta/sso?url=http://localhost:5000/secure')