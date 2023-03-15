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
    cookies = {}
    for key in request.args.keys():
        cookies[key] = request.args.get(key)
    print(cookies)

    p = Process(target=kill_execution, args=(server.pid,))
    p.start()

    return render_template('successful.html')


server = Process(target=app.run)
server.start()