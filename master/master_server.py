from flask import Flask
import logging
import time
from membership_manager import MembershipManager
app = Flask(__name__)

logging.basicConfig(level=logging.DEBUG)


@app.route("/")
def hello():
    return "Hello World!"


class MasterServer():
    def __init__(self):
        app.debug = True
        self.membership_manager = MembershipManager()

    def start_server(self):
        self.membership_manager.start_membership_service()
        app.run()


if __name__ == "__main__":
    server = MasterServer()
    server.start_server()