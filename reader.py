# IDEA: harvest.py?

import sys
import socket
import simplejson as json

from config import config

def add_project(data):
    return """
        Project %(name)s created with ID %(id)s.
    """ % data

def stop_project(data):
    if "error" in data:
        return data["error"]
    return """
        Project %(name)s stopped.
    """ % data

def list_projects(data):
    def _format_project(p):
        return """ %(name)s\t %(key)s %(keywords)s """ % {
            "name" : p["name"],
            "key" : p["key"],
            "keywords" : (", ".join(p["keywords"]))[0:30],
        }

    if len(data) > 0:
        message = """ Name\tKey\t\tSome Keywords ... \n"""
        message += "\n".join(map(_format_project, data))
        return message
    else:
        return """No active projects.\n"""


def shutdown(data):
    return data["message"]

dispatch_table = {
    "add-project" : add_project,
    "stop-project" : stop_project,
    "list-projects" : list_projects,
    "shutdown" : shutdown,
}

if len(sys.argv) == 1 or sys.argv[1] not in dispatch_table:
    print """Usage: 

add-project "Name" "tag1,tag2,tag3" -- add a project
stop-project "id" -- stop recording data for a project 
list-projects -- list all active projects
shutdown -- stop the daemon
    """
    sys.exit()

command = " ".join(sys.argv[1:])

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Connect the socket to the port where the server is listening
server_address = ('localhost', config["server_socket_port"])
sock.connect(server_address)

try:
    sock.sendall(command)
    data = ""
    while True:
        packet = sock.recv(1024)
        if not packet:
            break
        data += packet
    # print data
    print dispatch_table[sys.argv[1]](json.loads(data))
finally:
    sock.close()
