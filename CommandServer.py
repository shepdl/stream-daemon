from threading import Thread
import socket
import simplejson as json

from config import config

class CommandServer(Thread):
    def __init__(self, monitor, config):
        super(CommandServer, self).__init__(name="CommandServer")
        self.monitor = monitor
        self.project_list = monitor.project_list
        self.running = True

    def run(self):
        sock = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM
        )
        sock.bind(("localhost", config["server_socket_port"],))
        sock.listen(1)
        while self.running:
            connection, client_addr = sock.accept()
            message = ""
            try:
                message = connection.recv(4096)
                # while True:
                #    packet = connection.recv(4096)
                #    if packet:
                #        print packet
                #        message += packet
                #    else:
                #        break
                print "Received command. Whole message: %s" % (message,)
                response = json.dumps(self.dispatch(message))
                connection.sendall(response)
            finally:
                connection.close()
        print "Shutting down CommandServer thread: falling off end of run() ..."
    
    def stop(self):
        self.running = False

    def dispatch(self, message):
        name = message.split(" ")[0] 
        args = message.split(" ")[1:]
        dispatch_table = {
            "add-project" : self.add_project,
            "stop-project" : self.stop_project,
            "list-projects" : self.list_projects,
            "shutdown" : self.shutdown,
        }
        if name in dispatch_table:
            return dispatch_table[name](*args)
        else:
            return {"message" : "Invalid command"}


    # add project {name} {tags}
    def add_project(self, name, tags):
        (project, id,) = self.project_list.create(name, tags)
        return {
            "message" : "Project created",
            "name" : project.name,
            "id" : project.id,
        }

    # stop project {name}
    def stop_project(self, id):
        # TODO: commit data
        return self.project_list.stop(id)

    # list projects
    def list_projects(self):
        projects = []
        for project in self.project_list.active_projects.itervalues():
            projects.append({
                "key" : project.id,
                "name" : project.name,
                "keywords" : project.watching
            })
        return projects

    # shutdown
    def shutdown(self):
        # send monitor command to shut down
        self.monitor.stop()
        self.stop()
        return {
            "message" : "Stopped",
        }