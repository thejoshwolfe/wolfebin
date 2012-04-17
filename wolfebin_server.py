#!/usr/bin/env python

__version__ = "5.0"

import os, sys
import struct, threading, time
import hashlib
import json
import SocketServer

config_path = "config.json"
current_config_version = 0
config = {
    "version": current_config_version,
    "host_name": "localhost",
    "port_number": 55247,
    "chunk_size": 0x1000,
    "data_root": "wolfebin_data",
}
def read_json(path):
    with open(path) as f:
        return json.loads(f.read())
def write_json(path, json_object):
    with open(path, "w") as f:
        f.write(json.dumps(json_object, sort_keys=True, indent=4))
        f.write("\n")
try:
    config = read_json(config_path)
    if config["version"] > current_config_version:
        sys.exit("ERROR: config is too new.")
except IOError:
    sys.stderr.write("WARNING: initializing defaults in {}\n".format(config_path))
    write_json(config_path, config)

try:
    from wolfebin_config import *
except ImportError:
    pass

data_root = config["data_root"]
database_path = os.path.join(data_root, "index.json")
file_data_dir = os.path.join(data_root, "files")
def check_database():
    if os.path.exists(data_root):
        return
    sys.stderr.write("WARNING: creating new database in {}\n".format(data_root))
    os.mkdir(data_root)
    os.mkdir(file_data_dir)
    database = {}
    save_database(database)

state_lock = threading.RLock()

active_sessions = {}
class Session:
    def __init__(self, key):
        # should have a lock
        self.key = key
        self.is_done = False
        active_sessions[key] = self
    def done(self):
        self.is_done = True
        with state_lock:
            if active_sessions.get(self.key, None) == self:
                del active_sessions[self.key]
def find_session(key):
    # should have a lock already
    return active_sessions.get(key, None)

class Connection:
    def __init__(self, actual_connection):
        self.connection = actual_connection
    def read(self, length):
        chunks = []
        while length != 0:
            chunk = self.connection.recv(length)
            if len(chunk) == 0:
                break
            chunks.append(chunk)
            length -= len(chunk)
        return "".join(chunks)
    def read_fmt(self, fmt):
        data = self.read(struct.calcsize(fmt))
        return struct.unpack(fmt, data)[0]
    def read_int(self):
        return self.read_fmt(">I")
    def read_long(self):
        return self.read_fmt(">Q")
    def read_string(self):
        length = self.read_int()
        return self.read(length)

    def write(self, data):
        self.connection.sendall(data)
    def write_fmt(self, fmt, value):
        data = struct.pack(fmt, value)
        self.write(data)
    def write_int(self, value):
        self.write_fmt(">I", value)
    def write_long(self, value):
        self.write_fmt(">Q", value)
    def write_string(self, value):
        self.write_int(len(value))
        self.write(value)
    def write_error(self, message):
        self.write("b") # 'b' for "bad"
        self.write_string(message)

    def ok(self):
        self.write("w") # 'w' for "wolfebin"

def hash_to_file_name(key_hash):
    return os.path.join(file_data_dir, key_hash)
def key_to_hash(key):
    return hashlib.md5(key).hexdigest()
def open_file(key_hash, mode):
    return open(hash_to_file_name(key_hash), mode)
def delete_file(key_hash):
    os.remove(hash_to_file_name(key_hash))

def get_database():
    # lock the database in case the caller didn't
    return read_json(database_path)
def save_database(database):
    # caller should have a database lock
    write_json(database_path, database)

def get(connection, protocol):
    key = connection.read_string()
    key_hash = key_to_hash(key)
    try:
        with state_lock:
            entry = get_database()[key]
            session = find_session(key)
            file_handle = open_file(key_hash, "rb")
    except KeyError:
        connection.write_error("key not found: " + repr(key))
        return
    try:
        connection.ok()
        # header
        connection.write_int(len(entry["files"]))
        for file_entry in entry["files"]:
            connection.write_string(file_entry["name"])
            connection.write_long(file_entry["size"])
        while True:
            session_is_done = session == None or session.is_done
            chunk = file_handle.read(config["chunk_size"])
            if len(chunk) != 0:
                connection.write(chunk)
                continue
            # no more to read. will there be in a moment?
            if session_is_done:
                # no more to read ever.
                break
            # streaming buffer underflow. pause a moment and try again.
            time.sleep(0.1)
    finally:
        file_handle.close()

def put(connection):
    key = connection.read_string()
    connection.ok()
    key_hash = key_to_hash(key)
    file_count = connection.read_int()
    entry = {
        "files": [
            {
                "name": connection.read_string(),
                "size": connection.read_long()
            }
        for _ in range(file_count)],
    }
    with state_lock:
        database = get_database()
        database[key] = entry
        save_database(database)
        # prevent two open write handles to the same file
        try:
            delete_file(key_hash)
        except OSError:
            pass
        file_handle = open_file(key_hash, "wb")
        session = Session(key)
    try:
        for file_entry in entry["files"]:
            file_size = file_entry["size"]
            thus_far = 0
            digester = hashlib.md5()
            while thus_far < file_size:
                read_size = min(config["chunk_size"], file_size - thus_far)
                chunk = connection.read(read_size)
                if len(chunk) == 0:
                    return # incomplete upload
                file_handle.write(chunk)
                digester.update(chunk)
                thus_far += len(chunk)
            # check the md5
            md5sum = digester.hexdigest()
            supposed_md5sum = connection.read(len(md5sum))
            if md5sum == supposed_md5sum:
                connection.ok()
            else:
                connection.write_error("")
            file_handle.write(md5sum)
    finally:
        file_handle.close()
        session.done()

def delete(connection):
    key = connection.read_string()
    key_hash = key_to_hash(key)
    try:
        with state_lock:
            database = get_database()
            del database[key]
            delete_file(key_hash)
            save_database(database)
    except KeyError:
        connection.write_error("key not found: " + repr(key))
        return
    connection.ok()

def list_keys(connection):
    database_items = get_database().items()
    connection.ok()
    connection.write_string(__version__)
    connection.write_int(len(database_items))
    for (key, entry) in database_items:
        connection.write_string(key)
        connection.write_int(len(entry["files"]))
        for file_entry in entry["files"]:
            connection.write_string(file_entry["name"])
            connection.write_long(file_entry["size"])

def server_forever():
    class ConnectionHandler(SocketServer.BaseRequestHandler):
        def handle(self):
            connection = Connection(self.request)
            command = connection.read(1)
            if command == "g":
                get(connection, command)
            elif command == "p":
                put(connection)
            elif command == "d":
                delete(connection)
            elif command == "l":
                list_keys(connection)
            else:
                sys.stderr.write("bad command: " + repr(command) + "\n")
                connection.write("you suck")
    class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
        allow_reuse_address = True
    server = ThreadedTCPServer((config["host_name"], config["port_number"]), ConnectionHandler)
    server.serve_forever()

if __name__ == "__main__":
    check_database()
    server_forever()

