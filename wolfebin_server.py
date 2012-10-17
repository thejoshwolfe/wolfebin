#!/usr/bin/env python

__version__ = "5.0"

import os, sys
import struct, threading, time
import hashlib
import json
import SocketServer

import imp
sys.dont_write_bytecode = True
try:
    wolfebin_path = os.path.join(os.path.dirname(__file__), "wolfebin")
    with open(wolfebin_path) as f:
        Connection = imp.load_source("wolfebin", wolfebin_path, f).Connection
finally:
    sys.dont_write_bytecode = False

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
except IOError:
    sys.stderr.write("WARNING: initializing defaults in {}\n".format(config_path))
    write_json(config_path, config)

if config["version"] > current_config_version:
    sys.exit("ERROR: config is too new.")

data_root = config["data_root"]
database_path = os.path.join(data_root, "index.json")
file_data_dir = os.path.join(data_root, "files")
def check_database():
    if os.path.exists(data_root):
        return
    sys.stderr.write("WARNING: creating new database in {}\n".format(data_root))
    os.mkdir(data_root)
    os.mkdir(file_data_dir)
    save_database({})

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

def hash_to_file_name(key_hash):
    return os.path.join(file_data_dir, key_hash)
def key_to_hash(key):
    return hashlib.sha1(key).hexdigest()
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

def get(connection, request):
    key = request["key"]
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
        connection.write_json({
            "files": entry["files"],
        })
        def read_from_the_file(size):
            read_size = 0
            chunks = []
            while read_size < size:
                session_is_done = session == None or session.is_done
                chunk = file_handle.read(size - read_size)
                if len(chunk) != 0:
                    chunks.append(chunk)
                    read_size += len(chunk)
                    continue
                # no more to read. will there be in a moment?
                if session_is_done:
                    # no more to read ever.
                    break
                # streaming buffer underflow. pause a moment and try again.
                time.sleep(0.1)
            return "".join(chunks)

        for file_info in entry["files"]:
            file_size = file_info["size"]
            written_size = 0
            while written_size < file_size:
                chunk = read_from_the_file(min(config["chunk_size"], file_size - written_size))
                if len(chunk) == 0:
                    # incomplete
                    return
                connection.write_binary_chunk(chunk)
                written_size += len(chunk)
            checksum = read_from_the_file(len(hashlib.sha1().hexdigest()))
            connection.write_json({
                "sha1": checksum,
            })
    finally:
        file_handle.close()

def put(connection, request):
    key = request["key"]
    key_hash = key_to_hash(key)
    entry = {
        "files": [{
            "name": file_info["name"] + "",
            "size": file_info["size"] + 0,
        } for file_info in request["files"]],
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
        for file_info in entry["files"]:
            file_size = file_info["size"]
            thus_far = 0
            digester = hashlib.sha1()
            while thus_far < file_size:
                chunk = connection.read_binary_chunk()
                file_handle.write(chunk)
                digester.update(chunk)
                thus_far += len(chunk)
            # checksum
            checksum = digester.hexdigest()
            supposed_checksum = connection.read_json()["sha1"]
            if checksum == supposed_checksum:
                connection.write_json({})
            else:
                connection.write_warning("Checksum failed for {}".format(repr(file_info["name"])))
            file_handle.write(checksum)
    finally:
        file_handle.close()
        session.done()

def delete(connection, request):
    missing_keys = []
    with state_lock:
        for key in request["keys"]:
            key_hash = key_to_hash(key)
            database = get_database()
            try:
                del database[key]
            except KeyError:
                missing_keys.append(key)
                continue
            delete_file(key_hash)
            save_database(database)
    if len(missing_keys) != 0:
        connection.write_error("\n".join("key not found: " + repr(key) for key in missing_keys))
    else:
        connection.write_json({})

def list_keys(connection):
    database_items = get_database().items()
    connection.write_json({
        "version": __version__,
        "items": database_items,
    })

def server_forever():
    class ConnectionHandler(SocketServer.BaseRequestHandler):
        def handle(self):
            connection = Connection(self.request)
            request = connection.read_json()
            command = request["command"]
            if command == "get":
                get(connection, request)
            elif command == "put":
                put(connection, request)
            elif command == "delete":
                delete(connection, request)
            elif command == "list":
                list_keys(connection)
            else:
                connection.write_error("bad command: " + json.dumps(command))
    class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
        allow_reuse_address = True
    server = ThreadedTCPServer((config["host_name"], config["port_number"]), ConnectionHandler)
    server.serve_forever()

if __name__ == "__main__":
    check_database()
    server_forever()

