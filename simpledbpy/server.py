import socket
import threading
import time
from typing import Tuple
from simpledbpy.parser import Parser

from simpledbpy.simple_db import SimpleDB


simple_db = SimpleDB("simpledb")


def hello(clientsocket: socket.socket, address: Tuple[str, int]):
    planner = simple_db.planner()
    tx = simple_db.new_tx()
    end = False
    while True:
        try:
            buf = b""
            while b";" not in buf:
                tmp = clientsocket.recv(64)
                if tmp == b"":
                    end = True
                    break
                buf += tmp
            if end:
                break
            buf = buf[:buf.index(b";")]
            qry = buf.decode()
            print(qry)
            print(qry.split(None, 1)[0].lower())
            if qry.split(None, 1)[0].lower() == "select":
                print("select")
                p = planner.create_query_plan(qry, tx)
                parser = Parser(qry)
                data = parser.query()
                print(data.fields)
                s = p.open()
                while s.next():
                    print([f"{s.get_val(fld)}" for fld in data.fields])
                s.close()
            else:
                res = planner.execute_update(qry, tx)
                print(res)
        except Exception as e:
            print(e)
            # tx.rollback()
    clientsocket.close()


def main():
    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        serversocket.bind(("localhost", 1344))
        serversocket.listen(5)
        while True:
            clientsocket, address = serversocket.accept()
            ct = threading.Thread(target=hello, args=(clientsocket, address))
            ct.run()
    finally:
        serversocket.close()
