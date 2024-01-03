import socket


def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("localhost", 1344))
    try:
        while True:
            s.send(input("> ").encode())
    finally:
        s.close()

"""
CREATE TABLE student (id INT, name VARCHAR(32));
INSERT INTO student(id, name) VALUES (1, 'hogetaro');
INSERT INTO student(id, name) VALUES (2, 'fugajiro');
SELECT id, name FROM student;
"""
