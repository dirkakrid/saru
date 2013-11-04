saru
====

Simple And Reliable UDP for Python


```python
# send
sock=saru.socket()
sock.sendto('Hi', ('127.0.0.1', 8111))

# recv
sock=saru.socket()
sock.bind(('127.0.0.1', 8111))
print sock.recvfrom()
```
