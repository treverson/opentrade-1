#!/usr/bin/env python

from ws4py.client.threadedclient import WebSocketClient
import json


class DummyClient(WebSocketClient):

  def opened(self):
    print('Opened up')

  def closed(self, code, reason=None):
    print('Closed down', code, reason)

  def received_message(self, m):
    print(m)
    print('')


addr = 'ws://127.0.0.1:9111/ot/'


def openWs():
  ws = DummyClient(addr, protocols=['http-only'])
  ws.connect()
  return ws


def login():
  login = ['login', "admin", 'test']
  ws = openWs()
  ws.send(json.dumps(login))
  return ws


if __name__ == '__main__':
  try:
    ws = login()
    ws.send(json.dumps(['shutdown', 3, 1]))
    ws.run_forever()
  except KeyboardInterrupt:
    ws.close()
