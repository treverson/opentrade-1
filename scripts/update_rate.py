#!/usr/bin/env python

import pg8000

types = ('STK', 'CASH', 'CMDTY', 'FUT', 'OPT', 'IND', 'FOP', 'WAR', 'BOND',
         'FUND')


def main():
  conn = pg8000.connect(
      host='127.0.0.1',
      database='opentrade',
      user='postgres',
      password='test')

  cursor = conn.cursor()
  exchanges = {}
  cursor.execute(
      'select symbol, close_price from security where "type"=\'CASH\'')
  fx = {}
  for m in cursor.fetchall():
    if not m[0].startswith('USD'): continue
    cur = m[0][3:]
    val = 1 / m[1]
    cursor.execute('update security set rate=%s where currency=%s',
                   (val, cur))
  conn.commit()


if __name__ == '__main__':
  main()
