#!/usr/bin/env python

import optparse
import pg8000

types = ('STK', 'CASH', 'CMDTY', 'FUT', 'OPT', 'IND', 'FOP', 'WAR', 'BOND',
         'FUND')


def main():
  opts = optparse.OptionParser()
  opts.add_option('-e', '--exchange', help='exchange name')
  opts.add_option('-t', '--sec_type', help='sec type: ' + ', '.join(types))
  opts.add_option('-f', '--file', help='security symbol list file')
  opts = opts.parse_args()[0]

  if not opts.exchange:
    print("Error: --exchange not give")
    return

  if not opts.file:
    print("Error: --file not give")
    return

  if not opts.sec_type:
    print("Error: --sec_type not give")
    return

  if opts.sec_type not in types:
    print("Error: invalid sec_type")
    return

  conn = pg8000.connect(
      host='127.0.0.1',
      database='opentrade',
      user='postgres',
      password='test')

  cursor = conn.cursor()
  exchanges = {}
  cursor.execute('select name, id from exchange')
  for m in cursor.fetchall():
    exchanges[m[0]] = m[1]
  exchange_id = exchanges.get(opts.exchange)
  if exchange_id is None:
    print('unknown exchange: ' + opts.exchange)
    return
  cursor.execute('select bbgid from security')
  bbgids = set([r[0] for r in cursor.fetchall()])

  with open(opts.file) as fh:
    fh.readline()
    for line in fh:
      toks = line.strip().split(',')
      fields = [
          'symbol', 'local_symbol', '', 'currency', 'bbgid', 'sedol', 'isin',
          'cusip', 'close_price', 'adv20', 'market_cap', 'sector',
          'industry_group', 'industry', 'sub_industry', '', 'lot_size',
          'multiplier', ''
      ]
      values = []
      valid_fields = []
      bbgid = None
      for i in range(0, len(fields)):
        f = fields[i]
        if not f: continue
        valid_fields.append(fields[i])
        values.append(toks[i])
        v = values[-1]
        if not v:
          values[-1] = None
          continue
        if f == 'bbgid': bbgid = v
        if f in ('close_price', 'adv20', 'market_cap', 'lot_size',
                 'multiplier'):
          values[-1] = float(v)
        if f in ('sector', 'industry_group', 'industry', 'sub_industry'):
          values[-1] = int(v)
      values[0] = values[0].split()[0]
      if bbgid not in bbgids:
        valid_fields.append('"type"')
        values.append(opts.sec_type)
        valid_fields.append('exchange_id')
        values.append(exchange_id)
        cursor.execute(
            'insert into security (' + ', '.join(valid_fields) + ') values(' +
            ', '.join(['%s'] * len(valid_fields)) + ')', values)
      else:
        cursor.execute('update security set ' + ', '.join(
            [f + '=%s' for f in valid_fields]) + ' where bbgid=%s',
                       values + [bbgid])
  conn.commit()


if __name__ == '__main__':
  main()
