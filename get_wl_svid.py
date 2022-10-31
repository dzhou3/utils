#!/usr/bin/env python3

import argparse
import pathlib
import re
if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--ica', action='store_true')
  parser.add_argument('--cert_fpath', type=pathlib.Path)
  args = parser.parse_args()
  pattern = re.compile('-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----', flags=re.M|re.S)
  with open(args.cert_fpath, 'rt') as f:
    certs = pattern.findall(f.read().strip())
  res = certs[1] if args.ica else certs[0]
  print(res, end='')
