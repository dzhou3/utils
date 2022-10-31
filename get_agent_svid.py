#!/usr/bin/env python3

from asn1crypto import x509, pem
import argparse
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--ica', action='store_true')
    args = parser.parse_args()
    with open('/opt/spire-gateway/data/agent/agent_svid.der', 'rb') as f_agent_svid_der:
        _bytes = f_agent_svid_der.read()
        agent_svid_bytes = x509.Certificate.load(_bytes)
        ica_svid_bytes = x509.Certificate.load(_bytes[len(agent_svid_bytes.dump()):])
        agent_svid = pem.armor('CERTIFICATE', agent_svid_bytes.dump()).decode()
        ica_svid = pem.armor('CERTIFICATE', ica_svid_bytes.dump()).decode()
    res = ica_svid if args.ica else agent_svid
    print(res, end='')
