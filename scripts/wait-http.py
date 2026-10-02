#!/usr/bin/env python3
"""Wait for an HTTP endpoint; optionally verify OpenSearch cluster health semantics."""
import argparse
import json
import time
import urllib.request


def healthy_opensearch(payload):
    return payload.get('status') in {'yellow', 'green'} and payload.get('timed_out') is False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('url')
    parser.add_argument('seconds', type=int, nargs='?', default=180)
    parser.add_argument('--opensearch-health', action='store_true')
    args = parser.parse_args()
    deadline = time.monotonic() + args.seconds
    last = 'no response'
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(args.url, timeout=4) as response:
                ready = response.status == 200
                if args.opensearch_health:
                    payload = json.load(response)
                    ready = ready and healthy_opensearch(payload)
                    last = f"status={payload.get('status')}, timed_out={payload.get('timed_out')}"
                if ready:
                    print('Ready:', args.url)
                    return
        except Exception as error:
            last = str(error)
        time.sleep(2)
    raise SystemExit(f'Timed out: {args.url}; {last}')


if __name__ == '__main__':
    main()
