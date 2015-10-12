import sys
import json
from http import client


if __name__ == "__main__":
    if len(sys.argv) != 3:
        # print(1)
        exit(1)
    host = sys.argv[1]
    url = sys.argv[2]
    conn = client.HTTPConnection(host, timeout=10)
    try:
        conn.request('GET', url)
    except Exception:
        conn.close()
        # print(2)
        exit(2)
    r1 = conn.getresponse()
    if r1.status != 200:
        conn.close()
        # print(3)
        exit(3)
    try:
        data = r1.readall().decode()
        data = json.loads(data)
        first = data[0]
        if first['attempts'][0]['completed']:
            # print(1)
            exit(1)
        else:
            # print(0)
            exit(0)
    except Exception:
        conn.close()
        # print(4)
        exit(4)
