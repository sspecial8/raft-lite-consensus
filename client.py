import sys
import requests

if len(sys.argv) < 3:
    print("Usage: python client.py <leader_port> <command>")
    print('Example: python client.py 8000 "SET x = 5"')
    sys.exit(1)

leader_port = sys.argv[1]
command = sys.argv[2]

url = f"http://127.0.0.1:{leader_port}/client_command"

response = requests.post(
    url,
    json={"cmd": command}
)

print("Response:", response.json())
