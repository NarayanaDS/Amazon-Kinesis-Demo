import os
import json
import boto3
import sys
import time
import datetime, socket, uuid


def get_filesystem_metrics(p):
    stats = os.statvfs("/")
    block_size = stats.f_frsize

    fs_size = block_size * stats.f_blocks
    fs_free = block_size * stats.f_bfree
    fs_available = block_size * stats.f_bavail
    
    return (fs_size, fs_free, fs_available)

def get_agent_version():
    return "0.1"

def get_hostname():
    return socket.gethostname()

def get_event_time():
    return datetime.datetime.now().isoformat()

def get_event_id():
    return str(uuid.uuid4())

def create_event():
    size, free, available = get_filesystem_metrics("/")
    event_id = get_event_id()
    payload = {
        "id": event_id,
        "subject": {
            "agent": {
                "version": get_agent_version()
            }
        },
        "verb": "read",
        "direct_object": {
            "filesystem_matrics": {
                "size": size,
                "free": free,
                "available": available
            }
        },
        "time": get_event_time(),
        "on": {
            "server": {
                "hostname": get_hostname()
            }
        }
    }
    
    return (event_id, payload)

def write_event(conn, stream_name):
    event_id, event_playload = create_event()
    event_json = json.dumps(event_playload)

    conn.put_record(StreamName=stream_name, 
                    Data=event_json,
                    PartitionKey=event_id)
    # Debug
    print("*"*100)
    print(event_json)    
    return event_id

if __name__ == "__main__":

    session = boto3.Session()
    conn = session.client("kinesis")
    
    if len(sys.argv) != 3:
        print("Invalide Argument")
        print("Usage: python agent.py <time dutation to run is seconds> <frequency of event trigger, e.g. every 10 seconds, or every 30 seconds>")
        sys.exit(1)
    
    exit_count = int(sys.argv[1])
    sleep_frequency = int(sys.argv[2])

    n = 0 
    m = 0
    while True:
        if n <= exit_count:
            event_id = write_event(conn, "events")
            m += 1
            print(f"Event no. {m}, Event id: {event_id}, time left {exit_count - n} seconds")
            n += sleep_frequency  
            
            time.sleep(sleep_frequency)          
        else:
            break


    
