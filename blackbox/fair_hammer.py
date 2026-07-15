import socket, time, subprocess, sys
PORT = "7788"
def blocked():
    out = subprocess.run(["redis-cli","-p",PORT,"info","clients"],capture_output=True,text=True,timeout=2).stdout
    return int(out.split("blocked_clients:")[1].split()[0])
def readline(s):
    s.settimeout(2.0); buf=b""
    while not buf.endswith(b"\r\n"): buf+=s.recv(1)
    return buf
def waitblk(n):
    for _ in range(200):
        if blocked()==n: return True
        time.sleep(0.01)
    return False
N = int(sys.argv[1]) if len(sys.argv)>1 else 500
for it in range(N):
    subprocess.run(["redis-cli","-p",PORT,"del","mylist"],capture_output=True,timeout=2)
    rd1=socket.create_connection(('127.0.0.1',int(PORT)))
    rd2=socket.create_connection(('127.0.0.1',int(PORT)))
    try:
        rd1.sendall(b"*3\r\n$5\r\nBLPOP\r\n$6\r\nmylist\r\n$1\r\n0\r\n")
        if not waitblk(1): print(f"it{it}: FAIL blk!=1 pre ({blocked()})",flush=True); break
        rd2.sendall(b"*3\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$1\r\n1\r\n*3\r\n$5\r\nBLPOP\r\n$6\r\nmylist\r\n$1\r\n0\r\n")
        h=readline(rd1)
        readline(rd1); readline(rd1); readline(rd1); readline(rd1)
        push=readline(rd2)
        if push[:2]!=b":1": print(f"it{it}: FAIL lpush reply {push}",flush=True); break
        if not waitblk(1): print(f"it{it}: FAIL blk!=1 mid ({blocked()})",flush=True); break
        subprocess.run(["redis-cli","-p",PORT,"lpush","mylist","2"],capture_output=True,timeout=2)
        if not waitblk(0): print(f"it{it}: FAIL blk!=0 end ({blocked()})",flush=True); break
        rd2.settimeout(2.0); r2=rd2.recv(200)
        if b"mylist" not in r2 or b"2" not in r2: print(f"it{it}: FAIL rd2 got {r2}",flush=True); break
    except Exception as e:
        print(f"it{it}: EXC {e} blk={blocked()}",flush=True); break
    finally:
        rd1.close(); rd2.close()
    if it%50==0: print(f"it{it} ok",flush=True)
else:
    print("ALL PASS",flush=True)
