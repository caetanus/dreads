#!/usr/bin/env bash
BIN=/home/caetano/lab/dreads/bin/dreads; CLI=redis-cli; R=/tmp/churn4
pkill -9 -x dreads 2>/dev/null; sleep 1; rm -rf $R
declare -A CONF
for id in 1 2 3; do d=$R/n$id; mkdir -p $d; peers=""
  for p in 1 2 3; do [ $p = $id ]&&continue; peers+="${peers:+,}$p@127.0.0.1:1700$p"; done
  printf 'port 700%s\ndir %s\nraft-node-id %s\nraft-peers %s\nraft-port 1700%s\nsynchronous off\n' $id $d $id "$peers" $id > $d/n.conf
  CONF[$id]=$d/n.conf; $BIN $d/n.conf >$d/out.log 2>&1 & done
sleep 3
L=""; LID=""; for id in 1 2 3; do [ "$($CLI -p 700$id RAFT STATUS 2>/dev/null|sed -n '2p')" = leader ] && { L=700$id; LID=$id; }; done
LPID=$(pgrep -f "n$LID/n.conf"); FIDS=(); for id in 1 2 3; do [ "$id" != "$LID" ] && FIDS+=($id); done
echo "leader=$L(n$LID) followers=n${FIDS[0]},n${FIDS[1]}"
kill -STOP $(pgrep -f "n${FIDS[0]}/n.conf") $(pgrep -f "n${FIDS[1]}/n.conf")   # freeze followers
cat > /tmp/churn4_client.py <<'PY'
import socket,sys,time
port=int(sys.argv[1]); s=socket.create_connection(("127.0.0.1",port)); s.settimeout(15)
def resp(*a): return ("*%d\r\n"%len(a))+"".join("$%d\r\n%s\r\n"%(len(x),x) for x in a)
s.sendall("".join(resp("SET","tk%d"%i,"v%d"%i) for i in range(1,41)).encode())  # 40 -> proposed on old leader only
got=0; data=b""; t0=time.time()
try:
    while time.time()-t0<15:
        c=s.recv(65536)
        if not c: break
        data+=c; got=data.count(b"\r\n")
        if got>=40: break
except (socket.timeout,OSError): pass
ro=data.count(b"READONLY")
print("CLIENT got %d/40 (READONLY=%d) in %.1fs -> %s"%(got,ro,time.time()-t0,"PASS (no hang)" if got>=40 else "FAIL (HUNG)"))
PY
python3 /tmp/churn4_client.py $L & CLIP=$!
sleep 1                                                # old leader proposed 40 (buffered at frozen followers)
kill -STOP $LPID                                       # freeze old leader with 40 in-flight
kill -9 $(pgrep -f "n${FIDS[0]}/n.conf") $(pgrep -f "n${FIDS[1]}/n.conf")   # kill followers -> buffered entries LOST
sleep 0.5
$BIN ${CONF[${FIDS[0]}]} >$R/n${FIDS[0]}/re.log 2>&1 & # restart followers WITHOUT the 40 entries
$BIN ${CONF[${FIDS[1]}]} >$R/n${FIDS[1]}/re.log 2>&1 &
sleep 4
echo "new leader (no 40 entries): $(for id in ${FIDS[@]}; do [ "$($CLI -p 700$id RAFT STATUS 2>/dev/null|sed -n '2p')" = leader ] && echo 700$id; done)"
kill -CONT $LPID                                       # old leader rejoins -> its 40 truncated -> failTruncated
wait $CLIP
sleep 1; echo "cluster keys (should be 0, the 40 never committed): n${FIDS[0]}=$($CLI -p 700${FIDS[0]} DBSIZE) n${FIDS[1]}=$($CLI -p 700${FIDS[1]} DBSIZE) old=$($CLI -p $L DBSIZE)"
pkill -9 -x dreads 2>/dev/null
