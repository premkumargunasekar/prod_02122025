\
#!/usr/bin/env python3
# Atomically append a single CSV line to the subnet CSV using file lock and temp file -> move.
# Usage:
#   update_subnet_csv.py --subnet-csv /opt/GCP/Subnet.csv --line "AO,pr,10.0.0.0/27,name,Owner,Created,Comments"
import argparse, fcntl, os, tempfile, shutil, sys

def append_atomic(path, line):
    dirn = os.path.dirname(path) or "."
    tmpname = None
    fd = None
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # Lock target file
        fd = open(path, "a+")
        fcntl.flock(fd, fcntl.LOCK_EX)
        # Create temp file in same dir to ensure atomic move
        fd_tmp, tmpname = tempfile.mkstemp(dir=dirn, prefix=".tmp_subnet_")
        with os.fdopen(fd_tmp, "w") as fh:
            fh.write(line.rstrip("\\n") + "\\n")
        # append temp file to original
        with open(path, "a") as target, open(tmpname, "r") as src:
            shutil.copyfileobj(src, target)
        # release lock
        fcntl.flock(fd, fcntl.LOCK_UN)
        fd.close()
        os.remove(tmpname)
        return 0, "OK"
    except Exception as e:
        try:
            if fd:
                fcntl.flock(fd, fcntl.LOCK_UN)
                fd.close()
        except Exception:
            pass
        if tmpname and os.path.exists(tmpname):
            os.remove(tmpname)
        return 2, str(e)

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--subnet-csv', required=True)
    p.add_argument('--line', required=True)
    args = p.parse_args()
    rc, msg = append_atomic(args.subnet_csv, args.line)
    if rc != 0:
        print(msg, file=sys.stderr)
    else:
        print(msg)
    sys.exit(rc)

if __name__ == '__main__':
    main()
