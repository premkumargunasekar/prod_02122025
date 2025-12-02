\
#!/usr/bin/env python3
# Improved CIDR allocator with file locking and simple overlap validation.
# Usage:
#   calc_next_subnet.py --block 10.0.0.0/16 --size 27 --subnet-csv /opt/GCP/Subnet.csv
#   calc_next_subnet.py --validate 10.0.0.0/27 --subnet-csv /opt/GCP/Subnet.csv

import argparse, csv, ipaddress, fcntl, os, sys

def load_allocated(path):
    entries = []
    if not os.path.exists(path):
        return entries
    with open(path) as fh:
        r = csv.reader(fh)
        for row in r:
            if len(row) >= 3:
                entries.append(row[2])
    return entries

def find_next(base_cidr, size, allocated):
    net = ipaddress.ip_network(base_cidr)
    for s in net.subnets(new_prefix=size):
        overlap = False
        for a in allocated:
            try:
                if ipaddress.ip_network(a).overlaps(s):
                    overlap = True
                    break
            except Exception:
                continue
        if not overlap:
            return str(s)
    return ""

def validate_cidr(cidr, allocated):
    try:
        c = ipaddress.ip_network(cidr)
    except Exception as e:
        return "INVALID"
    for a in allocated:
        try:
            if ipaddress.ip_network(a).overlaps(c):
                return "OVERLAP"
        except Exception:
            continue
    return "OK"

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--block', help='Base block like 10.0.0.0/16')
    p.add_argument('--size', type=int, help='Prefix size for new subnet')
    p.add_argument('--subnet-csv', help='Path to subnet CSV')
    p.add_argument('--validate', help='CIDR to validate')
    args = p.parse_args()

    allocated = load_allocated(args.subnet_csv or "")

    if args.validate:
        res = validate_cidr(args.validate, allocated)
        if res == "OK":
            print("OK", end="")
            sys.exit(0)
        else:
            print(res, end="")
            sys.exit(2)

    if not args.block or not args.size:
        print("", end="")
        sys.exit(1)

    lock_fd = None
    try:
        lock_fd = open(args.subnet_csv or "/tmp/.subnetcsv.lock", "a+")
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
    except Exception:
        pass

    try:
        next_cidr = find_next(args.block, args.size, allocated)
        print(next_cidr, end="")
    finally:
        try:
            if lock_fd:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                lock_fd.close()
        except Exception:
            pass

if __name__ == '__main__':
    main()
