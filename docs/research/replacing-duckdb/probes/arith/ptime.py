#!/usr/bin/env python3
"""Stand-in for `/usr/bin/time -v` (not installed on tp): wall time and the
child's peak RSS from getrusage(RUSAGE_CHILDREN), which is the same counter
GNU time prints as 'Maximum resident set size'."""
import resource, subprocess, sys, time

t0 = time.monotonic()
p = subprocess.run(sys.argv[1:])
wall = time.monotonic() - t0
ru = resource.getrusage(resource.RUSAGE_CHILDREN)
print(f"  [{' '.join(sys.argv[2:])}] wall {wall:.3f}s  peak RSS {ru.ru_maxrss / 1024:.0f} MB  exit {p.returncode}")
