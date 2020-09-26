#!/usr/bin/env python3
"""Toggle the bit at the specified offset.
Syntax: <cmdname> filename bit-offset"""

import sys
fname = sys.argv[1]
# Convert bit offset to bytes + leftover bits
bitpos = int(sys.argv[2])
nbytes, nbits = divmod(bitpos, 8)

# Open in read+write, binary mode; read 1 byte
fp = open(fname, "r+b")
fp.seek(nbytes, 0)
c = fp.read(1)

# Toggle bit at byte position `nbits`
toggled = bytes( [ ord(c)^(1<<nbits) ] )
# print(toggled) # diagnostic output

# Back up one byte, write out the modified byte
fp.seek(-1, 1)  # or absolute: fp.seek(nbytes, 0)
fp.write(toggled)
fp.close()
