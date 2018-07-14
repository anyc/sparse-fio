#! /bin/bash

SFIO_VERBOSITY=3
SFIO_PID_OUTPUT=1
SFIO_NO_STATS=1

rm devsdc

echo -e "Test 1\n"
dd if=/dev/zero bs=4000 count=10 2>/dev/null | ./sparse-fio -o devsdc
BYTES=$(wc -c devsdc | cut -d " " -f1)

[ "${BYTES}" == "40000" ] || { echo "error size: ${BYTES}"; exit 1; }
rm devsdc

echo -e "Test 2\n"
dd if=/dev/zero bs=4000 count=10 2>/dev/null | ./sparse-fio  | dd of=devsdc 2>/dev/null
BYTES=$(wc -c devsdc | cut -d " " -f1)

[ "${BYTES}" == "36" ] || { echo "error size: ${BYTES}"; exit 1; }
rm devsdc

echo -e "Test 1b\n"
dd if=/dev/urandom bs=4000 count=10 2>/dev/null | ./sparse-fio -o devsdc
BYTES=$(wc -c devsdc | cut -d " " -f1)

[ "${BYTES}" == "40000" ] || { echo "error size: ${BYTES}"; exit 1; }
rm devsdc

echo -e "Test 2b\n"
dd if=/dev/urandom bs=4000 count=10 2>/dev/null | ./sparse-fio  | ./sparse-fio -o devsdc
BYTES=$(wc -c devsdc | cut -d " " -f1)

[ "${BYTES}" == "40000" ] || { echo "error size: ${BYTES}"; exit 1; }
rm devsdc
