#! /bin/bash

SFIO_VERBOSITY=1
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

[ "${BYTES}" == "44" ] || { echo "error size: ${BYTES}"; exit 1; }
rm devsdc

echo -e "Test 1b\n"
dd if=/dev/urandom bs=4000 count=10 2>/dev/null | ./sparse-fio -o devsdc
BYTES=$(wc -c devsdc | cut -d " " -f1)

[ "${BYTES}" == "40000" ] || { echo "error size: ${BYTES}"; exit 1; }
rm devsdc

echo -e "Test 2b\n"
dd if=/dev/urandom bs=4000 count=10 of=test_in
# dd if=test_in 2>/dev/null | ./sparse-fio  | ./sparse-fio -o devsdc
dd if=test_in 2>/dev/null | ./sparse-fio  -o devsdc
# ./sparse-fio  -i test_in -o devsdc
BYTES=$(wc -c devsdc | cut -d " " -f1)

[ "${BYTES}" == "40000" ] || { echo "error size: ${BYTES}"; exit 1; }
cmp test_in devsdc || { echo "error content diff"; exit 1; }
rm devsdc
rm test_in


echo -e "Test 3\n"
truncate -s 1G testdummy
./sparse-fio testdummy devsdc
BYTES=$(wc -c devsdc | cut -d " " -f1)

[ "${BYTES}" == "1073741824" ] || { echo "error size: ${BYTES}"; exit 1; }
rm devsdc

echo -e "Test 4\n"
dd if=/dev/urandom bs=16M count=1 of=test_in
# dd if=test_in 2>/dev/null | ./sparse-fio  | ./sparse-fio -o devsdc
dd if=test_in 2>/dev/null | ./sparse-fio | ./sparse-fio  -o devsdc
# ./sparse-fio  -i test_in -o devsdc
BYTES=$(wc -c devsdc | cut -d " " -f1)

[ "${BYTES}" == "16777216" ] || { echo "error size: ${BYTES}"; exit 1; }
cmp test_in devsdc || { echo "error content diff"; exit 1; }
rm devsdc
rm test_in

echo success
