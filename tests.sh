#! /bin/bash

SFIO_VERBOSITY=1
SFIO_PID_OUTPUT=1
SFIO_NO_STATS=1

rm devsdc

echo -e "Test 1\n"
dd if=/dev/zero bs=4000 count=10 2>/dev/null | ./sparse-fio -A -o devsdc
BYTES=$(wc -c devsdc | cut -d " " -f1)

[ "${BYTES}" == "40000" ] || { echo "error size: ${BYTES}"; exit 1; }
rm devsdc


echo -e "Test 2\n"
dd if=/dev/zero bs=4000 count=10 2>/dev/null | ./sparse-fio -A | dd of=devsdc 2>/dev/null
BYTES=$(wc -c devsdc | cut -d " " -f1)

[ "${BYTES}" == "44" ] || { echo "error size: ${BYTES}"; exit 1; }
rm devsdc


echo -e "Test 1b\n"
dd if=/dev/urandom bs=4000 count=10 2>/dev/null | ./sparse-fio -A -o devsdc
BYTES=$(wc -c devsdc | cut -d " " -f1)

[ "${BYTES}" == "40000" ] || { echo "error size: ${BYTES}"; exit 1; }
rm devsdc


echo -e "Test 2b\n"
dd if=/dev/urandom bs=4000 count=10 of=test_in
# dd if=test_in 2>/dev/null | ./sparse-fio  | ./sparse-fio -o devsdc
dd if=test_in 2>/dev/null | ./sparse-fio  -A -o devsdc
# ./sparse-fio  -i test_in -o devsdc
BYTES=$(wc -c devsdc | cut -d " " -f1)

[ "${BYTES}" == "40000" ] || { echo "error size: ${BYTES}"; exit 1; }
cmp test_in devsdc || { echo "error content diff"; exit 1; }
rm devsdc
rm test_in


echo -e "Test 3\n"
truncate -s 1G testdummy
./sparse-fio -A testdummy devsdc
BYTES=$(wc -c devsdc | cut -d " " -f1)

[ "${BYTES}" == "1073741824" ] || { echo "error size: ${BYTES}"; exit 1; }
rm devsdc


echo -e "Test 4\n"
dd if=/dev/urandom bs=16M count=1 of=test_in
# dd if=test_in 2>/dev/null | ./sparse-fio  | ./sparse-fio -o devsdc
dd if=test_in 2>/dev/null | ./sparse-fio -A | ./sparse-fio -A -o devsdc
# ./sparse-fio  -i test_in -o devsdc
BYTES=$(wc -c devsdc | cut -d " " -f1)

[ "${BYTES}" == "16777216" ] || { echo "error size: ${BYTES}"; exit 1; }
cmp test_in devsdc || { echo "error content diff"; exit 1; }
rm devsdc
rm test_in

echo -e "Test 4b\n"
dd if=/dev/urandom bs=16M count=10 of=test_in
# dd if=test_in 2>/dev/null | ./sparse-fio  | ./sparse-fio -o devsdc
dd if=test_in 2>/dev/null | ./sparse-fio -A | ./sparse-fio -A -o devsdc
# ./sparse-fio  -i test_in -o devsdc
BYTES_IN=$(wc -c test_in | cut -d " " -f1)
BYTES_OUT=$(wc -c devsdc | cut -d " " -f1)

[ "${BYTES_IN}" == "${BYTES_OUT}" ] || { echo "error size: ${BYTES_IN} != ${BYTES_OUT}"; exit 1; }
cmp test_in devsdc || { echo "error content diff"; exit 1; }
rm devsdc
rm test_in


echo -e "Test 5\n"
echo "asd" | ./sparse-fio -A -p1 -o devsdc
BYTES=$(wc -c devsdc | cut -d " " -f1)

[ "${BYTES}" == "48" ] || { echo "error size: ${BYTES}"; exit 1; }
rm devsdc


echo -e "Test 6\n"
echo "asd" | ./sparse-fio -A -p0 -o devsdc
BYTES=$(wc -c devsdc | cut -d " " -f1)

[ "${BYTES}" == "4" ] || { echo "error size: ${BYTES}"; exit 1; }
rm devsdc

echo success
