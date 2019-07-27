#! /bin/bash

SFIO_VERBOSITY=1
SFIO_PID_OUTPUT=1
SFIO_NO_STATS=1

function cleanup {
	trap - EXIT
	set +e
	
	echo cleanup
	
	[ -z ${LOOP_DEV} ] || losetup -d ${LOOP_DEV}
	[ ! -e loopfile ] || rm loopfile
	
	[ ! -e testdummy ] || rm testdummy
}

[ -z ${OUTFILE} ] && OUTFILE=devsdc

if [ "$(whoami)" == "root" ]; then
	truncate -s 1G loopfile
	LOOP_DEV=$(losetup -f --show loopfile)
	echo "Loop dev ${LOOP_DEV}"
fi

trap cleanup EXIT

rm ${OUTFILE}

echo -e "Test 1\n"
dd if=/dev/zero bs=4000 count=10 2>/dev/null | ./sparse-fio -A -o ${OUTFILE}
BYTES=$(wc -c ${OUTFILE} | cut -d " " -f1)

[ "${BYTES}" == "40000" ] || { echo "error size: ${BYTES}"; exit 1; }
rm ${OUTFILE}


echo -e "Test 2\n"
dd if=/dev/zero bs=4000 count=10 2>/dev/null | ./sparse-fio -A | dd of=${OUTFILE} 2>/dev/null
BYTES=$(wc -c ${OUTFILE} | cut -d " " -f1)

[ "${BYTES}" == "44" ] || { echo "error size: ${BYTES}"; exit 1; }
rm ${OUTFILE}


echo -e "Test 1b\n"
dd if=/dev/urandom bs=4000 count=10 2>/dev/null | ./sparse-fio -A -o ${OUTFILE}
BYTES=$(wc -c ${OUTFILE} | cut -d " " -f1)

[ "${BYTES}" == "40000" ] || { echo "error size: ${BYTES}"; exit 1; }
rm ${OUTFILE}


echo -e "Test 2b\n"
dd if=/dev/urandom bs=4000 count=10 of=test_in
# dd if=test_in 2>/dev/null | ./sparse-fio  | ./sparse-fio -o ${OUTFILE}
dd if=test_in 2>/dev/null | ./sparse-fio  -A -o ${OUTFILE}
[ -z ${LOOP_DEV} ] || dd if=test_in 2>/dev/null | ./sparse-fio  -A -o ${LOOP_DEV}

# ./sparse-fio  -i test_in -o ${OUTFILE}
BYTES=$(wc -c ${OUTFILE} | cut -d " " -f1)

[ "${BYTES}" == "40000" ] || { echo "error size: ${BYTES}"; exit 1; }
cmp test_in ${OUTFILE} || { echo "error content diff"; exit 1; }
[ -z ${LOOP_DEV} ] || cmp -n "${BYTES}" test_in ${LOOP_DEV} || { echo "error content diff (blockdev)"; exit 1; }
rm ${OUTFILE}
rm test_in


echo -e "Test 3\n"
truncate -s 1G testdummy
./sparse-fio -A testdummy ${OUTFILE}
BYTES=$(wc -c ${OUTFILE} | cut -d " " -f1)

[ "${BYTES}" == "1073741824" ] || { echo "error size: ${BYTES}"; exit 1; }
rm ${OUTFILE}


echo -e "Test 4\n"
dd if=/dev/urandom bs=16M count=1 of=test_in
# dd if=test_in 2>/dev/null | ./sparse-fio  | ./sparse-fio -o ${OUTFILE}
dd if=test_in 2>/dev/null | ./sparse-fio -A | ./sparse-fio -A -o ${OUTFILE}
[ -z ${LOOP_DEV} ] || dd if=test_in 2>/dev/null | ./sparse-fio -A | ./sparse-fio -A -o ${LOOP_DEV}
# ./sparse-fio  -i test_in -o ${OUTFILE}
BYTES=$(wc -c ${OUTFILE} | cut -d " " -f1)

[ "${BYTES}" == "16777216" ] || { echo "error size: ${BYTES}"; exit 1; }
cmp test_in ${OUTFILE} || { echo "error content diff"; exit 1; }
[ -z ${LOOP_DEV} ] || cmp -n "${BYTES}" test_in ${LOOP_DEV} || { echo "error content diff (blockdev)"; exit 1; }
rm ${OUTFILE}
rm test_in

echo -e "Test 4b\n"
dd if=/dev/urandom bs=16M count=10 of=test_in
# dd if=test_in 2>/dev/null | ./sparse-fio  | ./sparse-fio -o ${OUTFILE}
dd if=test_in 2>/dev/null | ./sparse-fio -A | ./sparse-fio -A -o ${OUTFILE}
[ -z ${LOOP_DEV} ] || dd if=test_in 2>/dev/null | ./sparse-fio -A | ./sparse-fio -A -o ${LOOP_DEV}
# ./sparse-fio  -i test_in -o ${OUTFILE}
BYTES_IN=$(wc -c test_in | cut -d " " -f1)
BYTES_OUT=$(wc -c ${OUTFILE} | cut -d " " -f1)

[ "${BYTES_IN}" == "${BYTES_OUT}" ] || { echo "error size: ${BYTES_IN} != ${BYTES_OUT}"; exit 1; }
cmp test_in ${OUTFILE} || { echo "error content diff"; exit 1; }
[ -z ${LOOP_DEV} ] || cmp -n "${BYTES_IN}" test_in ${LOOP_DEV} || { echo "error content diff (blockdev)"; exit 1; }
rm ${OUTFILE}
rm test_in


echo -e "Test 5\n"
echo "asd" | ./sparse-fio -A -p1 -o ${OUTFILE}
BYTES=$(wc -c ${OUTFILE} | cut -d " " -f1)

[ "${BYTES}" == "48" ] || { echo "error size: ${BYTES}"; exit 1; }
rm ${OUTFILE}


echo -e "Test 6\n"
echo "asd" | ./sparse-fio -A -p0 -o ${OUTFILE}
BYTES=$(wc -c ${OUTFILE} | cut -d " " -f1)

[ "${BYTES}" == "4" ] || { echo "error size: ${BYTES}"; exit 1; }
rm ${OUTFILE}

cleanup

echo success
