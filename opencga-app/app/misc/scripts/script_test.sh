

cat <(echo "hello") - <(echo "world")

# my_fifo_1
## open the pipe on auxiliary FD #5 in both ways (otherwise it will block),
## then open descriptors for writing and reading and close the auxiliary FD
#exec 5<> my_fifo_1 3>my_fifo_1  4<my_fifo_1 5>&-
#rm my_fifo_1
#echo "FIFO created and opened for reading and writing" 1>&2
#
#echo "Writing to FIFO" 1>&2
#function writeToFifo() {
#  echo "Hello, FIFO!" >&3
#  cat << EOF >&3
#  This is a test message.
#EOF
##  seq 15000 >&3
#  echo "Closing FIFO" >&3
#  exec 3>&-
#  echo "input FIFO closed"
#}
#writeToFifo &
#sleep 1
#echo "Reading from FIFO" 1>&2
#echo "-----------" 1>&2
#cat <&4
#echo "-----------" 1>&2
#echo "Closing FIFO" 1>&2
#exec 4>&-
#echo "FIFO closed" 1>&2
#echo "Done" 1>&2
#
