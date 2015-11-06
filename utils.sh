check_non_empty() {
  # $1 is the content of the variable in quotes e.g. "$FROM_EMAIL"
  # $2 is the error message
  if [[ $1 == "" ]]; then
    echo "ERROR: specify $2"
    exit -1
  fi
}

check_exec_success() {
  # $1 is the content of the variable in quotes e.g. "$FROM_EMAIL"
  # $2 is the error message
  if [[ $1 != "0" ]]; then
    echo "ERROR: $2 failed"
    echo "$3"
    exit -1
  fi
}

get_host_ip() {
  echo "$(ifconfig eth0 | awk '/\<inet\>/ { print $2}' | sed 's/addr://g')"
}
