#! /bin/bash
export DEBUG="zilmqtt:*"
case $1 in
broker1)
    echo "Running Broker 1 on 1885"
    PORT=1885 BROKER_REMOTE_PORT=1885 yarn dev::server
    ;;
broker2)
    echo "Running Broker 2 on 1886"
    PORT=1886 BROKER_REMOTE_PORT=1886 yarn dev::server
    ;;
brokernolb)
    echo "Running Broker 2 on 1886"
    PORT=1886 BROKER_REMOTE_PORT=1886 LB=false yarn dev::server
    ;;
esac
