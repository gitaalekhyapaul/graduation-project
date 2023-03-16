#! /bin/bash
export DEBUG="zilmqtt:*"
echo -ne "Enter your option:\n1 : Switching on Load-Balanced Broker 1 on Port 1885\n2 : Switching on Load-Balanced Broker 2 on Port 1886\n3 : Switching on Broker without LB on Port 1885\nEnter your choice: "
read option
case $option in
1)
    echo "Running Broker 1 on 1885"
    PORT=1885 BROKER_REMOTE_PORT=1885 yarn dev::server
    ;;
2)
    echo "Running Broker 2 on 1886"
    PORT=1886 BROKER_REMOTE_PORT=1886 yarn dev::server
    ;;
3)
    echo "Broker without LB on Port 1885"
    PORT=1885 LB=false yarn dev::server
    ;;
*)
    echo "Wrong Input."
    ;;
esac
