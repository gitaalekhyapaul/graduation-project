import { createServer, createConnection } from "net";
import { Packet, parse } from "mqtt-parser";
import BrokerLB from "./services/broker-lb";

const PacketType: { [x: number]: string } = {
  1: "CONNECT",
  2: "CONNACK",
  3: "PUBLISH",
  4: "PUBACK",
  5: "PUBREC",
  6: "PUBREL",
  7: "PUBCOMP",
  8: "SUBSCRIBE",
  9: "SUBACK",
  10: "UNSUBSCRIBE",
  11: "UNSUBACK",
  12: "PINGREQ",
  13: "PINGRESP",
  14: "DISCONNECT",
};

const server = createServer(
  {
    allowHalfOpen: true,
  },
  (socket) => {
    const lb = new BrokerLB();
    for (const broker of lb.myBrokers) {
      console.log(broker.id);
      broker.connection.on("data", (data) => {
        const _packet = parse(data)[0];
        console.log("Packet received from broker ID '", broker.id, "':");
        console.log("Packet type: ", PacketType[_packet.packetType]);
        console.log(JSON.stringify(_packet));
        if (_packet.packetType === 2) {
          const packet = { ..._packet } as Packet.ConnAck;
          socket.write(data);
        } else if (_packet.packetType === 13) {
          const packet = { ..._packet } as Packet.PingResp;
          socket.write(data);
        } else if (broker.id === lb.getSelectedBroker()) {
          socket.write(data);
        }
      });
      broker.connection.on("close", () => {
        if (
          lb.getSelectedBrokerIndex() === lb.getCurrentBrokerIndex(broker.id)
        ) {
          lb.shiftBrokerLoad();
        }
      });
    }
    socket.on("data", (data) => {
      const _packet = parse(data)[0];
      console.log("Packet received from client:");
      console.log(socket.remoteAddress, socket.remotePort);
      console.log("Packet type: ", PacketType[_packet.packetType]);
      console.log(JSON.stringify(_packet));
      if (_packet.packetType === 8) {
        const packet = { ..._packet } as Packet.Subscribe;
        lb.clientSubscriptions.push(data);
        lb.myBrokers[lb.getSelectedBrokerIndex()].connection.write(data);
      } else if (_packet.packetType === 12) {
        const packet = { ..._packet } as Packet.PingReq;
        for (const broker of lb.myBrokers) {
          broker.connection.write(data);
        }
      } else if (_packet.packetType === 1) {
        const packet = { ..._packet } as Packet.Connect;
        for (const broker of lb.myBrokers) {
          broker.connection.write(data);
        }
      } else {
        lb.myBrokers[lb.getSelectedBrokerIndex()].connection.write(data);
      }
    });
  }

  // {
  // const brokerConn1 = createConnection({
  //   port: 1883,
  //   host: "127.0.0.1",
  // });
  // const brokerConn2 = createConnection({
  //   port: 1884,
  //   host: "127.0.0.1",
  // });
  // // socket.pipe(brokerConn1).pipe(socket);
  // socket.on("data", (data) => {
  //   console.log("Packet received from client:");
  //   console.log(socket.remoteAddress, socket.remotePort);
  //   parser.parse(data);
  //   brokerConn1.write(data);
  //   console.log(data.toString("hex").match(/../g)?.join(" "));
  // });
  // brokerConn1.on("data", (data) => {
  //   console.log("Packet received from broker:");
  //   parser.parse(data);
  //   socket.write(data);
  //   console.log(data.toString("hex").match(/../g)?.join(" "));
  // });
  // brokerConn1.on("close", () => {
  //   socket.pipe(brokerConn2).pipe(socket);
  // });
  // }
);

server.listen(1885, () => {
  BrokerLB.initBrokers();
  console.log("TCP LB running on 1885");
});

process.on("SIGHUP", () => {
  process.exit(0);
});
process.on("SIGINT", () => {
  process.exit(0);
});
