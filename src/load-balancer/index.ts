import { createServer, createConnection, Socket } from "net";
import { Packet, parse } from "mqtt-parser";
import BrokerLB from "./services/broker-lb";
import { v4 } from "uuid";

const clients: { [x: string]: { socket: Socket; lb: BrokerLB } } = {};

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
    const socketId = v4();
    (socket as any).id = socketId;
    clients[socketId] = {
      socket: socket,
      lb: new BrokerLB(),
    };
    console.log("New client:", (socket as any).id);
    socket.on("data", (data) => {
      const lb = clients[socketId].lb;
      const _packet = parse(data)[0];
      console.log("Packet received from client:");
      console.log(socket.remoteAddress, socket.remotePort);
      console.log("Packet type: ", PacketType[_packet.packetType]);
      console.log(JSON.stringify(_packet));
      if (_packet.packetType === 8) {
        const packet = { ..._packet } as Packet.Subscribe;
        lb.clientSubscriptionPackets.push(data);
        lb.clientSubscriptionTopics.push(packet.topics[0].name);
        lb.myBrokers[lb.getSelectedBrokerIndex()].connection.write(data);
        BrokerLB.addBrokerTopicMapping(
          packet.topics[0].name,
          lb.getSelectedBroker()
        );
      } else if (_packet.packetType === 12) {
        const packet = { ..._packet } as Packet.PingReq;
        for (const broker of lb.myBrokers) {
          broker.connection.write(data);
        }
      } else if (_packet.packetType === 1) {
        const packet = { ..._packet } as Packet.Connect;
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
              console.log(
                "Broker ID'",
                broker.id,
                "' forwards packet to client."
              );
              socket.write(data);
            }
          });
          broker.connection.on("close", () => {
            console.log(
              "Broker ID'",
              broker.id,
              "' closed connection to clientID",
              socketId
            );
            if (
              lb.getSelectedBrokerIndex() ===
              lb.getCurrentBrokerIndex(broker.id)
            ) {
              lb.shiftBrokerLoad();
            }
          });
        }
        for (const broker of lb.myBrokers) {
          broker.connection.write(data);
        }
      } else {
        lb.myBrokers[lb.getSelectedBrokerIndex()].connection.write(data);
      }
    });
  }
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
