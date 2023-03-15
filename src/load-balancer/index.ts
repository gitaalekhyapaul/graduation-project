import { createServer, Socket } from "net";
import { Packet, parse } from "mqtt-parser";
import BrokerLB from "./services/broker-lb";
import DNSService from "./services/dns";
import { v4 } from "uuid";
import { config as dotenvConfig } from "dotenv";
import aedes from "aedes";
import { default as debugFactory } from "debug";

dotenvConfig();

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

const aedesServer = new aedes();
//@ts-ignore
const server = createServer(aedesServer.handle);

const loadBalancerServer = createServer(
  {
    allowHalfOpen: true,
  },
  (socket) => {
    const debug = debugFactory("zilmqtt:load-balancer:new-client");
    const socketId = v4();
    (socket as any).id = socketId;
    clients[socketId] = {
      socket: socket,
      lb: new BrokerLB(socketId),
    };
    debug("New client:", (socket as any).id);
    socket.on("data", async (data) => {
      const debug = debugFactory(
        `zilmqtt:load-balancer:client:${socketId}:data-event`
      );
      const lb = clients[socketId].lb;
      if (lb.myBrokers.length === 0) {
        await lb.initClientBrokerConnections();
      }
      const _packet = parse(data)[0];
      debug("Packet received from client:");
      debug("Packet type: ", PacketType[_packet.packetType]);
      debug(JSON.stringify(_packet));
      if (_packet.packetType === 8) {
        const packet = { ..._packet } as Packet.Subscribe;
        lb.clientSubscriptionPackets.push(data);
        lb.clientSubscriptionTopics.push(packet.topics[0].name);
        lb.myBrokers[lb.getSelectedBrokerIndex()].connection.write(data);
        await lb.addBrokerTopicMapping(
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
          const debug = debugFactory(
            `zilmqtt:load-balancer:client:${socketId}:broker:${broker.id}`
          );
          debug("Broker initialized for client.");
          broker.connection.on("data", (data) => {
            const debug = debugFactory(
              `zilmqtt:load-balancer:client:${socketId}:broker:${broker.id}:data-event`
            );
            const _packet = parse(data)[0];
            debug(
              `Packet received from broker '${broker.id}' for client '${socketId}':`
            );
            debug("Packet type: ", PacketType[_packet.packetType]);
            debug(JSON.stringify(_packet));
            if (_packet.packetType === 2) {
              const packet = { ..._packet } as Packet.ConnAck;
              socket.write(data);
            } else if (_packet.packetType === 13) {
              const packet = { ..._packet } as Packet.PingResp;
              socket.write(data);
            } else if (broker.id === lb.getSelectedBroker()) {
              debug(
                `Broker ID '${broker.id}' forwards packet to client '${socketId}'.`
              );
              socket.write(data);
            }
          });
          broker.connection.on("close", async () => {
            const debug = debugFactory(
              `zilmqtt:load-balancer:client:${socketId}:broker:${broker.id}:close-event`
            );
            debug(
              `Broker ID '${broker.id}' closed connection to client '${socketId}'.`
            );
            if (
              lb.getSelectedBrokerIndex() ===
              lb.getCurrentBrokerIndex(broker.id)
            ) {
              await lb.shiftBrokerLoad();
              debug(socketId, "load shifted to next available broker.");
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
new DNSService();
DNSService.initDNS().then((_) => {
  const debug = debugFactory(`zilmqtt:load-balancer:internal`);
  loadBalancerServer.listen(process.env.PORT ?? 1883, () => {
    debug("TCP LB running on", process.env.PORT ?? 1883);
  });
  server.listen(+process.env.PORT! + 1 ?? 1884, () => {
    debug("LB Topology Handler running on", +process.env.PORT! + 1 ?? 1884);
  });
});

aedesServer.authorizeSubscribe = (client, subscription, callback) => {
  const debug = debugFactory(`zilmqtt:load-balancer:aedes:authorizeSubscribe`);
  if (subscription.topic.startsWith("$ZILMQTT")) {
    const brokerData = subscription.topic.split("/");
    const brokerId = brokerData[1];
    const brokerIP = brokerData[2];
    const brokerPort = brokerData[3];
    debug(`New broker connected with Broker ID '${brokerId}'`);
    debug("Broker connection details:", `mqtt://${brokerIP}:${brokerPort}`);
    client.close();
    DNSService.addDNSRecord(`${brokerId}.zilmqtt`, brokerIP, brokerPort)
      .then((_) => {
        const debug = debugFactory(
          "zilmqtt:load-balancer:DNSService:addDNSRecord"
        );
        debug("DNS record added.");
      })
      .catch((e) => {
        const debug = debugFactory(
          "zilmqtt:load-balancer:DNSService:addDNSRecord"
        );
        debug("Error in adding DNS!");
        debug(e);
      });
    callback(null, subscription);
  }
};

process.on("SIGHUP", () => {
  process.exit(0);
});
process.on("SIGINT", () => {
  process.exit(0);
});
