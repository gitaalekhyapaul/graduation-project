import aedes from "aedes";
import net from "net";
import { config as dotenvConfig } from "dotenv";
import { connect } from "mqtt";
import debug from "debug";

import ZilliqaService from "./services/zilliqa";
import PersistenceService from "./services/persistence";
import DeadLetterExchangeService from "./services/dlx";
PersistenceService.initPersistence();
const aedesServer = new aedes({
  persistence: PersistenceService.getPersistence(),
});
//@ts-ignore
const server = net.createServer(aedesServer.handle);
dotenvConfig();
const PORT = process.env.PORT ?? 1883;
server.listen(PORT, () => {
  new ZilliqaService();
  const debugFactory = debug("zilmqtt:aedes:internal");
  debugFactory("MQTT Server Listening on Port", PORT);
  debugFactory("Aedes server ID:", aedesServer.id);
  if (process.env.LB === "true") {
    debugFactory(
      "Load balancing enabled on server. Attempting to connect to LB..."
    );
    const loadBalancerConn = connect(
      `mqtt://${process.env.LB_HOST}:${process.env.LB_PORT}`,
      {
        reconnectPeriod: 0,
      }
    );
    loadBalancerConn.on("connect", () => {
      debugFactory(
        "LB Connected at",
        `mqtt://${process.env.LB_HOST}:${process.env.LB_PORT}`
      );
      loadBalancerConn.subscribe(
        `$ZILMQTT/${aedesServer.id}/${process.env.BROKER_REMOTE_IP}/${process.env.BROKER_REMOTE_PORT}`
      );
      debugFactory(
        "Subscribed to",
        `$ZILMQTT/${aedesServer.id}/${process.env.BROKER_REMOTE_IP}/${process.env.BROKER_REMOTE_PORT}`,
        "on LB"
      );
    });
    loadBalancerConn.on("close", () => {
      debugFactory(
        "Received connection close request from LB. Closing MQTT connection..."
      );
      loadBalancerConn.end();
    });
  } else {
    debugFactory(
      "Environment variable 'LB' set to 'false'. Skipping Load Balancer Init..."
    );
  }
});

aedesServer.on("client", (client) => {
  const debugFactory = debug(`zilmqtt:aedes:client:${client.id}:connect-event`);
  debugFactory("New client connected!");
  debugFactory("Connected client ID:", client.id);
  debugFactory("Getting the dead letters for client...");
  Promise.all([ZilliqaService.getDeadLetterQueue(client.id)])
    .then((msg) => {
      const messages = msg[0];
      debugFactory("Dead letters retrieved:");
      debugFactory(JSON.stringify(messages));
      for (const payload of messages) {
        client.publish(payload, (e) => {
          if (e) debugFactory(e);
          else
            debugFactory(
              `Dead letter MQTT packet ID ${payload.messageId} published to client.`
            );
        });
      }
    })
    .catch((e) => debugFactory(e));
});

aedesServer.authorizeSubscribe = (client, subscription, callback) => {
  const debugFactory = debug(
    `zilmqtt:aedes:client:${client.id}:handler:authorizeSubscribe`
  );
  debugFactory(`Client subscribed to topic '${subscription.topic}'`);
  debugFactory(
    `Fetching retained messages for topic '${subscription.topic}'...`
  );
  Promise.all([ZilliqaService.getRetainedMessages(subscription.topic)])
    .then((msg) => {
      const messages = msg[0];
      debugFactory("Retained messages retrieved:");
      debugFactory(JSON.stringify(messages));
      for (const payload of messages) {
        client.publish(payload, (e) => {
          if (e) debugFactory(e);
          else
            debugFactory(
              `Retained message MQTT packet ID ${payload.messageId} published to client.`
            );
        });
      }
    })
    .catch((e) => debugFactory(e));
  callback(null, subscription);
};

aedesServer.authorizePublish = async (client, packet, callback) => {
  const debugFactory = debug(
    `zilmqtt:aedes:client:${client?.id}:handler:authorizePublish`
  );
  debugFactory(`Client wants to publish to topic '${packet.topic}'`);
  if (packet.qos > 0 && packet.retain === false) {
    debugFactory("Initializing DeadLetterExchangeService for the client IDs:");
    const clientIds = await PersistenceService.getClientsByTopic(packet.topic);
    debugFactory(JSON.stringify(clientIds));
    let wait = new DeadLetterExchangeService(clientIds, packet);
    Promise.all([
      (async () => {
        await wait.startTimer();
      })(),
    ]).catch((e) => debugFactory(e));
  } else {
    debug("QoS 0 PUBLISH received, skipping DeadLetterExchangeService...");
    Promise.all([
      ZilliqaService.setRetainedMessages(packet.topic, packet),
    ]).catch((e) => debugFactory(e));
  }
  callback(null);
};

process.on("SIGHUP", () => {
  process.exit(0);
});
process.on("SIGINT", () => {
  process.exit(0);
});

export default aedesServer;
