import aedes from "aedes";
import net from "net";
import { config as dotenvConfig } from "dotenv";

import { getRetainedMessages, setRetainedMessages } from "./services/zilliqa";

const aedesServer = new aedes();
//@ts-ignore
const server = net.createServer(aedesServer.handle);
dotenvConfig();
const PORT = process.env.PORT ?? 1883;
server.listen(PORT, () => {
  console.log("MQTT Server Listening on Port", PORT);
  console.log("Aedes server ID:", aedesServer.id);
});

aedesServer.on("client", (client) => {
  console.log("New client connected!");
  console.log("Connected client ID:", client.id);
});

aedesServer.authorizeSubscribe = (client, subscription, callback) => {
  Promise.all([getRetainedMessages(subscription.topic)])
    .then((msg) => {
      const messages = msg[0];
      for (const payload of messages) {
        client.publish(payload,
          (e) => {
            if (e) console.error(e);
          }
        );
      }
    })
    .catch((e) => console.error(e));
  callback(null, subscription);
};

aedesServer.authorizePublish = (client, packet, callback) => {
  Promise.all([setRetainedMessages(packet.topic, packet)]).catch((e) =>
    console.error(e)
  );
  callback(null);
};

process.on("SIGHUP", () => {
  process.exit(0);
});
process.on("SIGINT", () => {
  process.exit(0);
});
