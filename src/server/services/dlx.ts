import { PublishPacket, PubrelPacket } from "aedes:packet";
import Aedes from "aedes";
import { Client } from "aedes:client";
import { setDeadLetterQueue } from "./zilliqa";
import aedesServer from "..";

class DeadLetterExchangeService {
  private status: Record<string, PublishPacket>;
  private topic: string;
  private timeout: number = 10000;
  private encodePacket(packet: PublishPacket): string {
    const encodedPacket = Buffer.from(
      JSON.stringify({
        ...packet,
        payload: Buffer.from(packet.payload).toString("base64"),
      })
    ).toString("base64");
    return encodedPacket;
  }
  public static decodePacket(packet: string): PublishPacket {
    let parsedPacket = JSON.parse(
      Buffer.from(packet, "base64").toString("utf-8")
    );
    parsedPacket = {
      ...parsedPacket,
      payload: Buffer.from(parsedPacket.payload, "base64").toString("utf-8"),
    } as PublishPacket;
    return parsedPacket;
  }
  private ackHandler = (
    packet: PublishPacket | PubrelPacket,
    client: Client
  ) => {
    if (packet?.cmd === "publish" && packet?.topic === this.topic) {
      delete this.status[client.id];
      console.log(
        `Client ID '${client.id}' has ACKed the PUBLISH on topic '${this.topic}'`
      );
    }
  };
  constructor(clientIds: Array<string>, packet: PublishPacket) {
    this.status = {};
    this.topic = packet.topic;
    for (const clientId of clientIds) {
      this.status[clientId] = packet;
    }
    console.log(
      "New object for DeadLetterExchangeService created. Initial status:"
    );
    console.log(JSON.stringify(this.status));
    aedesServer.on("ack", this.ackHandler);
  }

  public startTimer = async () => {
    return new Promise((resolve, reject) => {
      setTimeout(async () => {
        try {
          aedesServer.removeListener("ack", this.ackHandler);
          const status: Record<string, string> = {};
          for (const clientId in this.status) {
            if (this.status[clientId].retain) {
              continue;
            } else {
              status[clientId] = this.encodePacket(this.status[clientId]);
            }
          }
          await setDeadLetterQueue(status);
          resolve(true);
        } catch (err) {
          reject(err);
        }
      }, this.timeout);
    });
  };
}

export default DeadLetterExchangeService;
