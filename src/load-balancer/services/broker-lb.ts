import { createConnection, Socket } from "net";
import DNSService from "./dns";
import { debug as debugFactory } from "debug";

type Brokers = {
  connection: Socket;
  id: string;
};
export type BrokerConnectionParams = {
  [id: string]: {
    host: string;
    port: number;
  };
};

export type BrokerTopicMap = {
  [topic: string]: Array<string>;
};

class BrokerLB {
  public clientSubscriptionPackets: Buffer[];
  public clientSubscriptionTopics: string[];
  public selectedBroker: string;
  public myBrokers: Brokers[];
  private socketId: string;
  constructor(socketId: string) {
    this.socketId = socketId;
    const debug = debugFactory(
      `zilmqtt:load-balancer:BrokerLB:${this.socketId}:constructor`
    );
    this.clientSubscriptionPackets = [];
    this.clientSubscriptionTopics = [];
    this.selectedBroker = "";
    this.myBrokers = [];
    debug("BrokerLB Service Initialized");
  }
  public initClientBrokerConnections = async () => {
    const debug = debugFactory(
      `zilmqtt:load-balancer:BrokerLB:${this.socketId}:initClientBrokerConnections`
    );
    const dns = await DNSService.getDNS();
    this.myBrokers = Object.keys(dns)
      .map((brokerId, index) => {
        try {
          return {
            id: brokerId,
            connection: createConnection({
              host: dns[brokerId].host,
              port: dns[brokerId].port,
            }),
          };
        } catch (e) {
          return null;
        }
      })
      .filter((e) => e !== null) as Brokers[];
    debug("this.myBrokers updated to:");
    debug(JSON.stringify(this.myBrokers.map((broker) => broker.id)));
    this.selectedBroker = this.myBrokers[0].id;
    debug("this.selectedBroker updated to:");
    debug(JSON.stringify(this.selectedBroker));
  };
  public getSelectedBroker = () => {
    const debug = debugFactory(
      `zilmqtt:load-balancer:BrokerLB:${this.socketId}:getSelectedBroker`
    );
    debug("this.selectedBroker:", this.selectedBroker);
    return this.selectedBroker;
  };
  public getSelectedBrokerIndex = () => {
    return this.myBrokers.findIndex((b) => b.id === this.getSelectedBroker());
  };
  public getCurrentBrokerIndex = (id: string) => {
    return this.myBrokers.findIndex((b) => b.id === id);
  };
  public shiftBrokerLoad = async () => {
    const debug = debugFactory(
      `zilmqtt:load-balancer:BrokerLB:${this.socketId}:shiftBrokerLoad`
    );
    const currentBrokerId = this.getSelectedBroker();
    debug("currentBrokerId:", currentBrokerId);
    const currentBrokerIndex = this.getSelectedBrokerIndex();
    debug("currentBrokerIndex:", currentBrokerIndex);
    const newBrokerIndex = (currentBrokerIndex + 1) % this.myBrokers.length;
    debug("newBrokerIndex:", newBrokerIndex);
    const newBrokerId = this.myBrokers[newBrokerIndex].id;
    debug("newBrokerId:", newBrokerId);
    this.selectedBroker = newBrokerId;
    for (const subscription of this.clientSubscriptionPackets) {
      this.myBrokers[newBrokerIndex].connection.write(subscription);
    }
    this.myBrokers.splice(currentBrokerIndex, 1);
    await this.updateBrokerTopicMapping(
      this.clientSubscriptionTopics,
      currentBrokerId,
      newBrokerId
    );
  };
  public addBrokerTopicMapping = async (topic: string, brokerId: string) => {
    const debug = debugFactory(
      `zilmqtt:load-balancer:BrokerLB:${this.socketId}:addBrokerTopicMapping`
    );
    const brokerTopicMap = await DNSService.getBrokerTopicMap();
    if (brokerTopicMap[topic]) {
      brokerTopicMap[topic].push(brokerId);
    } else {
      brokerTopicMap[topic] = [brokerId];
    }
    await DNSService.setBrokerTopicMap(brokerTopicMap, topic);
    debug("Updated DNSService.brokerTopicMap:");
    debug(JSON.stringify(brokerTopicMap));
  };
  private updateBrokerTopicMapping = async (
    topics: Array<string>,
    oldBrokerId: string,
    newBrokerId: string
  ) => {
    const debug = debugFactory(
      `zilmqtt:load-balancer:BrokerLB:${this.socketId}:updateBrokerTopicMapping`
    );
    const brokerTopicMap = await DNSService.getBrokerTopicMap();
    for (const topic of topics) {
      debug(
        `Changing the topic '${topic}' load from broker '${oldBrokerId}' to broker '${newBrokerId}'`
      );
      brokerTopicMap[topic] = brokerTopicMap[topic].filter(
        (broker) => broker !== oldBrokerId
      );
      brokerTopicMap[topic].push(newBrokerId);
      await DNSService.setBrokerTopicMap(brokerTopicMap, topic);
    }
    debug("Updated DNSService.brokerTopicMap:");
    debug(JSON.stringify(brokerTopicMap));
  };
}

export default BrokerLB;
