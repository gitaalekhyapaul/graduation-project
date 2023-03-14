import { createConnection, Socket } from "net";
import { generate } from "mqtt-packet";

type Brokers = {
  connection: Socket;
  id: string;
};
type BrokerConnectionParams = {
  [id: string]: {
    host: string;
    port: number;
  };
};

type BrokerTopicMap = {
  [topic: string]: Array<string>;
};

class BrokerLB {
  public static brokers: BrokerConnectionParams = {};
  public static brokerTopicMap: BrokerTopicMap = {};
  public clientSubscriptionPackets: Buffer[];
  public clientSubscriptionTopics: string[];
  public selectedBroker: string;
  public myBrokers: Brokers[];
  constructor() {
    this.myBrokers = Object.keys(BrokerLB.brokers).map((brokerId, index) => {
      return {
        id: brokerId,
        connection: createConnection({
          host: BrokerLB.brokers[brokerId].host,
          port: BrokerLB.brokers[brokerId].port,
        }),
      };
    });
    this.clientSubscriptionPackets = [];
    this.clientSubscriptionTopics = [];
    this.selectedBroker = this.myBrokers[0].id;
  }
  public getSelectedBroker = () => {
    return this.selectedBroker;
  };
  public getSelectedBrokerIndex = () => {
    return this.myBrokers.findIndex((b) => b.id === this.getSelectedBroker());
  };
  public getCurrentBrokerIndex = (id: string) => {
    return this.myBrokers.findIndex((b) => b.id === id);
  };
  public shiftBrokerLoad = () => {
    const currentBrokerId = this.getSelectedBroker();
    console.log("currentBrokerId:", currentBrokerId);
    const currentBrokerIndex = this.getSelectedBrokerIndex();
    console.log("currentBrokerIndex:", currentBrokerIndex);
    const newBrokerIndex = (currentBrokerIndex + 1) % this.myBrokers.length;
    console.log("newBrokerIndex:", newBrokerIndex);
    const newBrokerId = this.myBrokers[newBrokerIndex].id;
    console.log("newBrokerId:", newBrokerId);
    this.selectedBroker = newBrokerId;
    for (const subscription of this.clientSubscriptionPackets) {
      this.myBrokers[newBrokerIndex].connection.write(subscription);
    }
    this.myBrokers.splice(currentBrokerIndex, 1);
    this.updateBrokerTopicMapping(
      this.clientSubscriptionTopics,
      currentBrokerId,
      newBrokerId
    );
  };
  public static initBrokers = () => {
    BrokerLB.brokers = {
      Broker0_1883: {
        port: 1885,
        host: "127.0.0.1",
      },
      Broker1_1884: {
        port: 1886,
        host: "127.0.0.1",
      },
    };
  };
  public static addBrokerTopicMapping = (topic: string, brokerId: string) => {
    if (BrokerLB.brokerTopicMap[topic]) {
      BrokerLB.brokerTopicMap[topic].push(brokerId);
    } else {
      BrokerLB.brokerTopicMap[topic] = [brokerId];
    }
    console.log("Updated BrokerLB.brokerTopics:");
    console.log(JSON.stringify(BrokerLB.brokerTopicMap));
  };
  private updateBrokerTopicMapping = (
    topics: Array<string>,
    oldBrokerId: string,
    newBrokerId: string
  ) => {
    for (const topic of topics) {
      BrokerLB.brokerTopicMap[topic] = BrokerLB.brokerTopicMap[topic].filter(
        (broker) => broker !== oldBrokerId
      );
      BrokerLB.brokerTopicMap[topic].push(newBrokerId);
    }
    console.log("Updated BrokerLB.brokerTopics:");
    console.log(JSON.stringify(BrokerLB.brokerTopicMap));
  };
}

export default BrokerLB;
