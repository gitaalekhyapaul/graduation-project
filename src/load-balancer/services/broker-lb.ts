import { createConnection, Socket } from "net";
import { generate } from "mqtt-packet";

type Brokers = {
  connection: Socket;
  id: string;
};
type BrokerConnectionParams = {
  host: string;
  port: number;
  id: string;
};

type BrokerTopicMap = {
  [topic: string]: Array<string>;
};

class BrokerLB {
  public static brokers: BrokerConnectionParams[] = [];
  public static brokerTopicMap: BrokerTopicMap = {};
  public clientSubscriptionPackets: Buffer[];
  public clientSubscriptionTopics: string[];
  public selectedBroker: string;
  public myBrokers: Brokers[];
  constructor() {
    //! Change to some round-robin algorithm
    this.myBrokers = BrokerLB.brokers.map((connectionParam) => {
      return {
        id: connectionParam.id,
        connection: createConnection({
          host: connectionParam.host,
          port: connectionParam.port,
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
    BrokerLB.brokers = [
      {
        port: 1883,
        host: "127.0.0.1",
        id: "Broker0_1883",
      },
      {
        port: 1884,
        host: "127.0.0.1",
        id: "Broker1_1884",
      },
    ];
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
  public static publishPackets = (topic: string, packet: Buffer) => {
    const brokerIds = BrokerLB.brokerTopicMap[topic];
    for (const brokerId of brokerIds) {
      console.log(
        "Sending PUBLISH on topic'",
        topic,
        "' to BrokerID '",
        brokerId,
        "'"
      );
      const connDetails = BrokerLB.brokers.find((b) => b.id === brokerId)!;
      const tempConn = createConnection({
        host: connDetails.host,
        port: connDetails.port,
      });
      tempConn.write(packet);
      tempConn.on("data", (data) => {
        console.log("Broker received PUBLISH");
      });
    }
  };
}

export default BrokerLB;
