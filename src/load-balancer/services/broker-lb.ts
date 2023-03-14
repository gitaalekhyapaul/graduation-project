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

class BrokerLB {
  public static brokers: BrokerConnectionParams[] = [];
  public clientSubscriptions: Buffer[];
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
    this.clientSubscriptions = [];
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
    const currentBrokerIndex = this.getSelectedBrokerIndex();
    console.log("currentBrokerIndex", currentBrokerIndex);
    const newBrokerIndex = (currentBrokerIndex + 1) % this.myBrokers.length;
    console.log("newBrokerIndex", newBrokerIndex);
    this.selectedBroker = this.myBrokers[newBrokerIndex].id;
    for (const subscription of this.clientSubscriptions) {
      this.myBrokers[newBrokerIndex].connection.write(subscription);
    }
    this.myBrokers.splice(currentBrokerIndex, 1);
  };
  public static initBrokers() {
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
  }
}

export default BrokerLB;
