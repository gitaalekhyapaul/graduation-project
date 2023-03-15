import ZilliqaService from "./zilliqa";
import { default as debugFactory } from "debug";

export type BrokerConnectionParams = {
  [hostname: string]: {
    host: string;
    port: number;
  };
};

export type BrokerTopicMap = {
  [topic: string]: Array<string>;
};

class DNSService {
  private static brokerConnectionParams: BrokerConnectionParams;
  private static brokerTopicMap: BrokerTopicMap;
  constructor() {
    const debug = debugFactory("zilmqtt:load-balancer:DNSService:constructor");
    DNSService.brokerConnectionParams = {};
    DNSService.brokerTopicMap = {};
    new ZilliqaService();
    const interval = +(process.env.DNS_INTERVAL ?? 60000);
    debug(`Calling refreshDNS() every ${interval / 1000} seconds.`);
    setInterval(() => DNSService.refreshDNS(), interval);
    debug("DNS Service Initialized");
  }

  public static getDNS = async () => {
    return DNSService.brokerConnectionParams;
  };
  public static getBrokerTopicMap = async () => {
    return DNSService.brokerTopicMap;
  };
  public static setBrokerTopicMap = async (
    brokerTopicMap: BrokerTopicMap,
    topic: string
  ) => {
    const debug = debugFactory(
      "zilmqtt:load-balancer:DNSService:setBrokerTopicMap"
    );
    const updatedTopicMap = brokerTopicMap[topic];
    debug(`Topic Map of topic '${topic}' to be set:`);
    debug(JSON.stringify(updatedTopicMap));
    DNSService.brokerTopicMap[topic] = updatedTopicMap;
    ZilliqaService.updateTopicAssignments(topic, updatedTopicMap)
      .then((_) =>
        debug(`Topic Map for topic '${topic}' updated successfully in Zilliqa.`)
      )
      .catch((e) => {
        debug(`Error in updating topic Map for topic '${topic}' in Zilliqa:`);
        debug(e);
      });
    debug(`Topic Map for topic '${topic}' updated successfully locally.`);
  };
  public static initDNS = async () => {
    const debug = debugFactory("zilmqtt:load-balancer:DNSService:initDNS");
    debug("Initializing fresh new DNS...");
    await ZilliqaService.clearDNSCache();
    debug("DNS cache cleared.");
    const records = await ZilliqaService.getDNSRecords();
    debug("Records received from Zilliqa service:");
    debug(JSON.stringify(records));
    DNSService.brokerConnectionParams = records;
    const topicMap = await ZilliqaService.getBrokerTopicMap();
    debug("Topic Map received from Zilliqa service:");
    debug(JSON.stringify(topicMap));
    DNSService.brokerTopicMap = topicMap;
  };
  private static refreshDNS = async () => {
    const debug = debugFactory("zilmqtt:load-balancer:DNSService:refreshDNS");
    debug("Refreshing DNS...");
    const records = await ZilliqaService.getDNSRecords();
    debug("Records received from Zilliqa service:");
    debug(JSON.stringify(records));
    DNSService.brokerConnectionParams = records;
    const topicMap = await ZilliqaService.getBrokerTopicMap();
    debug("Topic Map received from Zilliqa service:");
    debug(JSON.stringify(topicMap));
    DNSService.brokerTopicMap = topicMap;
  };
  public static addDNSRecord = async (
    hostname: string,
    ip: string,
    port: string
  ) => {
    const debug = debugFactory("zilmqtt:load-balancer:DNSService:addDNSRecord");
    await ZilliqaService.registerDNS(hostname, ip, port);
    debug("DNS Register success for:");
    debug("Hostname:", hostname);
    debug("IP:", ip);
    debug("Port:", port);
    debug("Manually refreshing local DNS...");
    await DNSService.refreshDNS();
  };
}

export default DNSService;
