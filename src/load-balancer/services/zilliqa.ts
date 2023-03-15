import { BN, bytes, Long, Zilliqa } from "@zilliqa-js/zilliqa";
import { Contract } from "@zilliqa-js/contract";
import { default as debugFactory } from "debug";

import { BrokerConnectionParams, BrokerTopicMap } from "./dns";

type ZilliqaDNSRecords = {
  [hostname: string]: Array<string>;
};

type ZilliqaBrokerTopicMap = {
  [topic: string]: Array<string>;
};

type ZilliqaConfig = {
  contract: Contract;
  zilliqa: Zilliqa;
  contractParams: {
    amount: BN;
    gasLimit: Long.Long;
    gasPrice: BN;
    version: number;
  };
};

class ZilliqaService {
  private static config: ZilliqaConfig;
  constructor() {
    const debug = debugFactory(
      "zilmqtt:load-balancer:ZilliqaService:constructor"
    );
    //@ts-ignore
    ZilliqaService.config = {};
    ZilliqaService.config.zilliqa = new Zilliqa(
      process.env.ZILLIQA_NETWORK_URL ?? "https://dev-api.zilliqa.com"
    );
    const walletAddress = ZilliqaService.config.zilliqa.wallet.addByPrivateKey(
      process.env.ZILLIQA_WALLET_PRIVATE_KEY ?? ""
    );
    debug(`Wallet address imported: ${walletAddress}`);
    ZilliqaService.config.contract = ZilliqaService.config.zilliqa.contracts.at(
      process.env.ZILLIQA_CONTRACT_ADDRESS ?? ""
    );
    debug(
      `Contract address imported: ${ZilliqaService.config.contract.address}`
    );
    ZilliqaService.config.contractParams = {
      amount: new BN(0),
      gasLimit: Long.fromNumber(25000),
      gasPrice: new BN(2000000000),
      version: bytes.pack(333, 1),
    };
    debug(
      `Contract params initialized: ${JSON.stringify(
        ZilliqaService.config.contractParams
      )}`
    );
    debug(`Zilliqa Service Initialized`);
  }
  public static getDNSRecords = async (): Promise<BrokerConnectionParams> => {
    const debug = debugFactory(
      "zilmqtt:load-balancer:ZilliqaService:getDNSRecords"
    );
    const contract = ZilliqaService.config.contract;
    const dns = (await contract.getSubState("broker_dns"))[
      "broker_dns"
    ] as ZilliqaDNSRecords;
    const parsedDNS: BrokerConnectionParams = {};
    Object.keys(dns).map((hostname) => {
      //@ts-ignore
      parsedDNS[hostname] = {};
      parsedDNS[hostname].host = dns[hostname][0];
      parsedDNS[hostname].port = +dns[hostname][1];
    });
    debug("Fetched DNS records from Zilliqa:");
    debug(JSON.stringify(dns));
    return parsedDNS;
  };

  public static getBrokerTopicMap = async (): Promise<BrokerTopicMap> => {
    const debug = debugFactory(
      "zilmqtt:load-balancer:ZilliqaService:getBrokerTopicMap"
    );
    const contract = ZilliqaService.config.contract;
    const topicMap = (await contract.getSubState("topic_assignment"))[
      "topic_assignment"
    ] as ZilliqaBrokerTopicMap;
    debug("Fetched Topic Map from Zilliqa:");
    debug(JSON.stringify(topicMap));
    return topicMap;
  };

  public static registerDNS = async (
    hostname: string,
    ip: string,
    port: string
  ): Promise<void> => {
    const debug = debugFactory(
      "zilmqtt:load-balancer:ZilliqaService:registerDNS"
    );
    const contract = ZilliqaService.config.contract;
    const args = [
      {
        vname: "hostname",
        type: "String",
        value: hostname,
      },
      {
        vname: "ip",
        type: "String",
        value: ip,
      },
      {
        vname: "port",
        type: "String",
        value: port,
      },
    ];
    debug("registerDNS() called with the following arguments:");
    debug(JSON.stringify(args));
    const timeStart = process.hrtime.bigint();
    const callTxn = await contract.call(
      "RegisterDNS",
      args,
      ZilliqaService.config.contractParams
    );
    const txnReceipt = callTxn?.getReceipt();
    debug(
      "Time elapsed since transaction:",
      (
        (process.hrtime.bigint() - timeStart) /
        BigInt(1000 * 1000 * 1000)
      ).toString(),
      "seconds."
    );
    debug(`Transaction: ${JSON.stringify(txnReceipt)}`);
  };
  public static clearDNSCache = async (): Promise<void> => {
    const debug = debugFactory(
      "zilmqtt:load-balancer:ZilliqaService:clearDNSCache"
    );
    debug("Clearing previous DNS cache...");
    const contract = ZilliqaService.config.contract;
    const timeStart = process.hrtime.bigint();
    const callTxn = await contract.call(
      "ClearDNSCache",
      [],
      ZilliqaService.config.contractParams
    );
    const txnReceipt = callTxn?.getReceipt();
    debug(
      "Time elapsed since transaction:",
      (
        (process.hrtime.bigint() - timeStart) /
        BigInt(1000 * 1000 * 1000)
      ).toString(),
      "seconds."
    );
    debug(`Transaction: ${JSON.stringify(txnReceipt)}`);
  };

  public static updateTopicAssignments = async (
    topic: string,
    brokers: Array<string>
  ): Promise<void> => {
    const debug = debugFactory(
      "zilmqtt:load-balancer:ZilliqaService:updateTopicAssignments"
    );
    const args = [
      {
        vname: "topic",
        type: "String",
        value: topic,
      },
      {
        vname: "brokers",
        type: "List String",
        value: brokers,
      },
    ];
    const contract = ZilliqaService.config.contract;
    debug("updateTopicAssignments() called with the following arguments:");
    debug(JSON.stringify(args));
    const timeStart = process.hrtime.bigint();
    const callTxn = await contract.call(
      "UpdateTopicAssignments",
      //@ts-ignore
      args,
      ZilliqaService.config.contractParams
    );
    const txnReceipt = callTxn?.getReceipt();
    debug(
      "Time elapsed since transaction:",
      (
        (process.hrtime.bigint() - timeStart) /
        BigInt(1000 * 1000 * 1000)
      ).toString(),
      "seconds."
    );
    debug(`Transaction: ${JSON.stringify(txnReceipt)}`);
  };
}

export default ZilliqaService;
