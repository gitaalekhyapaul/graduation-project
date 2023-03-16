import { BN, bytes, Long, Zilliqa } from "@zilliqa-js/zilliqa";
import { Contract } from "@zilliqa-js/contract";
import { PublishPacket } from "aedes:packet";
import debug from "debug";

import DeadLetterExchangeService from "./dlx";

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
    const debugFactory = debug(
      "zilmqtt:broker:services:ZilliqaService:constructor"
    );
    //@ts-ignore
    ZilliqaService.config = {};
    ZilliqaService.config.zilliqa = new Zilliqa(
      process.env.ZILLIQA_NETWORK_URL ?? "https://dev-api.zilliqa.com"
    );
    const walletAddress = ZilliqaService.config.zilliqa.wallet.addByPrivateKey(
      process.env.ZILLIQA_WALLET_PRIVATE_KEY ?? ""
    );
    debugFactory(`Wallet address imported: ${walletAddress}`);
    ZilliqaService.config.contract = ZilliqaService.config.zilliqa.contracts.at(
      process.env.ZILLIQA_CONTRACT_ADDRESS ?? ""
    );
    debugFactory(
      `Contract address imported: ${ZilliqaService.config.contract.address}`
    );
    ZilliqaService.config.contractParams = {
      amount: new BN(0),
      gasLimit: Long.fromNumber(25000),
      gasPrice: new BN(2000000000),
      version: bytes.pack(333, 1),
    };
    debugFactory(
      `Contract params initialized: ${JSON.stringify(
        ZilliqaService.config.contractParams
      )}`
    );
    debugFactory(`Zilliqa Service Initialized`);
  }
  public static getRetainedMessages = async (
    topic: string
  ): Promise<Array<PublishPacket>> => {
    const debugFactory = debug(
      `zilmqtt:broker:services:ZilliqaService:getRetainedMessages:topic:${topic}`
    );
    const contract = ZilliqaService.config.contract;
    const messages = await contract.getSubState("retained_messages", [
      `${topic}`,
    ]);
    const retainedMessages = (messages?.["retained_messages"]?.[topic] ??
      []) as Array<any>;
    const parsedMessages = retainedMessages.map((e) => {
      let temp = JSON.parse(Buffer.from(e, "base64").toString("utf-8"));
      temp.payload = Buffer.from(temp.payload, "base64");
      return temp;
    });
    debugFactory(
      "Retrieved retained messages:",
      JSON.stringify(parsedMessages)
    );
    return parsedMessages;
  };

  public static setRetainedMessages = async (
    topic: string,
    packet: PublishPacket
  ) => {
    const debugFactory = debug(
      `zilmqtt:broker:services:ZilliqaService:setRetainedMessages:topic:${topic}`
    );
    if (!packet.retain) return;
    const updatedPacket = DeadLetterExchangeService.encodePacket(packet);
    const contract = ZilliqaService.config.contract;
    debugFactory(`Setting retained message for topic '${topic}'`);
    const args = [
      {
        vname: "topic",
        type: "String",
        value: topic,
      },
      {
        vname: "message",
        type: "String",
        value: updatedPacket,
      },
    ];
    debugFactory("appendMessage() called with the following parameters:");
    debugFactory(JSON.stringify(args));
    const timeStart = process.hrtime.bigint();
    const callTxn = await contract.call(
      "AppendMessage",
      args,
      ZilliqaService.config.contractParams
    );
    const txnReceipt = callTxn?.getReceipt();
    debugFactory(
      "Time elapsed since transaction:",
      (
        (process.hrtime.bigint() - timeStart) /
        BigInt(1000 * 1000 * 1000)
      ).toString(),
      "seconds."
    );
    debugFactory(`Transaction: ${JSON.stringify(txnReceipt)}`);
  };

  public static setDeadLetterQueue = async (
    records: Record<string, string>
  ) => {
    const debugFactory = debug(
      "zilmqtt:broker:services:ZilliqaService:setDeadLetterQueue"
    );
    debugFactory(
      "ZilliqaService.setDeadLetterQueue called with the records:",
      JSON.stringify(records)
    );
    const contract = ZilliqaService.config.contract;
    for (const clientId in records) {
      if (Object.prototype.hasOwnProperty.call(records, clientId)) {
        const deadLetter = records[clientId];
        const args = [
          {
            vname: "clientid",
            type: "String",
            value: clientId,
          },
          {
            vname: "message",
            type: "String",
            value: deadLetter,
          },
        ];
        debugFactory(
          "queueDeadLetters() called with the following parameters:"
        );
        debugFactory(JSON.stringify(args));
        const timeStart = process.hrtime.bigint();
        const callTxn = await contract.call(
          "QueueDeadLetters",
          args,
          ZilliqaService.config.contractParams
        );
        const txnReceipt = callTxn?.getReceipt();
        debugFactory(
          "Time elapsed since transaction:",
          (
            (process.hrtime.bigint() - timeStart) /
            BigInt(1000 * 1000 * 1000)
          ).toString(),
          "seconds."
        );
        debugFactory(`Transaction: ${JSON.stringify(txnReceipt)}`);
      }
    }
  };

  public static getDeadLetterQueue = async (
    clientId: string
  ): Promise<Array<PublishPacket>> => {
    const debugFactory = debug(
      `zilmqtt:broker:services:ZilliqaService:getDeadLetterQueue:client:${clientId}`
    );
    debugFactory(
      "ZilliqaService.getDeadLetterQueue called with the clientId:",
      clientId
    );
    const contract = ZilliqaService.config.contract;
    const messages = await contract.getSubState("dead_letter_queue", [
      `${clientId}`,
    ]);
    const deadLetterQueue = (messages?.["dead_letter_queue"]?.[clientId] ??
      []) as Array<string>;
    const parsedMessages = deadLetterQueue.map((e) => {
      return DeadLetterExchangeService.decodePacket(e);
    });
    if (parsedMessages.length === 0) {
      debugFactory("No dead letters found for client.");
      return parsedMessages;
    }
    debugFactory(
      "Retrieved dead letter messages:",
      JSON.stringify(parsedMessages)
    );
    Promise.all([
      (async () => {
        debugFactory("Attempting to clear dead letter queue...");
        const args = [
          {
            vname: "clientid",
            type: "String",
            value: clientId,
          },
        ];
        debugFactory(
          "dequeueDeadLetters() called with the following parameters:"
        );
        debugFactory(JSON.stringify(args));
        const timeStart = process.hrtime.bigint();
        const callTxn = await contract.call(
          "DequeueDeadLetters",
          args,
          ZilliqaService.config.contractParams
        );
        const txnReceipt = callTxn?.getReceipt();
        debugFactory(
          "Time elapsed since transaction:",
          (
            (process.hrtime.bigint() - timeStart) /
            BigInt(1000 * 1000 * 1000)
          ).toString(),
          "seconds."
        );
        debugFactory(`Transaction: ${JSON.stringify(txnReceipt)}`);
      })(),
    ]).catch((e) => debugFactory(e));
    return parsedMessages;
  };
}

export default ZilliqaService;
