import { BN, bytes, Long, Zilliqa } from "@zilliqa-js/zilliqa";
import { PublishPacket } from "aedes:packet";
import debug from "debug";

import DeadLetterExchangeService from "./dlx";
const zilliqa = new Zilliqa(
  process.env.NETWORK_URL ?? "https://dev-api.zilliqa.com"
);

export const getRetainedMessages = async (
  topic: string
): Promise<Array<PublishPacket>> => {
  const debugFactory = debug("zilmqtt:getRetainedMessages");
  const contract = zilliqa.contracts.at(process.env.CONTRACT_ADDRESS ?? "");
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
  return parsedMessages;
};

export const setRetainedMessages = async (
  topic: string,
  packet: PublishPacket
) => {
  const debugFactory = debug("zilmqtt:setRetainedMessages");
  if (!packet.retain) return;
  const updatedPacket = Buffer.from(
    JSON.stringify({
      ...packet,
      payload: Buffer.from((packet as any).payload).toString("base64"),
    })
  ).toString("base64");
  const contract = zilliqa.contracts.at(process.env.CONTRACT_ADDRESS ?? "");
  const walletAddress = zilliqa.wallet.addByPrivateKey(
    process.env.WALLET_PRIVATE_KEY ?? ""
  );
  debugFactory(`Wallet Address: ${walletAddress}`);
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
  const timeStart = process.hrtime.bigint();
  const callTxn = await contract.call("AppendMessage", args, {
    amount: new BN(0),
    gasLimit: Long.fromNumber(25000),
    gasPrice: new BN(2000000000),
    version: bytes.pack(333, 1),
  });
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

export const setDeadLetterQueue = async (records: Record<string, string>) => {
  const debugFactory = debug("zilmqtt:setDeadLetterQueue");
  debugFactory(
    "setDeadLetterQueue called with the records:",
    JSON.stringify(records)
  );
  const contract = zilliqa.contracts.at(process.env.CONTRACT_ADDRESS ?? "");
  const walletAddress = zilliqa.wallet.addByPrivateKey(
    process.env.WALLET_PRIVATE_KEY ?? ""
  );
  debugFactory(`Wallet Address: ${walletAddress}`);
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
      const timeStart = process.hrtime.bigint();
      const callTxn = await contract.call("QueueDeadLetters", args, {
        amount: new BN(0),
        gasLimit: Long.fromNumber(25000),
        gasPrice: new BN(2000000000),
        version: bytes.pack(333, 1),
      });
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

export const getDeadLetterQueue = async (
  clientId: string
): Promise<Array<PublishPacket>> => {
  const debugFactory = debug("zilmqtt:getDeadLetterQueue");
  debugFactory("getDeadLetterQueue called with the clientId:", clientId);
  const contract = zilliqa.contracts.at(process.env.CONTRACT_ADDRESS ?? "");
  const messages = await contract.getSubState("dead_letter_queue", [
    `${clientId}`,
  ]);
  const retainedMessages = (messages?.["dead_letter_queue"]?.[clientId] ??
    []) as Array<string>;
  const parsedMessages = retainedMessages.map((e) => {
    return DeadLetterExchangeService.decodePacket(e);
  });
  if (parsedMessages.length === 0) {
    return parsedMessages;
  }
  const walletAddress = zilliqa.wallet.addByPrivateKey(
    process.env.WALLET_PRIVATE_KEY ?? ""
  );
  debugFactory(`Wallet Address: ${walletAddress}`);
  const args = [
    {
      vname: "clientid",
      type: "String",
      value: clientId,
    },
  ];
  const timeStart = process.hrtime.bigint();
  const callTxn = await contract.call("DequeueDeadLetters", args, {
    amount: new BN(0),
    gasLimit: Long.fromNumber(25000),
    gasPrice: new BN(2000000000),
    version: bytes.pack(333, 1),
  });
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
  return parsedMessages;
};
