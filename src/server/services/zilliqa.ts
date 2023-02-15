import { BN, bytes, Long, Zilliqa } from "@zilliqa-js/zilliqa";
import { PublishPacket } from "aedes:packet";
import { IPublishPacket } from "mqtt-packet";
const zilliqa = new Zilliqa(
  process.env.NETWORK_URL ?? "https://dev-api.zilliqa.com"
);

export const getRetainedMessages = async (
  topic: string
): Promise<Array<IPublishPacket>> => {
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
  packet: IPublishPacket
) => {
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
  console.log(`Wallet Address: ${walletAddress}`);
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
  const callTxn = await contract.call("AppendMessage", args, {
    amount: new BN(0),
    gasLimit: Long.fromNumber(25000),
    gasPrice: new BN(2000000000),
    version: bytes.pack(333, 1),
  });
  const txnReceipt = callTxn?.getReceipt();
  console.log(`Transaction: ${JSON.stringify(txnReceipt)}`);
};

export const setDeadLetterQueue = async (
  records: Record<string, string>
) => {
  console.log("setDeadLetterQueue called with the records:");
  console.dir(records, { depth: null });
};

export const getDeadLetterQueue = async (clientId: string) => {
  console.log("getDeadLetterQueue called with the clientId:", clientId);
};
