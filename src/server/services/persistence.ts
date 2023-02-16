import aedesMemoryPersistence, { AedesPersistence } from "aedes-persistence";
import { promisify } from "util";
import debug from "debug";

class PersistenceService {
  private static persistence: AedesPersistence | null = null;
  private constructor() {}
  public static initPersistence() {
    const debugFactory = debug("zilmqtt:services:PersistenceService");
    this.persistence = aedesMemoryPersistence();
    debugFactory("Persistence initiated!");
  }
  public static getPersistence(): AedesPersistence {
    if (!this.persistence) {
      this.initPersistence();
    }
    return this.persistence!;
  }
  public static async getClientsByTopic(topic: string): Promise<Array<string>> {
    const debugFactory = debug(
      "zilmqtt:services:PersistenceService:getClientsByTopic"
    );
    return new Promise((resolve, reject) => {
      let data: Array<string> = [];
      const readableStream = this.persistence?.getClientList(topic);
      readableStream?.setEncoding("utf-8");
      readableStream?.on("data", (chunk) => {
        data.push(chunk);
      });
      readableStream?.on("end", () => {
        debugFactory(data);
        resolve(data);
      });
      readableStream?.on("error", (err) => {
        reject(err);
      });
    });
  }
}

export default PersistenceService;
