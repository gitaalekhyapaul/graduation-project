import aedesMemoryPersistence, { AedesPersistence } from "aedes-persistence";
import { promisify } from "util";

class PersistenceService {
  private static persistence: AedesPersistence | null = null;
  private constructor() {}
  public static initPersistence() {
    this.persistence = aedesMemoryPersistence();
    console.log("Persistence initiated!");
  }
  public static getPersistence(): AedesPersistence {
    if (!this.persistence) {
      this.initPersistence();
    }
    return this.persistence!;
  }
  public static async getClientsByTopic(topic: string): Promise<Array<string>> {
    return new Promise((resolve, reject) => {
      let data: Array<string> = [];
      const readableStream = this.persistence?.getClientList(topic);
      readableStream?.setEncoding("utf-8");
      readableStream?.on("data", (chunk) => {
        data.push(chunk);
      });
      readableStream?.on("end", () => {
        resolve(data);
      });
      readableStream?.on("error", (err) => {
        reject(err);
      });
    });
  }
}

export default PersistenceService;
