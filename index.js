import "dotenv/config";
import path from "path";
import { BlobServiceClient } from "@azure/storage-blob";
import { DefaultAzureCredential } from "@azure/identity";
import { fileURLToPath } from "url";
import { verifyDirectory, clearParquetFiles } from "./utils/handleDirectories.js";
import { paquetFileToDBTable } from "./utils/handleParquetFiles.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function main() {
  try {
    const accountName = process.env.AZURE_STORAGE_ACCOUNT_NAME;
    if (!accountName) throw Error("Azure Storage accountName not found");

    const blobServiceClient = new BlobServiceClient(
      `https://${accountName}.blob.core.windows.net`,
      new DefaultAzureCredential()
    );

    const containerClient = blobServiceClient.getContainerClient(process.env.AZURE_CONTAINER_NAME);

    for await (const blob of containerClient.listBlobsFlat()) {
      const blockBlobClient = containerClient.getBlockBlobClient(blob.name);
      if (blob.name.includes(".parquet")) {
        await verifyDirectory(`${__dirname}/parquets/${blob.name}`);
        console.log(`\n🔸 Name: ${blob.name}\tURL: ${blockBlobClient.url}`);
        await blockBlobClient.downloadToFile(`${__dirname}/parquets/${blob.name}`);
        console.log("\n\t🔺️ Saved parquet content...", blob.name);
        await paquetFileToDBTable(`${__dirname}/parquets/${blob.name}`);
      }
    }
  } catch (err) {
    console.error(`Error: ${err.message}`);
  }
}

await clearParquetFiles();
await main()
  .then(() => console.log("Done"))
  .catch((ex) => console.log(ex.message));
