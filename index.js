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
    console.log("\n----> Listing blobs...");

    // List the blob(s) in the container.
    const blobs = [];
    for await (const blob of containerClient.listBlobsFlat()) {
      // Get Blob Client from name, to get the URL
      const tempBlockBlobClient = containerClient.getBlockBlobClient(blob.name);
      blobs.push(tempBlockBlobClient);
      console.log(`\n\tname: ${blob.name}\n\tURL: ${tempBlockBlobClient.url}\n`);
    }
    for (const blob of blobs) {
      if (blob.name.includes(".parquet")) {
        await verifyDirectory(`${__dirname}/parquets/${blob.name}`);
        await blob.downloadToFile(`${__dirname}/parquets/${blob.name}`);
        console.log("\n\t🔺️ Saved blob content...", blob.name);

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
