import { promises as fs } from "fs";
import path from "path";

export const verifyDirectory = async (filePath) => {
  const directory = path.dirname(filePath);

  try {
    const stats = await fs.stat(directory);
    if (stats.isDirectory()) return;
  } catch (error) {
    if (error.code === "ENOENT") {
      await fs.mkdir(directory, { recursive: true });
      console.log(`\n🟢 Directory '${directory}' created.`);
    } else {
      console.error("Error:", error);
    }
  }
};

export const clearParquetFiles = async () => {
  const directory = "./parquets";

  try {
    const stats = await fs.stat(directory);

    if (stats.isDirectory()) {
      await fs.rm(directory, { recursive: true });
      console.log(`\n----> Contents of '${directory}' deleted.`);
    }
  } catch (error) {
    if (error.code === "ENOENT") {
      await fs.mkdir(directory, { recursive: true });
      console.log(`\n----> No directory to delete.\t----> Directory '${directory}' created`);
    } else {
      console.error("Error:", error);
    }
  }
};
