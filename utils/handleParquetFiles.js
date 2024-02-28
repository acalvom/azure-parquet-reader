import { Database } from "duckdb-async";

export const paquetFileToDBTable = async (parquetFile) => {
  const tableName = "parquet_as_table";
  const createTableFromParquet = `create table ${tableName} as select * from read_parquet($1)`;
  const selectFromTable = `select * from ${tableName} limit 50`;

  const db = await Database.create(":memory:");

  const parquet_as_table = await db.all(createTableFromParquet, [parquetFile]);
  console.log(`\n\t\🔹 Table created from parquet with ${parquet_as_table[0].Count} rows`);

  const table_content = await db.all(selectFromTable);
  // TODO: store this content in a Docker DB

  await db.close();
};
