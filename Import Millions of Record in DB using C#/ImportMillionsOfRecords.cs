using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Collections.Generic;

class ImportMillionsOfRecords
{
    static void Main(string[] args)
    {
        string filePath = @"C:\Imports\filename.txt"; // Change path or csv
        string connectionString = "your_connection_string_here";

        int batchSize = 10000;
		
		//Avoids storing all 3M rows in memory
        DataTable table = CreateTableSchema();
		
        int lineNumber = 0;

	    //Reads file line by line, low memory ----- StreamReader
        using (StreamReader reader = new StreamReader(filePath))
        {
            string line;
            bool isFirstLine = true;

            while ((line = reader.ReadLine()) != null)
            {
                if (isFirstLine)
                {
                    isFirstLine = false; // skip header
                    continue;
                }

                string[] fields = SplitCsvLine(line);
                if (fields.Length != table.Columns.Count)
                {
                    Console.WriteLine($"Skipping row #{lineNumber + 1} due to column mismatch.");
                    continue;
                }

                table.Rows.Add(fields);
                lineNumber++;

                // When batch is ready, insert to DB
                if (table.Rows.Count >= batchSize)
                {
                    BulkInsert(table, connectionString);
					
					//Frees up memory between batches
                    table.Clear();
                    Console.WriteLine($"Inserted {lineNumber} records...");
                }
            }

            // Final batch
            if (table.Rows.Count > 0)
            {
                BulkInsert(table, connectionString);
                Console.WriteLine($"Inserted final {table.Rows.Count} records.");
            }
        }

        Console.WriteLine("All records imported.");
    }

    static DataTable CreateTableSchema()
    {
        var table = new DataTable();
        table.Columns.Add("columname1", typeof(string));
        table.Columns.Add("columname2", typeof(string));
        table.Columns.Add("columname3", typeof(string));
        table.Columns.Add("columname4", typeof(string));
        table.Columns.Add("columname5", typeof(string));
        table.Columns.Add("columname6", typeof(string));
        table.Columns.Add("columname7", typeof(string));
        table.Columns.Add("columname8", typeof(string));
        table.Columns.Add("columname9", typeof(string));
        table.Columns.Add("columname10", typeof(string));
        table.Columns.Add("columname11", typeof(string));
        return table;
    }

    static void BulkInsert(DataTable table, string connectionString)
    {
        using (SqlConnection conn = new SqlConnection(connectionString))
        {
            conn.Open();
			
			//Fastest way to import to SQL Server  -- SqlBulkCopy
			
			
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn))
            {
                bulkCopy.DestinationTableName = "ImportedOrganizations";
                bulkCopy.WriteToServer(table);
            }
        }
    }

    static string[] SplitCsvLine(string line)
    {
        List<string> fields = new List<string>();
        bool inQuotes = false;
        string currentField = "";

        for (int i = 0; i < line.Length; i++)
        {
            char c = line[i];

            if (c == '"')
            {
                if (inQuotes && i + 1 < line.Length && line[i + 1] == '"')
                {
                    currentField += '"';
                    i++;
                }
                else
                {
                    inQuotes = !inQuotes;
                }
            }
            else if (c == ';' && !inQuotes)
            {
                fields.Add(currentField);
                currentField = "";
            }
            else
            {
                currentField += c;
            }
        }

        fields.Add(currentField); // Add last field
        return fields.ToArray();
    }
}
