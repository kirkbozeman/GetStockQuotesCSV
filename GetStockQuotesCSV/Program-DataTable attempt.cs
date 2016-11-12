using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Data;
using Microsoft.VisualBasic.FileIO;
using System.Data.SqlClient;

namespace GetStockQuotesCSV
{
    class Program
    {
        static void Main(string[] args)
        {

            // this Datatable is unnecessary, we can write straight to the server

            // create Datatable
            DataTable dataTable = new DataTable();
            dataTable.Clear();
            dataTable.Columns.Add("SYMBOL");
            dataTable.Columns.Add("DATE");
            dataTable.Columns.Add("OPEN");
            dataTable.Columns.Add("HIGH");
            dataTable.Columns.Add("LOW");
            dataTable.Columns.Add("CLOSE");
            dataTable.Columns.Add("VOLUME");
            dataTable.Columns.Add("ADJ_CLOSE");
            dataTable.Columns.Add("Pull_Datetime");


            string symbol = "SPY";
            string url = @"http://ichart.yahoo.com/table.csv?s=" + symbol;
            string strCSV;

            //puts csv into string
            using (var webpage = new WebClient())
            {
                strCSV = webpage.DownloadString(url); 
            }

            string[] dataArray = strCSV.Split("\n".ToCharArray(), StringSplitOptions.RemoveEmptyEntries);

            foreach (var row in dataArray)
            {
                //                Console.WriteLine(row);
                DataRow dtRow = dataTable.NewRow();
                dtRow["SYMBOL"] = symbol;
                dtRow["DATE"] = row[0];
                dtRow["OPEN"] = row[1];
                dtRow["HIGH"] = row[2];
                dtRow["LOW"] = row[3];
                dtRow["CLOSE"] = row[4];
                dtRow["VOLUME"] = row[5];
                dtRow["ADJ_CLOSE"] = row[6];
                dtRow["Pull_Datetime"] = row[7];
                dataTable.Rows.Add(dtRow);
            }

/*             foreach (DataRow r in dataTable.Rows)
             {
                 Console.WriteLine(r.ToString());
             } */

            
            using (SqlConnection connection = new SqlConnection("Data Source=KIRKBOZEMAN98C1\\SQLEXPRESS;Initial Catalog=Sandbox;Integrated Security=SSPI;"))
            {
                connection.Open();
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName = "dbo.STOCK_DATA";
                    bulkCopy.WriteToServer(dataTable);
                }

            }
            

            // just query for the last date in the table for the stock and pull from there to current date


            Console.ReadKey();

        }
    }
}
