using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GetStockQuotesCSV
{
    class SQLConnect
    {

        public static void GetQuotesFromLastDt(string symbol)
        {

            // get last date of quote data in SQL
            DateTime dtFrom = new DateTime();
            SqlConnection conn = new SqlConnection("Data Source=KIRKBOZEMAN98C1\\SQLEXPRESS;Initial Catalog=Sandbox;Integrated Security=SSPI;");
            using (SqlCommand cmd = new SqlCommand("dbo.GetLastDtForSymbol", conn))
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.Add("@symbol", SqlDbType.VarChar, 10);
                cmd.Parameters.Add("@lastdt", SqlDbType.DateTime).Direction = ParameterDirection.Output;
                cmd.Parameters["@symbol"].Value = symbol;

                conn.Open();
                cmd.ExecuteNonQuery();
                conn.Close();

                dtFrom = Convert.ToDateTime(cmd.Parameters["@lastdt"].Value);
            
            }

            // get all data from max date available
            if (DateTime.Today != dtFrom)
            { 
                // remember, Yahoo is inclusive (includes dtFrom sent)
                string url = @"http://ichart.yahoo.com/table.csv?s=" + symbol + "&a=" + (dtFrom.Month - 1).ToString() + "&b=" + dtFrom.Day.ToString() + "&c=" + dtFrom.Year.ToString();
                string strCSV;
                string[] dataArray;

                //puts csv into string
                using (var webpage = new System.Net.WebClient())
                {
                    strCSV = webpage.DownloadString(url);
                    dataArray = strCSV.Split("\n".ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
                }

                string query = "INSERT INTO dbo.[STOCK_DATA_FLAT] " +
                "VALUES (@symbol, @date, @open, @high, @low, @close, @volume, @adj_close, @pull_datetime)";

                for (int i = 1; i < dataArray.Length; i++) // skip CSV header row
                {
                    string[] rowArray = dataArray[i].Split(",".ToCharArray(), StringSplitOptions.RemoveEmptyEntries);

                    using (SqlCommand cmd = new SqlCommand(query, conn))
                    {

                        cmd.Parameters.Add("@symbol", SqlDbType.VarChar);
                        cmd.Parameters.Add("@date", SqlDbType.VarChar);
                        cmd.Parameters.Add("@open", SqlDbType.VarChar);
                        cmd.Parameters.Add("@high", SqlDbType.VarChar);
                        cmd.Parameters.Add("@low", SqlDbType.VarChar);
                        cmd.Parameters.Add("@close", SqlDbType.VarChar);
                        cmd.Parameters.Add("@volume", SqlDbType.VarChar);
                        cmd.Parameters.Add("@adj_close", SqlDbType.VarChar);
                        cmd.Parameters.Add("@pull_datetime", SqlDbType.VarChar);


                        cmd.Parameters["@symbol"].Value = symbol; // can use array location or param name
                        cmd.Parameters["@date"].Value = rowArray[0];
                        cmd.Parameters["@open"].Value = rowArray[1];
                        cmd.Parameters["@high"].Value = rowArray[2];
                        cmd.Parameters["@low"].Value = rowArray[3];
                        cmd.Parameters["@close"].Value = rowArray[4];
                        cmd.Parameters["@volume"].Value = rowArray[5];
                        cmd.Parameters["@adj_close"].Value = rowArray[6];
                        cmd.Parameters["@pull_datetime"].Value = DateTime.Now;

                        conn.Open();
                        cmd.ExecuteNonQuery();
                        conn.Close();
                    }

                }
            }
            else
            {
                Console.WriteLine("No new data available.");
            }

        }



    }
}
