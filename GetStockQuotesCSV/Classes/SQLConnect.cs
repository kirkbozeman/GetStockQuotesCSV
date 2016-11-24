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

        public static void GetQuotes(string symbol, DateTime dtFrom)
        {

            // remember, Yahoo is inclusive (includes dtFrom sent)
            string url = @"http://ichart.yahoo.com/table.csv?s=" + symbol + "&a=" + (dtFrom.Month - 1).ToString() + "&b=" + dtFrom.Day.ToString() + "&c=" + dtFrom.Year.ToString();
            string strCSV;

            //puts csv into string
            using (var webpage = new System.Net.WebClient())
            {
                strCSV = webpage.DownloadString(url);
            }

            string[] dataArray = strCSV.Split("\n".ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
            string query = "INSERT INTO dbo.[STOCK_DATA_FLAT] " +
                "VALUES (@symbol, @date, @open, @high, @low, @close, @volume, @adj_close, @pull_datetime)";
            SqlConnection conn = new SqlConnection("Data Source=KIRKBOZEMAN98C1\\SQLEXPRESS;Initial Catalog=Sandbox;Integrated Security=SSPI;");
            conn.Open();

            for (int i = 1; i < dataArray.Length; i++) // skip header row
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

                    cmd.ExecuteNonQuery();
                }

            }

            conn.Close();
        }


        public static DateTime GetLastDt(string symbol)
        {
            DateTime returnDate = new DateTime();
            string query =
            "DECLARE @lastdt datetime " +
            "EXEC GetLastDtForSymbol '" + symbol + "', @lastdt = @lastdt OUTPUT";

            //            "SELECT ISNULL(DATEADD(d,1,MAX(CONVERT(datetime,[DATE]))),'1-1-1900') AS [next_day] " +
            //           "FROM [dbo].[STOCK_DATA_FLAT] WHERE SYMBOL = '" + symbol + "'";

            SqlConnection conn = new SqlConnection("Data Source=KIRKBOZEMAN98C1\\SQLEXPRESS;Initial Catalog=Sandbox;Integrated Security=SSPI;");
            conn.Open();

            using (SqlCommand cmd = new SqlCommand(query, conn))
            {
                returnDate = (DateTime)cmd.ExecuteScalar();
            }
            conn.Close();

            return returnDate;

        }


        public static void GetQuotesFromLastDt(string symbol)
        {

            // get last date of quote data
            DateTime dtFrom = new DateTime();
//            string query =
    //        "SELECT ISNULL(DATEADD(d,1,MAX(CONVERT(datetime,[DATE]))),'1-1-1900') AS [next_day] " +
     //       "FROM [dbo].[STOCK_DATA_FLAT] WHERE SYMBOL = '" + symbol + "'";
  //          "DECLARE @lastdt datetime " +
   //         "EXEC GetLastDtForSymbol '" + symbol + "', @lastdt = @lastdt OUTPUT";


            using (SqlConnection conn = new SqlConnection("Data Source=KIRKBOZEMAN98C1\\SQLEXPRESS;Initial Catalog=Sandbox;Integrated Security=SSPI;"))
            using (SqlCommand cmd = new SqlCommand("dbo.GetLastDtForSymbol", conn))
            {
                conn.Open();

                dtFrom = (DateTime)cmd.ExecuteScalar();


            }



            // remember, Yahoo is inclusive (includes dtFrom sent)
            string url = @"http://ichart.yahoo.com/table.csv?s=" + symbol + "&a=" + (dtFrom.Month - 1).ToString() + "&b=" + dtFrom.Day.ToString() + "&c=" + dtFrom.Year.ToString();
            string strCSV;

            //puts csv into string
            using (var webpage = new System.Net.WebClient())
            {
                strCSV = webpage.DownloadString(url);
            }

            string[] dataArray = strCSV.Split("\n".ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
            query = "INSERT INTO dbo.[STOCK_DATA_FLAT] " +
                "VALUES (@symbol, @date, @open, @high, @low, @close, @volume, @adj_close, @pull_datetime)";
 //           SqlConnection conn = new SqlConnection("Data Source=KIRKBOZEMAN98C1\\SQLEXPRESS;Initial Catalog=Sandbox;Integrated Security=SSPI;");
//            conn.Open();

            for (int i = 1; i < dataArray.Length; i++) // skip header row
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

                    cmd.ExecuteNonQuery();
                }

            }

            conn.Close();
        }



    }
}
