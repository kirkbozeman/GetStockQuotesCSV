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

            SQLConnect.GetQuotesFromLastDt("SPY");
            SQLConnect.GetQuotesFromLastDt("QQQ");
            SQLConnect.GetQuotesFromLastDt("DIA");
            SQLConnect.GetQuotesFromLastDt("IWM");

        }
    }
}
