using Oracle.DataAccess.Client;
using System;
using System.Collections.Generic;
using System.Text;

namespace FanTest.Trans
{
    public class Manager
    {
        public static string GetConnStringOfOra1()
        {
            return Helper.GetConnString4OraTNS("192.168.100.52", "1521", "bhdevcomber", "bhdata", "bhdata");
        }
        public static OracleConnection GetConnOfOra1()
        {
            return new OracleConnection(Manager.GetConnStringOfOra1());
        }

        public static bool TestConn1()
        {
            using (OracleConnection conn = GetConnOfOra1())
            {
                try
                {
                    conn.Open();
                    return true;
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                    return false;
                }
            }
        }
    }
}
