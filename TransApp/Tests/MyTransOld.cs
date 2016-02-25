using Oracle.DataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Transactions;

namespace FanTest.Trans
{
    internal class MyTransOld
    {
        public static void Test1()
        {
            Console.WriteLine("\r\n==================\r\nCalling MyTrans.Test1()");

            //string connStr = Manager.GetConnStringOfOra1();
            string connStr = string.Format("Data Source={0};User ID={1};Password={2};Persist Security Info=True;Pooling=true",
                "bhdevcomber", //Helper.GetDatasource4OraTNS("192.168.100.52", "1521", "bhdevcomber"), //
                "bhdata",
                "bhdata");

            DbAccess db = new DbAccess(connStr);

            Console.WriteLine("Last-1:{0}", db.SelectByCode("1"));
            Console.WriteLine("------------------");

            try
            {
                bool commit = false;
                using (MyTransactionScopeOld scope = new MyTransactionScopeOld(new TimeSpan(0, 1, 20)))
                {
                    try
                    {
                        db.DeleteByCode("1");
                        db.Insert("1", "first");
                        Console.WriteLine("Exec-1:{0}", db.SelectByCode("1"));
                        //db.CreateTable();
                        //db.Insert("1", "first1");
                        commit = true;
                    }
                    catch (Exception ex1)
                    {
                        Console.WriteLine(ex1.ToString());
                    }

                    //using (MyTransactionScopeOld scope1 = new MyTransactionScopeOld())
                    //{
                    //    Console.WriteLine("scope1\r\n{{");

                    //    bool succeed = false;
                    //    try
                    //    {
                    //        db.DeleteByCode("0");
                    //        db.Insert("0", "-");
                    //        Console.WriteLine("Exec-0:{0}", db.SelectByCode("0"));
                    //        Console.WriteLine("Exec-1:{0}", db.SelectByCode("1"));

                    //        //db.Insert("1", "A");
                    //        //Console.WriteLine("Exec-1:{0}", db.SelectByCode("1"));

                    //        succeed = true;
                    //    }
                    //    catch (Exception ex2)
                    //    {
                    //        Console.WriteLine(ex2.ToString());
                    //    }

                    //    scope1.Complete();
                    //    Console.WriteLine("scope1.Complete()\r\n}");
                    //}

                    try
                    {
                        db.Insert("1", "first1");
                        commit = true;
                    }
                    catch (Exception ex1)
                    {
                        commit = false;
                        Console.WriteLine(ex1.ToString());
                    }

                    if (commit)
                    {
                        scope.Complete();
                        Console.WriteLine("Complete()");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Trans Error：" + ex.ToString());
            }

            Console.WriteLine("------------------");
            Console.WriteLine("Final-1:{0}", db.SelectByCode("1"));
        }


        public class DbAccess : MyOdpDataBaseOld
        {
            public DbAccess(string connString)
            {
                DBConnString = connString;
            }

            public string SelectByCode(string code)
            {
                OracleParameter[] parms = new OracleParameter[] {
                    new OracleParameter(":P", OracleDbType.Varchar2)
                };
                parms[0].Value = code;

                object objVal = ExecuteScalar("SELECT CREATE_TIME FROM TEST4FAN_TRANS WHERE CODE=:P", parms);
                return objVal == null ? null : objVal.ToString();
            }

            public void DeleteByCode(string code)
            {
                OracleParameter[] parms = new OracleParameter[] {
                    new OracleParameter(":P", OracleDbType.Varchar2)
                };
                parms[0].Value = code;

                ExecuteNonQuery("DELETE FROM TEST4FAN_TRANS WHERE CODE=:P", parms);
            }

            public void Insert(string code, string name)
            {
                OracleParameter[] parms = new OracleParameter[] {
                    new OracleParameter(":P1", OracleDbType.Varchar2),
                    new OracleParameter(":P2", OracleDbType.Varchar2),
                };
                parms[0].Value = code;
                parms[1].Value = name;

                ExecuteNonQuery("INSERT INTO TEST4FAN_TRANS(CODE,NAME,CREATE_TIME) VALUES(:P1,:P2,sysdate)", parms);
            }


            public void CreateTable()
            {
                ExecuteNonQuery("create table TEST4FAN_TRANS1(code varchar2(20))");
            }
        }
    }
}
