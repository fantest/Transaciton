using Oracle.DataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Transactions;

namespace FanTest.Trans
{
    internal class MyTrans
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

            Console.WriteLine("Last - 0:{0}", db.SelectByCode("0"));
            Console.WriteLine("Last - 1:{0}", db.SelectByCode("1"));
            Console.WriteLine("Last - 2:{0}", db.SelectByCode("2"));
            Console.WriteLine("Last -22:{0}", db.SelectByCode("22"));
            Console.WriteLine("Last - 3:{0}", db.SelectByCode("3"));
            Console.WriteLine("------------------");

            try
            {
                bool commit;
                using (MyTransactionScope scope = new MyTransactionScope(new TimeSpan(0, 1, 20)))
                {
                    try
                    {
                        db.DeleteByCode("0");
                        db.Insert("0", "zero");
                        Console.WriteLine("Insert - 0:{0}", db.SelectByCode("0"));
                        //db.CreateTable();
                        db.Insert("0", "zero1");
                        commit = true;
                    }
                    catch (Exception ex1)
                    {
                        Console.WriteLine(ex1.ToString());
                        commit = false;
                    }

                    TransactionScopeOption scopeOption1 = TransactionScopeOption.RequiresNew;
                    TransactionScopeOption scopeOption2 = TransactionScopeOption.Suppress;
                    #region Scope1
                    using (MyTransactionScope scope1 = new MyTransactionScope(scopeOption1))
                    {
                        Console.WriteLine("scope1.{0}\r\n{{", scopeOption1.ToString());
                        bool succeed1 = false;
                        try
                        {
                            Console.WriteLine("Read   - 0:{0}", db.SelectByCode("0"));
                            db.DeleteByCode("1");
                            db.Insert("1", "one");
                            Console.WriteLine("Insert - 1:{0}", db.SelectByCode("1"));

                            //db.Insert("0", "zero2");
                            //Console.WriteLine("Insert-0:{0}", db.SelectByCode("0"));

                            succeed1 = true;
                        }
                        catch (Exception ex2)
                        {
                            Console.WriteLine(ex2.ToString());
                        }

                        #region Scope2
                        Console.WriteLine("scope2.{0}\r\n{{", scopeOption2.ToString());
                        using (MyTransactionScope scope2 = new MyTransactionScope(scopeOption2))
                        {
                            bool succeed2 = false;
                            try
                            {
                                Console.WriteLine("Read   - 0:{0}", db.SelectByCode("0"));
                                Console.WriteLine("Read   - 1:{0}", db.SelectByCode("1"));

                                db.DeleteByCode("2");
                                db.Insert("2", "two");
                                Console.WriteLine("Insert - 2:{0}", db.SelectByCode("2"));

                                throw new Exception("test");
                                db.DeleteByCode("22");
                                db.Insert("22", "two-two");
                                Console.WriteLine("Insert - 3:{0}", db.SelectByCode("3"));

                                succeed2 = true;
                            }
                            catch (Exception ex2)
                            {
                                Console.WriteLine(ex2.ToString());
                            }

                            //Suppress不需要提交
                            if (scopeOption2 != TransactionScopeOption.Suppress && succeed2)
                            {
                                scope2.Complete();
                                Console.WriteLine("scope2.Complete()\r\n}");
                            }
                            else
                            {
                                Console.WriteLine("}");
                            }
                        }
                        #endregion

                        //Suppress不需要提交
                        if (scopeOption1 != TransactionScopeOption.Suppress && succeed1)
                        {
                            scope1.Complete();
                            Console.WriteLine("scope1.Complete()\r\n}");
                        }
                        else
                        {
                            Console.WriteLine("}");
                        }
                    }
                    #endregion

                    TransactionScopeOption scopeOption3 = TransactionScopeOption.RequiresNew;
                    using (MyTransactionScope scope3 = new MyTransactionScope(scopeOption3))
                    {
                        Console.WriteLine("scope3.{0}\r\n{{", scopeOption3.ToString());
                        bool succeed3 = false;
                        try
                        {
                            Console.WriteLine("Read   - 0:{0}", db.SelectByCode("0"));
                            Console.WriteLine("Read   - 1:{0}", db.SelectByCode("1"));
                            Console.WriteLine("Read   - 2:{0}", db.SelectByCode("2"));

                            db.DeleteByCode("3");
                            db.Insert("3", "three");
                            Console.WriteLine("Insert - 3:{0}", db.SelectByCode("3"));

                            succeed3 = true;
                        }
                        catch (Exception ex3)
                        {
                            Console.WriteLine(ex3.ToString());
                        }

                        //Suppress不需要提交
                        if (scopeOption3 != TransactionScopeOption.Suppress && succeed3)
                        {
                            scope3.Complete();
                            Console.WriteLine("scope3.Complete()\r\n}");
                        }
                        else
                        {
                            Console.WriteLine("}");
                        }
                    }


                    if (commit)
                    {
                        try
                        {
                            db.Insert("1", "first1");
                        }
                        catch (Exception ex1)
                        {
                            commit = false;
                            Console.WriteLine(ex1.ToString());
                        }
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
            Console.WriteLine("Final - 0:{0}", db.SelectByCode("0"));
            Console.WriteLine("Final - 1:{0}", db.SelectByCode("1"));
            Console.WriteLine("Final - 2:{0}", db.SelectByCode("2"));
            Console.WriteLine("Final -22:{0}", db.SelectByCode("22"));
            Console.WriteLine("Final - 3:{0}", db.SelectByCode("3"));
        }


        public class DbAccess : MyOdpDataBase
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
