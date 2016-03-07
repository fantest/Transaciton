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

            Console.WriteLine("Last - 0:{0}", db.SelectCreateTimeByCode("0"));
            Console.WriteLine("Last - 1:{0}", db.SelectCreateTimeByCode("1"));
            Console.WriteLine("Last - 2:{0}", db.SelectCreateTimeByCode("2"));
            Console.WriteLine("Last -22:{0}", db.SelectCreateTimeByCode("22"));
            Console.WriteLine("Last - 3:{0}", db.SelectCreateTimeByCode("3"));
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
                        Console.WriteLine("Insert - 0:{0}", db.SelectCreateTimeByCode("0"));
                        //db.CreateTable();
                        //db.Insert("0", "zero1");
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
                            Console.WriteLine("Read   - 0:{0}", db.SelectCreateTimeByCode("0"));
                            db.DeleteByCode("1");
                            db.Insert("1", "one");
                            Console.WriteLine("Insert - 1:{0}", db.SelectCreateTimeByCode("1"));

                            db.Insert("0", "zero2");
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
                                Console.WriteLine("Read   - 0:{0}", db.SelectCreateTimeByCode("0"));
                                Console.WriteLine("Read   - 1:{0}", db.SelectCreateTimeByCode("1"));

                                db.DeleteByCode("2");
                                db.Insert("2", "two");
                                Console.WriteLine("Insert - 2:{0}", db.SelectCreateTimeByCode("2"));

                                throw new Exception("test");
                                db.DeleteByCode("22");
                                db.Insert("22", "two-two");
                                Console.WriteLine("Insert - 3:{0}", db.SelectCreateTimeByCode("3"));

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
                            Console.WriteLine("Read   - 0:{0}", db.SelectCreateTimeByCode("0"));
                            Console.WriteLine("Read   - 1:{0}", db.SelectCreateTimeByCode("1"));
                            Console.WriteLine("Read   - 2:{0}", db.SelectCreateTimeByCode("2"));

                            db.DeleteByCode("3");
                            db.Insert("3", "three");
                            Console.WriteLine("Insert - 3:{0}", db.SelectCreateTimeByCode("3"));

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


                    //if (commit)
                    //{
                    //    try
                    //    {
                    //        db.Insert("1", "first1");
                    //    }
                    //    catch (Exception ex1)
                    //    {
                    //        commit = false;
                    //        Console.WriteLine(ex1.ToString());
                    //    }
                    //}

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
            Console.WriteLine("Final - 0:{0}", db.SelectCreateTimeByCode("0"));
            Console.WriteLine("Final - 1:{0}", db.SelectCreateTimeByCode("1"));
            Console.WriteLine("Final - 2:{0}", db.SelectCreateTimeByCode("2"));
            Console.WriteLine("Final -22:{0}", db.SelectCreateTimeByCode("22"));
            Console.WriteLine("Final - 3:{0}", db.SelectCreateTimeByCode("3"));
        }

        //嵌套事务中,在不同事务环境里对同一条记录做了更新.会在提交时发生死锁.
        public static void Test2(bool showLock)
        {
            Console.WriteLine("\r\n==================\r\nCalling MyTrans.Test2()");

            //string connStr = Manager.GetConnStringOfOra1();
            string connStr = string.Format("Data Source={0};User ID={1};Password={2};Persist Security Info=True;Pooling=true",
                "bhdevcomber", //Helper.GetDatasource4OraTNS("192.168.100.52", "1521", "bhdevcomber"), //
                "bhdata",
                "bhdata");
            DbAccess db = new DbAccess(connStr);

            //确保记录存在,用于验证Update.
            try
            {
                db.Insert("8", "eight-0");
            }
            catch { }
            try
            {
                db.Insert("9", "nine-0");
            }
            catch { }

            Console.WriteLine("Last - 8:{0}", db.SelectEditTimeByCode("8"));
            Console.WriteLine("Last - 9:{0}", db.SelectEditTimeByCode("9"));
            Console.WriteLine("------------------");

            try
            {
                using (MyTransactionScope scope = new MyTransactionScope())
                {
                    if (showLock)
                    {
                        db.Update("8", "eight-1");
                        Console.WriteLine("Update - 8:{0}", db.SelectEditTimeByCode("8"));
                    }

                    db.Update("9", "nine-1");
                    Console.WriteLine("Update - 9:{0}", db.SelectEditTimeByCode("9"));

                    using (MyTransactionScope scope1 = new MyTransactionScope( TransactionScopeOption.RequiresNew))
                    {
                        db.Update("8", "eight-2");
                        Console.WriteLine("Update in inner scope - 8:{0}", db.SelectEditTimeByCode("8"));

                        if (showLock)
                        {
                            db.Update("9", "nine-2");
                            Console.WriteLine("Update in inner scope - 9:{0}", db.SelectEditTimeByCode("9"));
                        }

                        scope1.Complete();
                    }

                    scope.Complete();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Trans Error：" + ex.ToString());
            }

            Console.WriteLine("------------------");
            Console.WriteLine("Last - 8:{0}", db.SelectEditTimeByCode("8"));
            Console.WriteLine("Last - 9:{0}", db.SelectEditTimeByCode("9"));
        }


        public class DbAccess : MyOdpDataBase
        {
            public DbAccess(string connString)
            {
                DBConnString = connString;
            }

            public string SelectCreateTimeByCode(string code)
            {
                OracleParameter[] parms = new OracleParameter[] {
                    new OracleParameter(":P", OracleDbType.Varchar2)
                };
                parms[0].Value = code;

                object objVal = ExecuteScalar("SELECT CREATE_TIME FROM TEST4FAN_TRANS WHERE CODE=:P", parms);
                return objVal == null ? null : objVal.ToString();
            }

            public string SelectEditTimeByCode(string code)
            {
                OracleParameter[] parms = new OracleParameter[] {
                    new OracleParameter(":P", OracleDbType.Varchar2)
                };
                parms[0].Value = code;

                object objVal = ExecuteScalar("SELECT EDIT_TIME FROM TEST4FAN_TRANS WHERE CODE=:P", parms);
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
            
            public void Update(string code, string name)
            {
                OracleParameter[] parms = new OracleParameter[] {
                    new OracleParameter(":P1", OracleDbType.Varchar2),
                    new OracleParameter(":P2", OracleDbType.Varchar2),
                };
                parms[0].Value = code;
                parms[1].Value = name;

                ExecuteNonQuery("UPDATE TEST4FAN_TRANS SET NAME=:P2, EDIT_TIME=sysdate WHERE CODE=:P1", parms);
            }


            public void CreateTable()
            {
                ExecuteNonQuery("create table TEST4FAN_TRANS1(code varchar2(20))");
            }
        }
    }
}
