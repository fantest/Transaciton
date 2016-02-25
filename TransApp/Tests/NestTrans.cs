using Oracle.DataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Transactions;

namespace FanTest.Trans
{
    internal class NestTrans
    {
        /*
         * [关键点]
         *  1)事务环境是在conn.Open()时生效
         * 
         * RequiresNew
         *   特性：
         *     1)查询时，是事务未提交前的数据环境（如，在外层事务中执行了数据修改，查询到的还会是修改前的内容）。
         * Suppress
         *   特性：
         *     1)不需要调用Complete()提交，因为在其范围内的每个数据库操作都会直接提交。
         *     2)查询时，是事务未提交前的数据环境（如，在外层事务中执行了数据修改，查询到的还会是修改前的内容）。
         *   应用场景：
         *     1)DDL语句不能放到事务中(因为会立即COMMIT)
         *     2)在事务内提交错误日志
         *     
         * [疑问]
         *  1)TNS方式的连接串conn.Open()放在事务中，会导致进程崩溃？
        */
        public static void Test1()
        {
            //string connStr = Manager.GetConnStringOfOra1();
            string connStr = string.Format("Data Source={0};User ID={1};Password={2};Persist Security Info=True;Pooling=true",
                "bhdevcomber", //Helper.GetDatasource4OraTNS("192.168.100.52", "1521", "bhdevcomber"), //
                "bhdata", 
                "bhdata");

            using (OracleConnection conn = new OracleConnection(connStr))
            {
                Console.WriteLine("Last-0:{0}", SelectByCode(conn, "0"));
                Console.WriteLine("Last-1:{0}", SelectByCode(conn, "1"));
                Console.WriteLine("------------------");

                try
                {
                    bool commit = false;
                    TransactionOptions transOption = new TransactionOptions();
                    transOption.IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted;
                    transOption.Timeout = new TimeSpan(0, 0, 20);
                    using (TransactionScope scope = new TransactionScope(TransactionScopeOption.Required, transOption))
                    {
                        try
                        {
                            DeleteByCode(conn, "1");
                            Insert(conn, "1", "first");
                            //CreateTable(conn);
                            commit = true;
                            Console.WriteLine("Exec-1:{0}", SelectByCode(conn, "1"));
                        }
                        catch (Exception ex1)
                        {
                            Console.WriteLine(ex1.ToString());
                        }

                        TransactionScopeOption scopeOption = TransactionScopeOption.Suppress;
                        using (TransactionScope scope1 = new TransactionScope(scopeOption))
                        {
                            Console.WriteLine("scope1.{0}\r\n{{", scopeOption.ToString());

                            bool succeed = false;
                            try
                            {
                                DeleteByCode(conn, "0");
                                Insert(conn, "0", "-");
                                Console.WriteLine("Exec-0:{0}", SelectByCode(conn, "0"));
                                Console.WriteLine("Exec-1:{0}", SelectByCode(conn, "1"));

                                //Insert(conn, "1", "A");
                                //Console.WriteLine("Exec-1:{0}", SelectByCode(conn, "1"));

                                succeed = true;
                            }
                            catch (Exception ex2)
                            {
                                Console.WriteLine(ex2.ToString());
                            }

                            //Suppress不需要提交
                            if (scopeOption != TransactionScopeOption.Suppress && succeed)
                            {
                                scope1.Complete();
                                Console.WriteLine("scope1.Complete()\r\n}");
                            }
                            else
                            {
                                Console.WriteLine("}");
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
                Console.WriteLine("Final-0:{0}", SelectByCode(conn, "0"));
                Console.WriteLine("Final-1:{0}", SelectByCode(conn, "1"));
            }
        }

        public static void Test2()
        {
            //string connStr = Manager.GetConnStringOfOra1();
            string connStr = string.Format("Data Source={0};User ID={1};Password={2};Persist Security Info=True;Pooling=true",
                "bhdevcomber", //Helper.GetDatasource4OraTNS("192.168.100.52", "1521", "bhdevcomber"), //
                "bhdata",
                "bhdata");

            using (OracleConnection conn = new OracleConnection(connStr))
            {
                Console.WriteLine("Last-0:{0}", SelectByCode(conn, "0"));
                Console.WriteLine("Last-1:{0}", SelectByCode(conn, "1"));
                Console.WriteLine("------------------");

                try
                {
                    bool commit = false;
                    TransactionOptions transOption = new TransactionOptions();
                    transOption.IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted;
                    transOption.Timeout = new TimeSpan(0, 0, 20);
                    using (TransactionScope scope = new TransactionScope(TransactionScopeOption.Suppress, transOption))
                    {
                        try
                        {
                            DeleteByCode(conn, "1");
                            Insert(conn, "1", "first");
                            //CreateTable(conn);
                            commit = true;
                            Console.WriteLine("Exec-1:{0}", SelectByCode(conn, "1"));
                        }
                        catch (Exception ex1)
                        {
                            Console.WriteLine(ex1.ToString());
                        }

                        TransactionScopeOption scopeOption = TransactionScopeOption.Required;
                        using (TransactionScope scope1 = new TransactionScope(scopeOption))
                        {
                            Console.WriteLine("scope1.{0}\r\n{{", scopeOption.ToString());

                            bool succeed = false;
                            try
                            {
                                DeleteByCode(conn, "0");
                                Insert(conn, "0", "-");
                                Console.WriteLine("Exec-0:{0}", SelectByCode(conn, "0"));
                                Console.WriteLine("Exec-1:{0}", SelectByCode(conn, "1"));

                                Insert(conn, "1", "A");
                                //Console.WriteLine("Exec-1:{0}", SelectByCode(conn, "1"));

                                succeed = true;
                            }
                            catch (Exception ex2)
                            {
                                Console.WriteLine(ex2.ToString());
                            }

                            //Suppress不需要提交
                            if (scopeOption != TransactionScopeOption.Suppress && succeed)
                            {
                                scope1.Complete();
                                Console.WriteLine("scope1.Complete()\r\n}");
                            }
                            else
                            {
                                Console.WriteLine("}");
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
                Console.WriteLine("Final-0:{0}", SelectByCode(conn, "0"));
                Console.WriteLine("Final-1:{0}", SelectByCode(conn, "1"));
            }
        }

        static string SelectByCode(OracleConnection conn, string code)
        {
            conn.Open();
            try
            {
                using (OracleCommand cmd = new OracleCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = "SELECT CREATE_TIME FROM TEST4FAN_TRANS WHERE CODE=:P";
                    cmd.Parameters.Add(":P", OracleDbType.Varchar2, code, ParameterDirection.Input);
                    object objVal = cmd.ExecuteScalar();
                    return objVal == null ? null : objVal.ToString();
                }
            }
            finally
            {
                conn.Close();
            }
        }
        static void DeleteByCode(OracleConnection conn, string code)
        {
            conn.Open();
            try
            {
                using (OracleCommand cmd = new OracleCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = "DELETE FROM TEST4FAN_TRANS WHERE CODE=:P";
                    cmd.Parameters.Add(":P", OracleDbType.Varchar2, code, ParameterDirection.Input);
                    cmd.ExecuteNonQuery();
                }
            }
            finally
            {
                conn.Close();
            }
        }
        static void Insert(OracleConnection conn, string code, string name)
        {
            conn.Open();
            try
            {
                using (OracleCommand cmd = new OracleCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = "INSERT INTO TEST4FAN_TRANS(CODE,NAME,CREATE_TIME) VALUES(:P1,:P2,sysdate)";
                    cmd.Parameters.Add(":P1", OracleDbType.Varchar2, code, ParameterDirection.Input);
                    cmd.Parameters.Add(":P2", OracleDbType.Varchar2, name, ParameterDirection.Input);
                    cmd.ExecuteNonQuery();
                }
            }
            finally
            {
                conn.Close();
            }
        }

        static void CreateTable(OracleConnection conn)
        {
            conn.Open();
            try
            {
                using (OracleCommand cmd = new OracleCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = "create table TEST4FAN_TRANS1(code varchar2(20))";
                    cmd.ExecuteNonQuery();
                }
            }
            finally
            {
                conn.Close();
            }
        }
    }
}
