using System;
using System.Collections;
using System.Collections.Specialized;
using System.Configuration;
using System.Xml;
using System.Data;
using Oracle.DataAccess.Client;
using System.Transactions;
using System.Data.Common;
using System.Collections.Generic;

namespace FanTest.Trans
{
    public class MyOdpDataBaseOld : DataAccessBase
    {
        int blobSize = -1;

        /// <summary>
        ///构造函数 
        /// </summary>
        public MyOdpDataBaseOld()
        {
        }

        /// <summary>
        /// 获取数据库连接
        /// </summary>
        /// <returns></returns>
        protected OracleConnection GetConnection()
        {
            if (MyTransactionScopeOld.Current != null)
            {
                DbConnection conn = MyTransactionScopeOld.Current.GetConnection(DBConnString);
                if (conn == null)
                {
                    conn = new OracleConnection(DBConnString);
                    MyTransactionScopeOld.Current.JoinConnectionToTransaction(DBConnString, conn);
                }
                return conn as OracleConnection;
            }
            else
            {
                OracleConnection conn = new OracleConnection(DBConnString);
                conn.Open();
                return conn;
            }
        }

        /// <summary>
        /// 获取当前事务
        /// </summary>
        /// <returns></returns>
        OracleTransaction GetTransaction()
        {
            if (MyTransactionScopeOld.Current != null)
            {
                DbTransaction trans = MyTransactionScopeOld.Current.GetTransaction(DBConnString);
                if (trans == null)
                {
                    throw new Exception("当前事务环境还未创建事务");
                }
                return trans as OracleTransaction;
            }
            return null;
        }

        /// <summary>
        /// 创建Oracle参数
        /// </summary>
        /// <param name="text"></param>
        /// <returns></returns>
        OracleParameter[] CreateParameters(object[] text)
        {
            List<OracleParameter> list = new List<OracleParameter>();
            for (int i = 0; i < text.Length; i += 2)
            {
                //这里是解决一个比较奇怪的问题，如果参数的类型是char，新建的OracleParameter的OracleType会变成byte，执行时会出错，只好在这里进行特殊处理
                //但是，编写一个小程序的里面，char类型的转换是正确，转换成OracleType.VarChar，执行结果正确。
                OracleParameter p = null;
                if (text[i + 1].GetType() == typeof(char))
                {
                    p = new OracleParameter(text[i].ToString(), OracleDbType.Char);
                    p.Value = text[i + 1];
                }
                else
                {
                    p = new OracleParameter(text[i].ToString(), text[i + 1]);
                }
                list.Add(p);
            }
            return list.ToArray();
        }

        /// <summary>
        /// 执行sql获取数据
        /// </summary>
        /// <param name="query"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public OracleDataReader ExecuteReader(string query, params object[] parameters)
        {
            return ExecuteReader(query, CreateParameters(parameters));
        }

        /// <summary>
        /// 执行sql获取数据
        /// </summary>
        /// <param name="query"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public OracleDataReader ExecuteReader(string query, OracleParameter[] parameters)
        {
            return ExecuteReader(query, parameters, CommandType.Text);
        }

        /// <summary>
        /// 执行sql获取数据
        /// </summary>
        /// <param name="query"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public OracleDataReader ExecuteReader(string query, OracleParameter[] parameters, CommandType type)
        {
            OracleConnection conn = GetConnection();
            try
            {
                using (OracleCommand cmd = new OracleCommand(query, conn))
                {
                    cmd.CommandType = type;
                    cmd.AddToStatementCache = true;
                    cmd.BindByName = true;
                    cmd.InitialLOBFetchSize = blobSize;
                    if (parameters != null)
                    {
                        foreach (OracleParameter p in parameters)
                        {
                            cmd.Parameters.Add(p);
                        }
                    }

                    //备注:连接关闭后,Reader读取数据将报错.
                    if (MyTransactionScopeOld.Current == null)
                        return cmd.ExecuteReader(CommandBehavior.CloseConnection); //关闭Reader时,一并关闭连接.
                    else
                        return cmd.ExecuteReader();
                }
            }
            catch (Exception ex)
            {
                if (conn != null && MyTransactionScopeOld.Current == null)
                {
                    conn.Clone();
                    conn.Dispose();
                }

                HandleOraException(ex as OracleException, conn);
                throw ex;
            }
        }

        /// <summary>
        /// 执行sql获取对象
        /// </summary>
        /// <param name="query"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public object ExecuteScalar(string query, params object[] parameters)
        {
            return ExecuteScalar(query, CreateParameters(parameters));
        }

        /// <summary>
        /// 执行sql获取对象
        /// </summary>
        /// <param name="query"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public object ExecuteScalar(string query, OracleParameter[] parameters)
        {
            return ExecuteScalar(query, parameters, CommandType.Text);
        }

        /// <summary>
        /// 执行sql获取对象
        /// </summary>
        /// <param name="query"></param>
        /// <param name="parameters"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public object ExecuteScalar(string query, OracleParameter[] parameters, CommandType type)
        {
            OracleConnection conn = GetConnection();
            try
            {
                using (OracleCommand cmd = new OracleCommand(query, conn))
                {
                    cmd.CommandType = type;
                    cmd.AddToStatementCache = true;
                    cmd.BindByName = true;
                    cmd.InitialLOBFetchSize = blobSize;

                    if (parameters != null)
                    {
                        foreach (OracleParameter p in parameters)
                        {
                            cmd.Parameters.Add(p);
                        }
                    }

                    return cmd.ExecuteScalar();
                }
            }
            catch (OracleException oraEx)
            {
                HandleOraException(oraEx, conn);
                throw oraEx;
            }
            finally
            {
                if (conn != null && MyTransactionScopeOld.Current == null)
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// 执行sql
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        public void ExecuteNonQuery(string sql, params object[] parameters)
        {
            ExecuteNonQuery(sql, CreateParameters(parameters));
        }

        /// <summary>
        /// 执行sql
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        public void ExecuteNonQuery(string sql, OracleParameter[] parameters)
        {
            ExecuteNonQuery(sql, parameters, CommandType.Text);
        }

        /// <summary>
        /// 执行sql
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <param name="type"></param>
        public void ExecuteNonQuery(string sql, OracleParameter[] parameters, CommandType type)
        {
            OracleConnection conn = GetConnection();
            try
            {
                using (OracleCommand cmd = new OracleCommand(sql, conn))
                {
                    cmd.Transaction = GetTransaction();
                    cmd.CommandType = type;
                    cmd.AddToStatementCache = true;
                    cmd.BindByName = true;
                    cmd.InitialLOBFetchSize = blobSize;
                    if (parameters != null)
                    {
                        foreach (OracleParameter p in parameters)
                        {
                            cmd.Parameters.Add(p);
                        }
                    }
                    cmd.ExecuteNonQuery();
                }
            }
            catch (OracleException oraEx)
            {
                HandleOraException(oraEx, conn);
                throw oraEx;
            }
            finally
            {
                if (conn != null && MyTransactionScopeOld.Current == null)
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// 执行sql获取Datatable
        /// </summary>
        /// <param name="query"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public DataTable ExecuteDataTable(string query, params object[] parameters)
        {
            return ExecuteDataTable(query, CreateParameters(parameters));
        }

        /// <summary>
        /// 执行sql获取Datatable
        /// </summary>
        /// <param name="query"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public DataTable ExecuteDataTable(string query, OracleParameter[] parameters)
        {
            return ExecuteDataTable(query, parameters, CommandType.Text);
        }

        /// <summary>
        /// 执行sql获取Datatable
        /// </summary>
        /// <param name="query"></param>
        /// <param name="parameters"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public DataTable ExecuteDataTable(string query, OracleParameter[] parameters, CommandType type)
        {
            OracleConnection conn = GetConnection();
            try
            {
                using (OracleCommand cmd = new OracleCommand(query, conn))
                {
                    cmd.CommandType = type;
                    cmd.AddToStatementCache = true;
                    cmd.BindByName = true;
                    cmd.InitialLOBFetchSize = blobSize;

                    using (OracleDataAdapter adapter = new OracleDataAdapter(cmd))
                    {
                        if (parameters != null)
                        {
                            foreach (OracleParameter p in parameters)
                            {
                                adapter.SelectCommand.Parameters.Add(p);
                            }
                        }
                        DataTable dt = new DataTable();
                        adapter.Fill(dt);
                        return dt;
                    }
                }
            }
            catch (OracleException oraEx)
            {
                HandleOraException(oraEx, conn);
                throw oraEx;
            }
            finally
            {
                if (conn != null && MyTransactionScopeOld.Current == null)
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }


        /// <summary>
        /// 针对性处理数据库异常
        /// </summary>
        /// <param name="oraEx"></param>
        /// <param name="conn"></param>
        public static void HandleOraException(OracleException oraEx, OracleConnection conn)
        {
            if (oraEx == null)
                return;

            switch (oraEx.Number)
            {
                case 3113:  //ORA-03113: 通信通道的文件结尾
                    //可能发生在重启数据库
                    OracleConnection.ClearAllPools();
                    throw new Exception("发生ORA-03113错误，已清空数据库连接池。", oraEx);
                case 28:    //ORA-00028: 会话己被终止(session kill) - 发生在会话被Kill之后
                case 1012:  //ORA-01012: 没有登录(not logon) - 发生在ORA-00028后再访问数据库
                case 2396:  //ORA-02396: 超出最大空闲时间(exceeded maximum idle time)
                case 12535: //ORA-12535: TNS操作超时(TNS:operation timed out)
                    if (conn != null)
                    {
                        OracleConnection.ClearPool(conn);
                        throw new Exception("发生ORA-" + oraEx.Number.ToString() + "错误，已从数据库连接池清理掉当前连接。", oraEx);
                    }
                    else
                    {
                        OracleConnection.ClearAllPools();
                        throw new Exception("发生ORA-" + oraEx.Number.ToString() + "错误，已清空数据库连接池。", oraEx);
                    }
                default:
                    break;
            }

            //ORA-01089: 正在执行立即关闭 - 发生在数据库正在关闭
        }
    }
}
