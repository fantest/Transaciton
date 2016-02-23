using Oracle.DataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;

namespace FanTest.Trans
{
    public class Helper
    {
        /// <summary>
        /// 检查数据库连接
        /// </summary>
        /// <param name="dbType">数据库类型</param>
        /// <param name="connString">连接字符串</param>
        /// <returns>True:连接成功，False:连接失败</returns>
        internal static bool IsDbConnectionValid(DatabaseType dbType, string connString)
        {
            bool isValid = false;
            DbConnection conn = null;
            switch (dbType)
            {
                case DatabaseType.SqlServer:
                    conn = new System.Data.SqlClient.SqlConnection(connString);
                    break;
                case DatabaseType.Oracle:
                    conn = new OracleConnection(connString);
                    break;
                default:
                    break;
            }
            if (conn != null)
            {
                try
                {
                    conn.Open();
                    isValid = true;
                }
                catch { }
                finally
                {
                    if (conn.State == ConnectionState.Open)
                        conn.Close();
                    conn.Dispose();
                }
            }
            return isValid;
        }

        /// <summary>
        /// 拼接连接字符串
        /// </summary>
        /// <param name="dataSource">服务器地址（不做空判断）</param>
        /// <param name="user">用户名（不做空判断）</param>
        /// <param name="password">密码（不做空判断）</param>
        /// <returns></returns>
        internal static string GetConnString(DatabaseType dbType, string dataSource, string user, string password)
        {
            string connString = string.Empty;
            switch (dbType)
            {
                case DatabaseType.Oracle:
                    connString = string.Format(
                        "Data Source={0};User ID={1};Password={2};Persist Security Info=True;Pooling=true",
                        dataSource, user, password);
                    break;
                case DatabaseType.SqlServer:
                    connString = GetConnString4SqlServer(dataSource, "master", user, password);  //conn.ChangeDatabase(...)
                    break;
                default:
                    break;
            }
            return connString;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="database"></param>
        /// <param name="user"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        internal static string GetConnString4SqlServer(string dataSource, string database, string user, string password)
        {
            return string.Format("Data Source={0};Initial Catalog={1};User ID={2};Password={3};Persist Security Info=True",
                dataSource, database, user, password);  //conn.ChangeDatabase(...)
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="user"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        internal static string GetConnString4SqlServer(string dataSource, string user, string password)
        {
            return GetConnString4SqlServer(dataSource, "master", user, password);  //conn.ChangeDatabase(...)
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        internal static string GetConnString4SqlServerSSPI(string dataSource, string database)
        {
            return string.Format("Data Source={0};Initial Catalog={1};Integrated Security=SSPI;", dataSource, database);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="database"></param>
        /// <param name="user"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        internal static string GetConnString4ConfigFile_SqlServer(string dataSource, string database, string user, string password)
        {
            return string.Format("Data Source={0};Initial Catalog={1};User ID={2};Password={3}",
                dataSource, database, user, password);
        }

        /// <summary>
        /// 拼接连接字符串
        /// </summary>
        /// <param name="ipAddress"></param>
        /// <param name="port"></param>
        /// <param name="oraSID"></param>
        /// <param name="user"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        internal static string GetConnString4OraTNS(string ipAddress, string port, string oraSID, string user, string password)
        {
            return GetConnString(DatabaseType.Oracle, GetDatasource4OraTNS(ipAddress, port, oraSID), user, password);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="ipAddress"></param>
        /// <param name="port"></param>
        /// <param name="oraSID"></param>
        /// <returns></returns>
        internal static string GetDatasource4OraTNS(string ipAddress, string port, string oraSID)
        { 
            //return string.Format("(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={0})(PORT={1}))(CONNECT_DATA=(SERVICE_NAME={2})))",
            //    ipAddress, port, oraSID);
            return string.Format("(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST={0})(PORT={1})))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME={2})))",
                ipAddress, port, oraSID);
            //Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=MyHost)(PORT=MyPort)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=MyOracleSID)))
        }

        /// <summary>
        /// 解析连接字符串
        /// </summary>
        /// <param name="connString"></param>
        /// <param name="datasource"></param>
        /// <param name="user"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        internal static bool ParseOraConnString(string connString, out string datasource, out string user, out string password)
        {
            datasource = string.Empty;
            user = string.Empty;
            password = string.Empty;
            if (!string.IsNullOrEmpty(connString))
            {
                try
                {
                    using (OracleConnection conn = new OracleConnection(connString))
                    {
                        datasource = conn.DataSource ?? "";

                        OracleConnectionStringBuilder connSb;
                        try
                        {
                            connSb = new OracleConnectionStringBuilder(connString);
                        }
                        catch
                        {
                            connSb = null;
                            //说明：解决DataSource超长后new OracleConnectionStringBuilder报错的问题
                            //      通常是DataSource超长造成，如：
                            //      (DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.4.40)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=orcl)))
                            if (connString.Length >= 128)
                            {
                                if (connString.Contains("Data Source=" + datasource)) //避免替换了不该替换的内容
                                {
                                    connSb = new OracleConnectionStringBuilder(
                                        connString.Replace("Data Source=" + datasource, ""));
                                }
                                else if (datasource.Length > 30)
                                {
                                    connSb = new OracleConnectionStringBuilder(
                                        connString.Replace(datasource, ""));
                                }
                            }
                        }

                        if (connSb != null)
                        {
                            user = connSb.UserID;
                            password = connSb.Password;
                            return true;
                        }
                    }
                }
                catch { }
            }
            return false;
        }
        /// <summary>
        /// 解析OracleTNS参数
        /// </summary>
        /// <param name="tnsDatasource"></param>
        /// <param name="ipAddress"></param>
        /// <param name="port"></param>
        /// <param name="oraSID"></param>
        /// <returns></returns>
        internal static bool ParseOraTnsArgs(string tnsDatasource, out string ipAddress, out string port, out string oraSID)
        {
            ipAddress = string.Empty;
            port = string.Empty;
            oraSID = string.Empty;

            tnsDatasource = tnsDatasource.Trim();
            if (tnsDatasource.Length == 0)
                return false;
            bool found = false;
            int openBracketIdx = 0;
            int closeBracketIdx = -1;
            int equaltoIdx = -1;
            do
            {
                equaltoIdx = tnsDatasource.IndexOf('=', openBracketIdx);
                if (equaltoIdx > 0)
                {
                    string token = tnsDatasource.Substring((openBracketIdx + 1), (equaltoIdx - openBracketIdx - 1)).Trim().ToUpper();
                    switch (token)
                    {
                        case "SERVICE_NAME":
                            closeBracketIdx = tnsDatasource.IndexOf(')', equaltoIdx);
                            oraSID = tnsDatasource.Substring((equaltoIdx + 1), (closeBracketIdx - equaltoIdx - 1));
                            found = true;
                            break;
                        case "HOST":
                            closeBracketIdx = tnsDatasource.IndexOf(')', equaltoIdx);
                            ipAddress = tnsDatasource.Substring((equaltoIdx + 1), (closeBracketIdx - equaltoIdx - 1));
                            found = true;
                            break;
                        case "PORT":
                            closeBracketIdx = tnsDatasource.IndexOf(')', equaltoIdx);
                            port = tnsDatasource.Substring((equaltoIdx + 1), (closeBracketIdx - equaltoIdx - 1));
                            found = true;
                            break;
                    }
                }
                //取下一个
                openBracketIdx = tnsDatasource.IndexOf('(', openBracketIdx + 1);
            } while (openBracketIdx > 0);

            return found;
        }

        /// <summary>
        /// 获取TNS信息
        /// Columns:InstanceName,ServerName,ServiceName,Protocol,Port
        /// </summary>
        /// 需要安装ODP.NET
        /// <returns></returns>
        internal static DataTable GetTNSInstance()
        {
            try
            {
                System.Data.Common.DbProviderFactory factory =
                    System.Data.Common.DbProviderFactories.GetFactory("Oracle.DataAccess.Client");
                if (factory.CanCreateDataSourceEnumerator)
                {
                    System.Data.Common.DbDataSourceEnumerator dsenum = factory.CreateDataSourceEnumerator();
                    return dsenum.GetDataSources(); //InstanceName,ServerName,ServiceName,Protocol,Port
                }
            }
            catch { }
            return null;
        }
    }

    public enum DatabaseType
    {
        SqlServer = 1,
        Oracle = 2,
    }
}
