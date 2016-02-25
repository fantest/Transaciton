using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace FanTest.Trans
{
    public abstract class DataAccessBase
    {
        private string m_strDbConn = "";
        /// <summary>
        /// 数据库连接字符串
        /// </summary>
        public string DBConnString
        {
            get { return m_strDbConn; }
            set { m_strDbConn = value; }
        }

        /// <summary>
        /// 事务管理
        /// </summary>
        protected static Dictionary<string, DbTransaction> transactions = new Dictionary<string, DbTransaction>();

    }
}
