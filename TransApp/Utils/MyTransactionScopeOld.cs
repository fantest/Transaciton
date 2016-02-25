using System;
using System.Collections.Generic;
using System.Collections;
using System.Text;
using System.Threading;
using System.Data.Common;

namespace FanTest.Trans
{
    /// <summary>
    /// 使代码块成为AF事务性代码
    /// </summary>
    public sealed class MyTransactionScopeOld : IDisposable
    {
        /// <summary>
        /// 标志用户是不是提交了事务
        /// </summary>
        private bool isCompleted = false;

        /// <summary>
        /// 存放已加入当前事务环境的子事务
        /// </summary>
        private Dictionary<string, DbTransaction> transactionPool = new Dictionary<string, DbTransaction>();

        [ThreadStatic]
        private static MyTransactionScopeOld currentScope;
        /// <summary>
        /// 取得当前事务环境
        /// </summary>
        public static MyTransactionScopeOld Current
        {
            get
            {
                return currentScope;
            }
            private set
            {
                currentScope = value;
            }
        }

        private Guid scopeID = Guid.NewGuid();
        /// <summary>
        /// 事务ID
        /// </summary>
        public Guid ScopeID
        {
            get
            {
                return scopeID;
            }
        }

        /// <summary>
        /// 事务超时计时器
        /// </summary>
        private System.Threading.Timer timer = null;

        /// <summary>
        /// 事务超时时间
        /// </summary>
        private TimeSpan timeSpan = TimeSpan.Zero;

        /// <summary>
        /// 事务是否已经超时
        /// </summary>
        private bool isTimeOut = false;

        /// <summary>
        /// 线程锁定对象
        /// 这里之所以在ThreadStatic的情况下，还需要锁定对象的作用，主要是还存在超时的异步处理。
        /// 有可能同时访问transactionPool
        /// </summary>
        private object lockObj = new object();

        /// <summary>
        /// 构造方法
        /// </summary>
        public MyTransactionScopeOld()
        {
            if (Current == null)
            {
                Current = this;
            }
        }

        /// <summary>
        /// 构造方法
        /// </summary>
        /// <param name="timeSpan">事务超时时间</param>
        public MyTransactionScopeOld(TimeSpan outTime)
            : this()
        {
            timeSpan = outTime;
        }

        /// <summary>
        /// 调用此方法,将会在代码段结束后提交事务
        /// </summary>
        public void Complete()
        {
            isCompleted = true;
        }

        /// <summary>
        /// 加入当前事务
        /// </summary>
        /// <param name="connString">连接字符串</param>
        /// <param name="conn">连接实例</param>
        public void JoinConnectionToTransaction(string connString, DbConnection conn)
        {
            if (isTimeOut) throw new MyTransactionScopeTimeOut("事务已超时");
            if (conn == null) throw new ArgumentNullException("要加入到BH事务环境的连接不能为空");
            try
            {
                lock (lockObj)
                {
                    if (!transactionPool.ContainsKey(connString))
                    {
                        if (conn.State != System.Data.ConnectionState.Open)
                        {
                            conn.Open();
                        }
                        DbTransaction trans = conn.BeginTransaction();
                        transactionPool.Add(connString, trans);

                        if (timer == null && timeSpan != TimeSpan.Zero)
                        {
                            timer = new System.Threading.Timer(TimeOut, null, timeSpan, TimeSpan.FromMilliseconds(-1));
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("将连接加入到事务出错", ex);
            }
        }

        /// <summary>
        /// 获取连接
        /// 如果在当前事务中还不存在该连接，则返回null
        /// </summary>
        /// <returns></returns>
        public DbConnection GetConnection(string conn)
        {
            if (isTimeOut) throw new MyTransactionScopeTimeOut("事务已超时");
            lock (lockObj)
            {
                if (transactionPool.ContainsKey(conn))
                {
                    return transactionPool[conn].Connection;
                }
            }
            return null;
        }

        /// <summary>
        /// 获取指定连接在当前事务环境中的事务
        /// 如果该连接还未创建事务,则返回null
        /// </summary>
        /// <param name="conn"></param>
        /// <returns></returns>
        public DbTransaction GetTransaction(string conn)
        {
            if (isTimeOut) throw new MyTransactionScopeTimeOut("事务已超时");

            lock (lockObj)
            {
                if (transactionPool.ContainsKey(conn))
                {
                    return transactionPool[conn];
                }
            }
            return null;
        }

        /// <summary>
        /// 销毁资源
        /// </summary>
        public void Dispose()
        {
            if (Current == null)
                return;

            if (Current.scopeID != this.scopeID)
                return;

            lock (lockObj)
            {
                if (timer != null)
                {
                    timer.Dispose();
                }

                //数据库
                foreach (string connString in transactionPool.Keys)
                {
                    DbConnection connection = transactionPool[connString].Connection;
                    try
                    {
                        //如果用户提交了事务
                        if (isCompleted && !isTimeOut)
                        {
                            transactionPool[connString].Commit();
                        }
                        else
                        {
                            transactionPool[connString].Rollback();
                        }
                    }
                    finally
                    {
                        //关闭所有的连接
                        if (connection != null && connection.State != System.Data.ConnectionState.Closed)
                        {
                            connection.Close();
                            connection.Dispose();
                        }
                        transactionPool[connString].Dispose();
                    }
                }
                transactionPool.Clear();

                Current = null;

                if (isTimeOut)
                {
                    throw new MyTransactionScopeTimeOut("事务已超时");
                }
            }
        }

        /// <summary>
        /// 事务超时
        /// </summary>
        private void TimeOut(object value)
        {
            try
            {
                lock (lockObj)
                {
                    isTimeOut = true;

                    //数据库
                    foreach (DbTransaction tran in transactionPool.Values)
                    {
                        DbConnection connection = tran.Connection;
                        if (connection != null && connection.State != System.Data.ConnectionState.Closed)
                        {
                            connection.Close();
                            connection.Dispose();
                        }
                        tran.Dispose();
                    }
                    transactionPool.Clear();
                }
            }
            catch
            {
            }
        }
    }
}
