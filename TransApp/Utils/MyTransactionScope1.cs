using System;
using System.Collections.Generic;
using System.Collections;
using System.Data.Common;
using System.Text;
using System.Threading;

namespace FanTest.Trans
{
    /// <summary>
    /// 使代码块成为AF事务性代码
    /// </summary>
    public sealed class MyTransactionScope1 : IDisposable
    {
        /// <summary>
        /// 标志用户是不是提交了事务
        /// </summary>
        private bool isCompleted = false;

        /// <summary>
        /// 存放已加入当前事务环境的子事务
        /// </summary>
        private Dictionary<string, DbTransaction> transactionPool = new Dictionary<string, DbTransaction>();

        private Stack<MyTransactionScope1> scopeStack;

        [ThreadStatic]
        private static MyTransactionScope1 rootScope;
        /// <summary>
        /// 取得根级事务环境
        /// </summary>
        private static MyTransactionScope1 Root
        {
            get
            {
                return rootScope;
            }
            set
            {
                rootScope = value;
            }
        }

        [ThreadStatic]
        private static MyTransactionScope1 currentScope;
        /// <summary>
        /// 取得当前事务环境
        /// </summary>
        public static MyTransactionScope1 Current
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

        /// <summary>
        /// 获取当前是否包含在事务环境
        /// </summary>
        public static bool InTransEnvironment
        {
            get
            {
                if (Current == null)
                    return false;
                return Current.ScopeOption != System.Transactions.TransactionScopeOption.Suppress;
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

        private System.Transactions.TransactionScopeOption scopeOption;
        /// <summary>
        /// 描述与此事务范围关联的事务要求
        /// </summary>
        public System.Transactions.TransactionScopeOption ScopeOption
        {
            get
            {
                return scopeOption;
            }
        }

        /// <summary>
        /// 事务超时计时器
        /// </summary>
        private System.Threading.Timer timer = null;

        /// <summary>
        /// 事务超时时间
        /// </summary>
        private TimeSpan timeSpan;

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
        /// <param name="option">描述与此事务范围关联的事务要求</param>
        /// <param name="timeout">在它之后，事务范围将超时并中止此事务</param>
        public MyTransactionScope1(System.Transactions.TransactionScopeOption option, TimeSpan timeout)
        {
            this.scopeOption = option;
            this.timeSpan = timeout;

            //Root的Option可能是:Required,RequiresNew,Suppress
            //Current不是Root的情况下,Option可能是:RequiresNew,Suppress
            //备注：因为Root和Current是ThreadStatic,所以处理时不用lock.

            if (Root == null)
            {
                this.scopeStack = null;

                Root = this;
                Current = this;
            }
            else
            {
                //如果是直接加入当前事务环境,则不更新Current
                if (option == System.Transactions.TransactionScopeOption.Required)
                {
                    switch (Current.ScopeOption)
                    {
                        case System.Transactions.TransactionScopeOption.Required:
                        case System.Transactions.TransactionScopeOption.RequiresNew:
                            return; //不更新Current
                        case System.Transactions.TransactionScopeOption.Suppress:
                        default:
                            break;
                    }
                }

                //更新Current前,将之前的Current入栈.
                if (Current != null)
                {
                    if (Root.scopeStack == null)
                        Root.scopeStack = new Stack<MyTransactionScope1>();
                    Root.scopeStack.Push(Current);
                }
                Current = this;
            }
        }
        /// <summary>
        /// 构造方法
        /// </summary>
        public MyTransactionScope1()
            : this(System.Transactions.TransactionScopeOption.Required, TimeSpan.Zero)
        {
        }
        /// <summary>
        /// 构造方法
        /// </summary>
        /// <param name="option">描述与此事务范围关联的事务要求</param>
        public MyTransactionScope1(System.Transactions.TransactionScopeOption option)
            : this(option, TimeSpan.Zero)
        {
        }
        /// <summary>
        /// 构造方法
        /// </summary>
        /// <param name="timeout">事务超时时间</param>
        public MyTransactionScope1(TimeSpan timeout)
            : this(System.Transactions.TransactionScopeOption.Required, timeout)
        {
        }

        /// <summary>
        /// 如果当前在事务环境中,则将数据库连接加入当前事务.
        /// </summary>
        /// <param name="connString">连接字符串</param>
        /// <param name="conn">连接实例</param>
        /// <returns>当前是否包含在事务环境</returns>
        public static bool TryJoinToCurrentTransaction(string connString, DbConnection conn)
        {
            if (InTransEnvironment)
            {
                Current.JoinConnectionToTransaction(connString, conn);
                return true;
            }
            return false;
        }

        /// <summary>
        /// 尝试从当前事务环境中获取数据库连接
        /// 返回null的情况：没有事务环境、当前事务中还不存在该连接
        /// </summary>
        /// <param name="connString"></param>
        /// <param name="conn"></param>
        /// <returns>当前是否包含在事务环境</returns>
        public static bool TryGetDbConnection(string connString, out DbConnection conn)
        {
            if (InTransEnvironment)
            {
                conn = Current.GetConnection(connString);
                return true;
            }
            else
            {
                conn = null;
                return false;
            }
        }
        /// <summary>
        /// 尝试获取指定连接在当前事务环境中的事务
        /// 如果该连接还未创建事务,则trans为null
        /// </summary>
        /// <param name="connString"></param>
        /// <param name="trans"></param>
        /// <returns>当前是否包含在事务环境</returns>
        public static bool TryGetTransaction(string connString, out DbTransaction trans)
        {
            if (InTransEnvironment)
            {
                trans = Current.GetTransaction(connString);
                return true;
            }
            else
            {
                trans = null;
                return false;
            }
        }


        /// <summary>
        /// 调用此方法,将会在代码段结束后提交事务
        /// </summary>
        public void Complete()
        {
            isCompleted = true;
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
            if (Root == null || this.ScopeOption == System.Transactions.TransactionScopeOption.Suppress)
                return;

            //Root的Option可能是:Required,RequiresNew,Suppress
            //Current不是Root的情况下,Option可能是:RequiresNew,Suppress
            //
            //主体逻辑：
            //  如果是根级事务环境，处理后将事务环境置空。
            //  如果是非根级事务环境，提交当前层级事务Current后，根据“事务链”更新Current。

            lock (lockObj)
            {
                if (Root.scopeID == this.scopeID)
                {
                    try
                    {
                        Root.DisposeCore();
                    }
                    finally
                    {
                        if (rootScope.scopeStack != null)
                            Root.scopeStack.Clear();
                        Root.scopeStack = null;
                        Root = null;
                        Current = null;
                    }
                }
                else
                {
                    //特别说明：不管当前实例是哪个，按照开始嵌套事务的顺序获取。即Current。

                    if (Current == null)
                        return;
                    //if (this.scopeID != Current.scopeID)
                    //    throw new NotSupportedException("事务调用顺序不正确");

                    try
                    {
                        Current.DisposeCore();
                    }
                    finally
                    {
                        if (Root.scopeStack.Count > 0)
                            Current = Root.scopeStack.Pop();
                        else
                            Current = Root;
                    }
                }
            }
        }

        void DisposeCore()
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
            
            if (isTimeOut)
            {
                throw new MyTransactionScopeTimeOut("事务已超时");
            }
        }
        
        /// <summary>
        /// 加入当前事务
        /// </summary>
        /// <param name="connString">连接字符串</param>
        /// <param name="conn">连接实例</param>
        void JoinConnectionToTransaction(string connString, DbConnection conn)
        {
            if (conn == null) throw new ArgumentNullException("要加入到BH事务环境的连接不能为空");

            if (this.scopeOption == System.Transactions.TransactionScopeOption.Suppress)
            {
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
                        }
                    }
                }
                catch (Exception ex)
                {
                    throw new Exception("将连接加入到事务出错", ex);
                }
            }
            else
            {
                if (isTimeOut) throw new MyTransactionScopeTimeOut("事务已超时");
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
