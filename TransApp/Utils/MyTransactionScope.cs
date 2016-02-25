using System;
using System.Collections.Generic;
using System.Collections;
using System.Data.Common;
using System.Text;
using System.Threading;

namespace FanTest.Trans
{
    public sealed class MyTransactionScope : IDisposable
    {
        /// <summary>
        /// 标志用户是不是提交了事务
        /// </summary>
        private bool isCompleted = false;

        /// <summary>
        /// 存放已加入当前事务环境的子事务
        /// </summary>
        private Dictionary<string, DbTransaction> transactionPool = new Dictionary<string, DbTransaction>();

        /// <summary>
        /// 事务链，仅rootScope使用。
        /// </summary>
        private Stack<MyTransactionScope> scopeStack;

        /// <summary>
        /// 根级事务环境（作用：持有并维护“事务链”，用来在结束当前事务环境后，重新指定当前事务环境）
        /// </summary>
        [ThreadStatic]
        private static MyTransactionScope rootScope;

        [ThreadStatic]
        private static MyTransactionScope currentScope;
        /// <summary>
        /// 取得当前事务环境
        /// </summary>
        public static MyTransactionScope Current
        {
            get
            {
                if (InTransEnvironment)
                    return currentScope;
                return null;
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
                if (currentScope == null || currentScope.ScopeOption == System.Transactions.TransactionScopeOption.Suppress)
                    return false;
                return true;
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

        private Guid hostScopeID;

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
        /// <param name="timeout">在该时间间隔之后，事务范围将超时并中止此事务。
        /// 非根级事务环境中，仅当option为RequiresNew时有效。
        /// 特：rootScope是Suppress时，根级事务环境就不是rootScope。
        /// </param>
        public MyTransactionScope(System.Transactions.TransactionScopeOption option, TimeSpan timeout)
        {
            this.scopeOption = option;
            this.timeSpan = timeout;

            /*********************************************************************
            ** rootScope的Option可能是:Required,RequiresNew,Suppress
            ** currentScope不是rootScope的情况下,Option可能是:RequiresNew,Suppress
            **
            ** 备注：因为Root和Current是ThreadStatic,所以处理时不用lock.
            *********************************************************************/

            if (rootScope == null) //设置根级事务环境
            {
                this.scopeStack = null;

                rootScope = this;
                currentScope = this;
            }
            else //更新当前事务环境
            {
                //两种情况不更新当前事务环境currentScope：
                //  1)向RequiresNew/Required的环境下，创建Required环境；
                //  2)向Suppress的环境下，创建Suppress环境。
                if (option == System.Transactions.TransactionScopeOption.Required)
                {
                    switch (currentScope.ScopeOption)
                    {
                        case System.Transactions.TransactionScopeOption.Required:
                        case System.Transactions.TransactionScopeOption.RequiresNew:
                            this.hostScopeID = currentScope.ScopeID;
                            return; //不更新Current
                        case System.Transactions.TransactionScopeOption.Suppress:
                        default:
                            break;
                    }
                }
                else if (option == System.Transactions.TransactionScopeOption.Suppress)
                {
                    if (currentScope.ScopeOption == System.Transactions.TransactionScopeOption.Suppress)
                    {
                        this.hostScopeID = currentScope.ScopeID;
                        return; //不更新Current
                    }
                }
                this.hostScopeID = Guid.Empty;

                if (currentScope != null)
                {
                    //更新前,将之前的事务环境加入“事务链”.
                    if (rootScope.scopeStack == null)
                        rootScope.scopeStack = new Stack<MyTransactionScope>();
                    rootScope.scopeStack.Push(currentScope);
                }
                currentScope = this;
            }
        }
        /// <summary>
        /// 构造方法
        /// </summary>
        public MyTransactionScope()
            : this(System.Transactions.TransactionScopeOption.Required, TimeSpan.Zero)
        {
        }
        /// <summary>
        /// 构造方法
        /// </summary>
        /// <param name="option">描述与此事务范围关联的事务要求</param>
        public MyTransactionScope(System.Transactions.TransactionScopeOption option)
            : this(option, TimeSpan.Zero)
        {
        }
        /// <summary>
        /// 构造方法
        /// </summary>
        /// <param name="timeout">事务超时时间</param>
        public MyTransactionScope(TimeSpan timeout)
            : this(System.Transactions.TransactionScopeOption.Required, timeout)
        {
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
            if (this.scopeOption == System.Transactions.TransactionScopeOption.Suppress)
                throw new NotSupportedException("Suppress事务不支持该方法");

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
            /*********************************************************************
            ** 说明：不管当前实例是哪个，只会按照“事务链”进行处理。即始终都是处理currentScope。
            **   rootScope的Option可能是:Required,RequiresNew,Suppress
            **   currentScope不是rootScope的情况下,Option可能是:RequiresNew,Suppress
            ** 
            ** 主体逻辑：
            **   if (currentScope.ScopeOption == Suppress)
            **   {
            **      没有可提交的内容，直接.
            **   }
            **   else
            **   {
            **      提交当前层级事务后，根据“事务链”更新currentScope。
            **   }
            **   根据“事务链”更新currentScope。
            **   如果当前处理的是根级事务，则将事务环境置空。
            *********************************************************************/

            if (rootScope == null || currentScope == null)
                return;
            if (this.hostScopeID != Guid.Empty)
                return;
            if (this.scopeID != currentScope.scopeID)
                throw new ApplicationException("事务调用顺序不正确");

            lock (lockObj)
            {
                try
                {
                    if (currentScope.ScopeOption != System.Transactions.TransactionScopeOption.Suppress)
                        currentScope.DisposeCore();
                }
                finally
                {
                    if (rootScope.ScopeID == currentScope.ScopeID)
                    {
                        if (rootScope.scopeStack != null)
                        {
                            rootScope.scopeStack.Clear();
                            rootScope.scopeStack = null;
                        }
                        rootScope = null;
                        currentScope = null;
                    }
                    else
                    {
                        if (rootScope.scopeStack != null && rootScope.scopeStack.Count > 0)
                            currentScope = rootScope.scopeStack.Pop();
                        else
                            currentScope = rootScope;
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

    /// <summary>
    /// 事务容器超时异常
    /// </summary>
    public class MyTransactionScopeTimeOut : ApplicationException
    {
        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="message">描述错误的消息</param>
        public MyTransactionScopeTimeOut(string message)
            : base(message)
        {

        }
    }
}
