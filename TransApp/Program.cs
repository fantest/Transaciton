using System;
using System.Collections.Generic;
using System.Text;

namespace FanTest.Trans
{
    class Program
    {
        static void Main(string[] args)
        {
            System.AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);

            Console.WriteLine("conn-1 {0}",  Manager.TestConn1() ? "is ok!" : "can not open!");

            //try
            //{
            //    NestTrans.Test2();
            //}
            //catch (Exception ex)
            //{
            //    Console.WriteLine("Call NestTrans.Test1() Error:\r\n" + ex.ToString());
            //}

            try
            {
                MyTrans.Test1();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Call MyTrans.Test1() Error:\r\n" + ex.ToString());
            }

            //try
            //{
            //    MyTransOld.Test1();
            //}
            //catch (Exception ex)
            //{
            //    Console.WriteLine("Call MyTransOld.Test1() Error:\r\n" + ex.ToString());
            //}

            Console.WriteLine("End...");
            Console.ReadLine();
        }

        static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            Console.WriteLine("未处理异常:" + e.ExceptionObject.ToString());
        }
    }
}
