package ReadingOracle.TextReading;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Mineczxc on 2016/3/8.
 */
public class reader_File_and_Group {
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        Connection con = null;// 创建一个数据库连接
        PreparedStatement pre = null;// 创建预编译语句对象，一般都是用这个而不用Statement
        ResultSet result = null;// 创建一个结果集对象
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");// 加载Oracle驱动程序
            System.out.println("开始尝试连接数据库！");
            // 114.212.83.50是oracle主机地址，hehe是精简版Oracle的默认数据库名
            String url = "jdbc:oracle:" + "thin:@114.212.83.50:1521:hehe";
            String user = "system";// 用户名,系统默认的账户名
            String password = "123";// 你安装时选设置的密码
            con = DriverManager.getConnection(url, user, password);// 获取连接
            System.out.println("连接成功！");
            String sql = "select * from wxgk.TB_WX_MSGMONITOR";// 预编译语句，“？”代表参数

            pre = con.prepareStatement(sql);// 实例化预编译语句
            result = pre.executeQuery();// 执行查询，注意括号中不需要再加参数
            //建立写入文件txt
            System.out.println("读取数据并写入文件！");
            int i=1, j=1;
            String path = "G:\\公安组文件\\5.1数据抽取并按天分割\\";
            while (result.next())
            {
                String type = result.getString("MSGTYPE");
                if(!type.trim().equals("1"))
                {
                    i++;
                    continue;
                }
                java.sql.Clob clob = result.getClob("CONTENT");
                if(clob == null)
                {
                    i++;
                    continue;
                }
                Reader inStream = clob.getCharacterStream();
                char[] c = new char[(int) clob.length()];
                inStream.read(c);
                String data = new String(c);
                String msg_time = result.getString("MSGTIME").trim();
                String path_folder = path + msg_time.split(" ")[0].trim();
                String user_name = result.getString("ACCOUNTNAME").trim();
                String recer_name = result.getString("RECERUSERNAME").trim();
                File folder = new File(path_folder);
                String file_name = (user_name.compareTo(recer_name) < 0) ? (user_name + "-" + recer_name) : (recer_name + "-" + user_name);
                File file = new File(path_folder + "\\" +file_name + ".txt");
                System.out.print(i+":");
                System.out.println(path_folder + "\\" +file_name + ".txt");
                if(folder.exists() && folder.isDirectory()) {
                    if (!file.exists()) file.createNewFile();
                }
                else
                {
                    folder.mkdir();

                    file.createNewFile();
                }
                FileWriter fw = new FileWriter(file,true);
                PrintWriter pw = new PrintWriter(fw);
                pw.println(data);
                pw.flush();
                fw.flush();
                pw.close();
                fw.close();
//                if(i%40000 == 0)
//                {
//                    System.out.print(i);
//                }
                i++;
            }

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        finally
        {
            try
            {
                // 逐一将上面的几个对象关闭，因为不关闭的话会影响性能、并且占用资源
                // 注意关闭的顺序，最后使用的最先关闭
                if (result != null)
                    result.close();
                if (pre != null)
                    pre.close();
                if (con != null)
                    con.close();
                System.out.println("数据库连接已关闭！");
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }


}
