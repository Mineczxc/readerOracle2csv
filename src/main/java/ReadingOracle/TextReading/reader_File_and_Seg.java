package ReadingOracle.TextReading;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class reader_File_and_Seg {

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
            String sql = "select * from wxgk.TB_WX_GROUPMESSAGE";// 预编译语句，“？”代表参数
            pre = con.prepareStatement(sql);// 实例化预编译语句
            //pre.setString(1, "刘显安");// 设置参数，前面的1表示参数的索引，而不是表中列名的索引
            result = pre.executeQuery();// 执行查询，注意括号中不需要再加参数
            // Reader inStream = null;
            //建立写入文件txt
            System.out.println("读取数据并写入文件！");
            int i=1, j=1, block = 1;
            String path = "G:\\公安组文件\\4_重新读取数据并按组分割\\WX_G" + String.format("%02d",j) + ".txt";
            File filename = new File(path);
            if(!filename.exists())
            {
                filename.createNewFile();
            }
            FileOutputStream fos = new FileOutputStream(path);
            OutputStreamWriter osw = new OutputStreamWriter(fos);
            PrintWriter pw = new PrintWriter(osw);
            String temp_CMDID = new String("");
            while (result.next())
            {
                int type = result.getInt("MSGTYPE");
                java.sql.Clob clob = result.getClob("CONTENT");
                if(type != 1 || clob == null) continue;
                if(!temp_CMDID.equals(result.getString("CMDID")))
                {
                    pw.println("Block:" + block);
                    temp_CMDID = result.getString("CMDID");
                    block++;
                }
                Reader inStream = clob.getCharacterStream();
                char[] c = new char[(int) clob.length()];
                inStream.read(c);
                String data = new String(c);
                pw.println( data );
                clob.free( );
                inStream.close( );
                i++;
                if(i >= 40000)
                {
                    i=1;
                    j++;
                    pw.close();
                    path = "G:\\公安组文件\\4_重新读取数据并按组分割\\WX_G" + String.format("%02d",j) + ".txt";
                    filename = new File(path);
                    if(!filename.exists())
                    {
                        filename.createNewFile();
                    }
                    fos = new FileOutputStream(path);
                    osw = new OutputStreamWriter(fos);
                    pw = new PrintWriter(osw);
                }
            }
            pw.close();


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
