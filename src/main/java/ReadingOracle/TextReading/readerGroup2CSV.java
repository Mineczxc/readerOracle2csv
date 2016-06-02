package ReadingOracle.TextReading;

import java.io.*;
import java.sql.*;

/**
 * Created by 晨之星晨 on 2016/5/17.
 */
public class readerGroup2CSV {
    public static void main(String[] args){
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
            //String sql = "select * from wxgk.TB_WX_ACCOUNT";
            String sql = "select * from wxgk.TB_WX_GROUP";
            pre = con.prepareStatement(sql);// 实例化预编译语句
            //pre.setString(1, "刘显安");// 设置参数，前面的1表示参数的索引，而不是表中列名的索引
            result = pre.executeQuery();// 执行查询，注意括号中不需要再加参数
            // Reader inStream = null;
            //建立写入文件txt
            System.out.println("读取数据并写入文件！");
            //String path = "G:\\公安组文件\\WX_ACCOUNT表中的文件\\WX_ACCOUNT.csv";
            String path = "G:\\\\公安组文件\\\\WX_ACCOUNT表中的文件\\\\WX_GROUP.csv";
            File filename = new File(path);
            if(!filename.exists())
            {
                filename.createNewFile();
            }
            FileOutputStream fos = new FileOutputStream(path);
            OutputStreamWriter osw = new OutputStreamWriter(fos);
            PrintWriter pw = new PrintWriter(osw);
//            String[] property = new String[]{"ACCOUNTNAME","USERID","USERNAME","IMID","IDNO","EMAIL","MOBILEPHONE","NICKNAME","REGIP",
//                    "REGTIME","SEX","IDTYPE","TELEPHONE","COUNTRY","PROVINCE","CITY","ADDRESS","ICON","UPDATETIME","CMDID","OVERSEA"};
            String[] property = new String[]{"GROUPID","GROUPNAME","CREATOR","CREATORNICKNAME",
                        "CREATETIME","MEMBERNUM","LASTCHATTIME","UPDATETIME","CMDID"};

            for(String str:property){
                if(!str.matches("CMDID")) pw.print(str + ",");
                else pw.println(str);
            }

            while (result.next()){
                for(String t:property){
                    if(!t.matches("CMDID")) pw.print(result.getString(t) + ",");
                    else pw.println(result.getString(t));
                }
            }
            pw.close();

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try{
                if (result != null)
                    result.close();
                if (pre != null)
                    pre.close();
                if(con != null)
                    con.close();
                System.out.println("数据库连接已关闭！");
            }catch (Exception e){
                e.printStackTrace();
            }
        }

    }
}
