
import net.sourceforge.pinyin4j.PinyinHelper;
import net.sourceforge.pinyin4j.format.HanyuPinyinCaseType;
import net.sourceforge.pinyin4j.format.HanyuPinyinOutputFormat;
import net.sourceforge.pinyin4j.format.HanyuPinyinToneType;
import net.sourceforge.pinyin4j.format.exception.BadHanyuPinyinOutputFormatCombination;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class ReadFileAccess {
    // MySQL 8.0 以下版本 - JDBC 驱动名及数据库 URL
    private static String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static String DB_URL = "jdbc:mysql://localhost:3306/goods_test?useSSL=false";
    private static final Log log = LogFactory.getLog(ReadFileAccess.class);

    // 全部导入设为空
//    private static final String name = "TakeRecord";

    //     MySQL 8.0 以上版本 - JDBC 驱动名及数据库 URL
//    static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
//    static final String DB_URL = "jdbc:mysql://localhost:3306/goods?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    // 数据库的用户名与密码，需要根据自己的设置
    private static String USER = "root";
    private static String PASS = "root";
    private static String tables = "CaiLiaoBaseInform,CaiLiaoKuCun,CaiLiaopRuKu,KCQiChuShu,KuBie,LeiBie,supplier,TakeRecord2";
    private static List<String> tableList = new ArrayList<String>();

    private static void readFileAccess(File mdbFile, String dwmc, String pwd, String name, String add) {
        Properties prop = new Properties();
        prop.put("charSet", "gb2312"); // 这里是解决中文乱码
        prop.put("user", "username");
        prop.put("password", pwd);
        String url = "jdbc:odbc:driver={Microsoft Access Driver (*.mdb)};DBQ="
                + mdbFile.getAbsolutePath();
        Statement stmt = null;
        ResultSet rs = null;

        Map<String, Integer> map = new HashMap<String, Integer>();
        int wid = getSuffix(dwmc);
        IdWorker idWorker = new IdWorker(wid, 1, 1);
        try {
            Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
            Connection conn = DriverManager.getConnection(url, prop);
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet set = metaData.getTables(mdbFile.getAbsolutePath(), null, null, new String[]{"TABLE"});
//            ResultSet set = metaData.getTables(mdbFile.getAbsolutePath(), null, null, new String[]{"VIEW"});

            // 获取第一个表名
            while (set.next()) {
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
                String tableName = set.getString("TABLE_NAME");
                System.out.println(tableName);
                log.info("读取到表：" +tableName);
                // 检查是否在导入列表中
                boolean flag = false;
                for (String s : tableList) {
                    if (tableName.contains(s)) {
                        flag = true;
                        break;
                    }
                }

                if (flag) {
                    stmt = conn.createStatement();
                    // 读取第一个表的内容
                    rs = stmt.executeQuery("select * from " + tableName);
                    ResultSetMetaData data = rs.getMetaData();
                    StringBuffer sb = new StringBuffer();
                    List<String> columnData = new ArrayList<String>();
                    // 合并每年的出库记录
                    if (tableName.contains("TakeRecord2")) {
                        tableName = "take_record";

                        if (!map.containsKey(tableName)) {
                            map.put(tableName, 1);
                        } else {
                            map.put(tableName, map.get(tableName) + 1);
                        }

                    }
                    // 合并分散在不同库的材料信息
                    if (tableName.contains("CaiLiaoBaseInform")) {
                        tableName = "cailiaobaseinform";

                        if (!map.containsKey(tableName)) {
                            map.put(tableName, 1);
                        } else {
                            map.put(tableName, map.get(tableName) + 1);
                        }
                    }

                    // 构造插入sql
                    sb.append("insert into " + getLowerStr(tableName) + " (");
                    sb.append("id,dwmc,");
                    for (int i = 1; i <= data.getColumnCount(); i++) {
                        sb.append(trimColumn(data.getColumnName(i).toLowerCase())).append(",");
                    }
                    sb.append("cjsj) values ");

                    // 删除并创建新的表
                    for (int i = 1; i <= data.getColumnCount(); i++) {
                        columnData.add(data.getColumnName(i).toLowerCase());
                    }
                    generateTableMysql(getLowerStr(tableName), columnData);

                    // 清空单个公司之前的数据，全部替换为新的
                    try {
                        if (map.containsKey(tableName) && map.get(tableName) > 1) {
                            System.out.println("合并" + tableName);
                        } else {
                            clearTable(getLowerStr(tableName), dwmc);
                        }

                    } catch (Exception e) {
                        System.out.println("不存在");
                    }

                    log.info("开始插入到表：" + tableName);
                    while (rs.next()) {
                        StringBuilder value = new StringBuilder();
                        value.append("(");

                        value.append("'").append(idWorker.nextId()).append("','" + dwmc + "',");

                        for (int i = 1; i <= data.getColumnCount(); i++) {
                            value.append("'").append(trimValue(rs.getString(i))).append("',");
                        }
                        value.append("'").append(df.format(new Date())).append("')");
                        insertIntoMysql(sb.toString() + value.toString());
                    }
                }
            }
            System.out.println("数据导入完成");
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
    }

    public static void getProperties() throws IOException {
        InputStream p = ReadFileAccess.class.getResourceAsStream("/jdbc.properties");
        Properties prop = new Properties();
        prop.load(p);
        // 根据关键字获取value值
        USER = prop.getProperty("user");
        PASS = prop.getProperty("pwd");
        JDBC_DRIVER = prop.getProperty("jdbc_driver");
        DB_URL = prop.getProperty("db_url");

        InputStream p2 = ReadFileAccess.class.getResourceAsStream("/tables.properties");
        Properties prop2 = new Properties();
        prop2.load(p2);
        // 根据关键字获取value值
        tables = prop2.getProperty("tables");
        tableList = Arrays.asList(tables.split(","));
        log.info(tables);
//        System.out.println(USER + PASS + JDBC_DRIVER + DB_URL);

    }

    public static void main(String[] args) throws IOException {
        // TakeRecord表zhanghu字段字段长度不能为25
        String dwmc = args[0];
        String pwd = args[1];
        String filePath = args[2];
        String table = null;
        String add = null;
        if (args.length > 3) {
            add = args[3];
        }
//        if (args.length > 4) {
//            table = args[4];
//        }

//        String path = "E:\\3\\wuzi\\2\\laborProtect.mdb";
        // 读取数据库配置
        getProperties();

        readFileAccess(new File(filePath), dwmc, pwd, table, add);
    }

    public static Connection getConnection() {
        Connection conn = null;
        try {
            // 注册 JDBC 驱动
            Class.forName(JDBC_DRIVER);
            // 打开链接
//            System.out.println("连接数据库...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            return conn;
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
            return null;
        }
    }

    public static void insertIntoMysql(String sql) {
//        System.out.println(sql);
//        log.info(sql);
        Statement stmt = null;
        try {
            Connection conn = getConnection();
            // 执行查询
//            System.out.println(" 实例化Statement对象...");
            stmt = conn.createStatement();
            stmt.execute(sql);
            // 完成后关闭
            close(stmt);
            close(conn);
        } catch (SQLException se) {
            // 处理 JDBC 错误
            se.printStackTrace();
            log.error(se.getMessage());
        } catch (Exception e) {
            // 处理 Class.forName 错误
            e.printStackTrace();
            log.error(e.getMessage());
        } finally {
            // 关闭资源
            try {
                if (stmt != null) stmt.close();
            } catch (SQLException se2) {
                log.error(se2.getMessage());
            }// 什么都不做
        }
    }

    private static void executeSql(String sql) {
        log.info(sql);
        Statement stmt = null;
        try {
            Connection conn = getConnection();

            stmt = conn.createStatement();
            stmt.execute(sql);
            close(stmt);
            close(conn);
        } catch (SQLException e) {
            e.printStackTrace();
            log.error(e.getMessage());
        } finally {
            try {
                if (stmt != null) stmt.close();
            } catch (SQLException se2) {
            }
        }
    }

    private static String trimColumn(String s) {
//        if ("dwmc".equals(s)) {
//            return "dwmcs";
//        }
//        if ("cjsj".equals(s)) {
//            return "cjsjs";
//        }
        return "id".equals(s) ? "ids" : s;
    }

    private static String trimValue(String s) {
        if (!"".equals(s) && s != null) {
            if (s.contains("\\") || s.contains("'")) {
                return s.replace("\\", "\\\\").replace("'", "");
            }
        }
        return s;
    }


    private static String getLowerStr(String name) {
        if (name != null && !"".equals(name)) {
            StringBuilder str = new StringBuilder();
            for (int i = 0; i < name.length(); i++) {
                String characters = String.valueOf(name.charAt(i));
                str.append(converterToFirstSpell(characters).toLowerCase().charAt(0));
            }
            return str.toString();
        }
        return "";
    }

    private static String converterToFirstSpell(String str) {
        StringBuilder pinyinName = new StringBuilder();
        char[] nameChar = str.toCharArray();
        HanyuPinyinOutputFormat defaultFormat = new HanyuPinyinOutputFormat();
        defaultFormat.setCaseType(HanyuPinyinCaseType.LOWERCASE);
        defaultFormat.setToneType(HanyuPinyinToneType.WITHOUT_TONE);
        for (char characters : nameChar) {
            String string = String.valueOf(characters);
            if (string.matches("[\\u4e00-\\u9fa5]")) {
                try {
                    String[] mPinyinArray = PinyinHelper.toHanyuPinyinStringArray(characters, defaultFormat);
                    pinyinName.append(mPinyinArray[0]);
                } catch (BadHanyuPinyinOutputFormatCombination e) {
                    e.printStackTrace();
                }
            } else {
                pinyinName.append(characters);
            }
        }
        return pinyinName.toString();
    }

    private static void clearTable(String tableName, String dwmc) {
//        String sql = "truncate table" + tableName;
        String sql = "delete from " + tableName + " where dwmc = '" + dwmc + "'";
        executeSql(sql);
    }

    private static String generateTableMysql(String tableName, List<String> columnData) {
        StringBuilder sqlSb = new StringBuilder();

        sqlSb.append("create table if not exists " + tableName + "( \n");
        sqlSb.append("    id  varchar(32),\n    dwmc   varchar(10),\n");
        for (String name : columnData) {
            sqlSb.append("    " + trimColumn(name).toLowerCase() + " ");
            sqlSb.append("  varchar(255),\n");
        }
        sqlSb.append("    cjsj  varchar(32),\n").append("    primary key (id) \n);");
        executeSql(sqlSb.toString());
        return sqlSb.toString();
    }


    private static void close(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                log.error(e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private static void close(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                log.error(e.getMessage());
                e.printStackTrace();
            }
        }
    }

// 获取不同公司数据id
    private static Integer getSuffix(String dwmc) {
        Statement stmt = null;
        Integer id = 1;
        try {
            Connection conn = getConnection();
            PreparedStatement ps = conn.prepareStatement("SELECT remark from sys_dd where name = ?");
            ps.setObject(1, dwmc);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                id = rs.getInt(1);
            }
            close(conn);
        } catch (SQLException e) {
            e.printStackTrace();
            log.error(e.getMessage());
        } finally {
            try {
                if (stmt != null) stmt.close();
            } catch (SQLException se2) {
            }
        }
        return id==null?1:id;
    }

}


































