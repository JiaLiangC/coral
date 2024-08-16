package com.linkedin.coral.coralservice.apps;

import com.linkedin.coral.coralservice.apps.err.DefaultErrorHandler;
import com.linkedin.coral.coralservice.apps.err.ErrorHandler;
import com.linkedin.coral.coralservice.apps.parser.HiveSqlParser;
import com.linkedin.coral.coralservice.apps.parser.SqlParser;
import com.linkedin.coral.coralservice.apps.parser.SqlParserFactory;
import com.linkedin.coral.coralservice.apps.plugin.PluginManager;
import com.linkedin.coral.coralservice.apps.plugin.PluginRegistry;
import com.linkedin.coral.coralservice.apps.transformer.SqlTransformer;
import com.linkedin.coral.coralservice.apps.transformer.SqlTransformers;
import com.linkedin.coral.hive.hive2rel.parsetree.parser.ASTNode;
import com.linkedin.coral.hive.hive2rel.parsetree.parser.CoralParseDriver;
import com.linkedin.coral.hive.hive2rel.parsetree.parser.ParseDriver;
import com.linkedin.coral.hive.hive2rel.parsetree.parser.ParseException;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.dialect.SparkSqlDialect;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlTransformationApplication {
    private final ConfigurationManager configManager;
    private final PluginManager pluginManager;
    private final PluginRegistry pluginRegistry;

    private SqlTransformationApplication(Builder builder) {
        this.configManager = builder.configManager;
        this.pluginManager = builder.pluginManager;
        this.pluginRegistry = builder.pluginRegistry;
    }

    public static class Builder {
        private ConfigurationManager configManager;
        private PluginManager pluginManager;
        private PluginRegistry pluginRegistry;
        private String configFile =null;
        private String pluginDir = null;

        public Builder() {
            this.configManager = new ConfigurationManager();
            this.pluginManager = PluginManager.getInstance();
            this.pluginRegistry =  pluginManager.getRegistry();
        }

        public Builder configFile(String configFile) {
            this.configFile = configFile;
            return this;
        }

        public Builder pluginDir(String pluginDir) {
            this.pluginDir = pluginDir;
            return this;
        }

        public SqlTransformationApplication build() {
            configManager.loadConfig(configFile);
            if(pluginDir!=null && !pluginDir.isEmpty()){pluginManager.loadPlugins(pluginDir);}
            pluginManager.initializePlugins();
            return new SqlTransformationApplication(this);
        }
    }

    public String transformSql(String inputSql, String sourceDialect, String targetDialect) {

        // 1.TransformationRule 一定会带source 和target rule 的属性，因为Rule是服务于从a 方言转换为b方言的
        // 2.pluginRegistry 获得所有 从a到b 方言转换的rule,然后使用 继承了 sql shuttle 的 SqlNode Converter 转换后的 sql node 生成 Sql String

        //get source dialect parser
        SqlParser parser = SqlParserFactory.createParser(sourceDialect, pluginRegistry);

        //get rules from pluginRegistry by source and target sqldialect
        ErrorHandler errorHandler = new DefaultErrorHandler(); // 假设有一个默认实现

        SqlDialectConverter converter = new SqlDialectConverter(pluginRegistry, parser, errorHandler);

        String sparkSql = converter.convert(inputSql, HiveSqlDialect.DEFAULT, SparkSqlDialect.DEFAULT);
        System.out.println("Converted SQL: " + sparkSql);

        return sparkSql;
    }

    public void shutdown() {
        pluginManager.shutdownPlugins();
    }

    public static void main(String[] args) throws ParseException, org.apache.hadoop.hive.ql.parse.ParseException, IOException {
        String sql11_ddl = "select  i_item_id ,i_item_desc  ,i_category  ,i_class  ,i_current_price ,sum(ws_ext_sales_price) as itemrevenue  ,sum(ws_ext_sales_price)*100/sum(sum(ws_ext_sales_price)) over (partition by i_class) as revenueratio from  web_sales ,item  ,date_dim where  ws_item_sk = i_item_sk  and i_category in ('Electronics', 'Books', 'Women') and ws_sold_date_sk = d_date_sk and d_date between cast('1998-01-06' as date)  and (cast('1998-01-06' as date) + 30 days) group by  i_item_id ,i_item_desc  ,i_category ,i_class ,i_current_price order by  i_category ,i_class ,i_item_id ,i_item_desc ,revenueratio limit 100";
        String sql1_ddl = " CREATE EXTERNAL TABLE complex_example ( id INT COMMENT 'Unique identifier', name STRING, age INT CHECK (age >= 18) ENABLE, email STRING UNIQUE DISABLE NOVALIDATE, preferences MAP<STRING, STRING>, tags ARRAY<STRING>, address STRUCT<street:STRING, city:STRING, zip:INT>, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() ) COMMENT 'A complex table example' PARTITIONED BY (year INT, month INT) CLUSTERED BY (id) SORTED BY (name ASC) INTO 16 BUCKETS ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY ':' STORED AS ORC LOCATION '/user/hive/complex_example' TBLPROPERTIES ('creator'='Data Team', 'created_at'='2023-05-01')";
        String sql13_ddl = "CREATE TABLE basic_table (id INT, name STRING)";
        String sql12_ddl = "CREATE TABLE apachelog (host STRING, identity STRING, user1 STRING, time1 STRING,request STRING,status STRING,size STRING,referer STRING, agent STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe' WITH SERDEPROPERTIES ('input.regex' = '([^]*) ([^]*) ([^]*) (-|\\[^\\]*\\]) ([^ \"]*|\"[^\"]*\") (-|[0-9]*) (-|[0-9]*)(?: ([^ \"]*|\".*\") ([^ \"]*|\".*\"))?')STORED AS TEXTFILE";

        String createDb1 = "CREATE DATABASE IF NOT EXISTS finance_db COMMENT 'Database for financial data analysis' LOCATION '/user/hive/warehouse/finance.db' MANAGEDLOCATION '/user/hive/warehouse/finance_managed.db' WITH DBPROPERTIES ('creator' = 'John Doe','department' = 'Finance','created_on' = '2024-08-12','version' = '1.0')";
        String createDb =  "CREATE REMOTE DATABASE IF NOT EXISTS external_sales COMMENT 'Remote database for sales data from external systems' USING mysql_connector WITH DBPROPERTIES ( 'connector.host' = 'remote-mysql-server.example.com', 'connector.port' = '3306', 'connector.user' = 'hive_user', 'connector.password' = 'secret_password', 'connector.database' = 'sales_data' )";
        String createDb2 = "CREATE DATABASE analytics_db LOCATION '/data/analytics' WITH DBPROPERTIES ( 'purpose' = 'Data analysis and reporting', 'team' = 'Data Science' )";

        String dropdatabase  = "DROP DATABASE IF EXISTS financial_analytics_2023_q4 CASCADE";
        String dropdatabase1  = "DROP SCHEMA customer_data_warehouse RESTRICT";
        String dropdatabase2  = "DROP DATABASE IF EXISTS `data-science_projects.2024`";

        String alter0 =  "ALTER TABLE table_name CHANGE COLUMN old_col_name new_col_name INT COMMENT 'New column comment' AFTER existing_column CASCADE";
        String alter3 = "ALTER TABLE list_bucket_single  SKEWED BY (key) ON (1,5,6) STORED AS DIRECTORIES";
        String alter4 = "ALTER TABLE list_bucket_single  SKEWED BY (key) ON (1,5,6) STORED AS DIRECTORIES";


        String switchDatabaseStatement = "USE database_name";
        ParseDriver pd = new CoralParseDriver(false);
        ASTNode root = pd.parse(alter3);
        HiveAstPrinter.printAstTree(root);

        HiveSqlParser parser = new HiveSqlParser(HiveSqlDialect.DEFAULT);
        SqlNode rootnode = parser.parse(alter3);
        CalciteSqlNodeTreePrinter  printer = new CalciteSqlNodeTreePrinter();
        String detailedOutput = printer.print(rootnode);
        System.out.println(detailedOutput);

        SqlTransformationApplication app = new SqlTransformationApplication.Builder().build();
        String result = app.transformSql(alter3,"Hive","Spark");
        System.out.println(result);
    /*        ArrayList<String> errSqls = new ArrayList<>();
            String[] sqlStatements = parseSqlFile("alter.sql");

            for (String sql : sqlStatements) {
                try {
                    parseSql(sql);
                }catch (Exception e){
                    errSqls.add(sql);
                    System.err.println("---------Error reading file: " + e.getMessage()+ sql);
                }
            }


            System.out.println("totaly "+ errSqls.size()+"error");
            for(String sql : errSqls){
                System.out.println(sql+";");
            }*/


    }


    private static final Pattern SQL_DELIMITER = Pattern.compile(";(?=(?:[^']*'[^']*')*[^']*$)");
    public static String[] parseSqlFile(String fileName) throws IOException {
        List<String> sqlStatements = new ArrayList<>();

        try (InputStream is = SqlTransformationApplication.class.getClassLoader().getResourceAsStream(fileName);
             BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {

            if (is == null) {
                throw new IOException("File not found: " + fileName);
            }

            String line;
            StringBuilder sqlBuilder = new StringBuilder();

            while ((line = reader.readLine()) != null) {
                sqlBuilder.append(line).append("\n");
            }

            String fullSql = sqlBuilder.toString().trim();
            Matcher matcher = SQL_DELIMITER.matcher(fullSql);

            int lastEnd = 0;
            while (matcher.find()) {
                String sql = fullSql.substring(lastEnd, matcher.start()).trim();
                if (!sql.isEmpty()) {
                    sqlStatements.add(sql);
                }
                lastEnd = matcher.end();
            }

            // 添加最后一个SQL语句（如果有的话）
            String lastSql = fullSql.substring(lastEnd).trim();
            if (!lastSql.isEmpty()) {
                sqlStatements.add(lastSql);
            }
        }

        return sqlStatements.toArray(new String[0]);
    }

    private static void parseSql(String sql) throws org.apache.hadoop.hive.ql.parse.ParseException, ParseException {
            ParseDriver pd = new CoralParseDriver(false);
            ASTNode root = pd.parse(sql);
            System.out.println("Parsing SQL: " + sql);
            HiveAstPrinter.printAstTree(root);
    }

}
