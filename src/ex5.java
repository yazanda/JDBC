import java.io.*;
import java.sql.*;
import java.util.*;

public class ex5 {

    public static Connection connect() {
        Connection c;
        try {
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:StackOverFlow.db");
            System.out.println("Opened database successfully");
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e);
        }
        return c;
    }

    public static void load(Connection con) throws SQLException, FileNotFoundException {
        String fileName = "stckoverflow.csv";
        File f = new File(fileName);
        String table = "CREATE TABLE StackOverFlow (Idx INTEGER PRIMARY KEY, 'MainBranch' text,'Employment' text,'Country' text, 'Age1st' text,'LearnCode' text," +
                " 'YearsCode' text,'LanguagesWorkedWith' text, 'AgeStart' INTEGER, 'AgeEnd' INTEGER, 'Gender' text);";
        String insert = "insert into StackOverFlow values (?,?,?,?,?,?,?,?,?,?,?)";
        String table2= """
                    create table Languages(
                        Idx Integer,
                        Language text,
                        foreign key (Idx) references StackOverFlow
                    );
                    """;
        String insert2 = "insert into Languages values(?, ?);";
        Statement st = con.createStatement();
        st.executeUpdate(table);
        st.executeUpdate(table2);
        st.close();
        PreparedStatement execute = con.prepareStatement(insert);
        PreparedStatement language = con.prepareStatement(insert2);
        Scanner scanner = new Scanner(f);
        String curLine;
        String[] line = new String[11];
        int batch = 0;
        String ageStart = "", ageEnd = "";
        scanner.nextLine();
        int index;
        while (scanner.hasNextLine()) {
            curLine = scanner.nextLine();
            String[] sep = curLine.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            index = Integer.parseInt(sep[0]);
            if (sep.length >= 9) {
                if (sep[8].equals("") || sep[8].charAt(0) == 'P' || sep[8].length() == 0) {
                    ageStart = "0";
                    ageEnd = "0";
                } else if (sep[8].charAt(0) == 'U') {
                    ageStart = "0";
                    ageEnd = String.valueOf(Integer.parseInt(sep[8].replaceAll("[\\D]", "")));
                } else if (sep[8].charAt(0) == 'O') {
                    ageStart = String.valueOf(Integer.parseInt(sep[8].replaceAll("[\\D]", "")));
                    ageEnd = "100";
                } else if (sep[8].indexOf('-') != -1) {
                    String[] range = sep[8].split("-");
                    ageStart = String.valueOf(Integer.parseInt(range[0].replaceAll("[\\D]", "")));
                    ageEnd = String.valueOf(Integer.parseInt(range[1].replaceAll("[\\D]", "")));
                }
            }
            System.arraycopy(sep, 0, line, 0, sep.length);
            line[8] = ageStart;
            line[9] = ageEnd;
            line[10] = sep.length == 10 ? sep[9] : "";

            execute.setInt(1, index);
            for (int i = 1; i < 11; i++) {
                if (i == 8 || i == 9) {
                    if (line[i].equals(""))
                        line[i] = "0";
                    execute.setInt(i + 1, Integer.parseInt(line[i]));
                } else
                    execute.setString(i + 1, line[i]);
            }
            ageStart = "";
            ageEnd = "";
            String[] languages = line[7].split(";");
            if(languages.length==0){
                language.setInt(1,index);
                language.setString(2,"");
                language.executeUpdate();
            }
            else{
                for(String i : languages){
                    language.setInt(1,index);
                    language.setString(2,i);
                    language.executeUpdate();
                }
            }
            execute.addBatch();
            batch++;
            if (batch % 100 == 0){
                execute.executeBatch();
            }
        }
        scanner.close();
        execute.close();
        language.close();
    }

    public static void languageQuery(Connection con){
        Scanner scan = new Scanner(System.in);
        System.out.println("enter programming languages separated by coma ',' for example - Python,C#,Java :");
        String lang = scan.nextLine();
        String[] langs = lang.split(",");
        String table = "select s.Employment, s.Country, s.Gender from StackOverFlow s, Languages l where s.Idx = l.Idx and l.language = ";
        String query = table;
        for(int i = 0; i < langs.length; i++){
            query += "'" + langs[i] + "'";
            if(i != langs.length-1) query += " intersect " + table;
        }
        query += ";";
        Statement st;
        try {
            st = con.createStatement();
            ResultSet rs =  st.executeQuery(query);
            while(rs.next()){
                System.out.println("Employment: " + rs.getString(1) + ", City: " + rs.getString(2) +
                        ", Gender: " + rs.getString(3));
            }
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        scan.close();
    }

    public static void ageQuery(Connection con){
        Scanner scan1 = new Scanner(System.in);
        Scanner scan2 = new Scanner(System.in);
        System.out.println("Please enter an age average in format of two numbers for example 18 35 :");
        int firstNum = scan1.nextInt();
        int secNum = scan1.nextInt();
        System.out.print("Please enter a city name if you don't want press enter :");
        String city = scan2.nextLine();
        String query = "select count(Idx) from StackOverFlow where AgeStart <= " + firstNum + " and AgeEnd >= " + secNum;
        if(!city.equals(""))
            query += " and Country == '" + city + "'";
        query += ";";
        Statement st;
        try {
            st = con.createStatement();
            ResultSet rs =  st.executeQuery(query);
            while(rs.next()){
                System.out.println("Count: " + rs.getString(1));
            }
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        scan1.close();
        scan2.close();
    }

    public static void specialQuery(Connection con){
        String query = "select s.* from StackOverFlow s, Languages l where s.Idx = l.Idx and s.Country == 'United States of America' " +
                "group by l.Idx HAVING count(l.Language) <= 2;";
        Statement st;
        try {
            st = con.createStatement();
            ResultSet rs =  st.executeQuery(query);
            while(rs.next()){
                for(int i = 1; i < 12; i++)
                    System.out.print(rs.getString(i) + "\t");
                System.out.println();
            }
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static Set<String> ListOfLanguages(Connection con) throws SQLException{
        ResultSet res;
        String select = "select distinct LanguagesWorkedWith from stackOverFlow";
        Statement execute = con.createStatement();
        res = execute.executeQuery(select);
        Set<String> langs = new HashSet<>();
        while(res.next()){
            String[] l = res.getString("LanguagesWorkedWith").split(";");
            Collections.addAll(langs, l);
        }
        execute.close();
        return langs;

    }

    public static void main(String[] args) throws SQLException, FileNotFoundException {
        Connection connect = connect();
        Scanner scan = new Scanner(System.in);
        int input;
        boolean loaded = false;
        while(true){
            System.out.println("What do you want to do?\n"+
                               "1.Load Data\n2.Language\n3.Age\n4.Special\n5.Exit");
            input = scan.nextInt();
            if(input == 1) {
                if(!loaded) {
                    System.out.println("loading data ..");
                    load(connect);
                    System.out.println("data loaded!");
                    loaded = true;
                }else System.out.println("data already loaded");
            }
            else if(input == 2) {
                Set<String> langs = ListOfLanguages(connect);
                System.out.println(langs);
                languageQuery(connect);
            }
            else if(input == 3)
                ageQuery(connect);
            else if(input == 4)
                specialQuery(connect);
            else if(input == 5)
                break;
        }

        System.out.print("bye!");
    }
}