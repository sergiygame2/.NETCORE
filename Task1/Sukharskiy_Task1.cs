using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Data.Sqlite;
using Newtonsoft.Json;

namespace NIAdoNetOne
{
    public class Sukharskiy_Task1
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Testing START:\n");
            File.Create("database.db");

            using (var connection = new SqliteConnection("Data Source=database.db;"))
            {
                connection.Open();
                Console.WriteLine("\n1.Open DB Connection. SUCCESS!");

                SqliteCommand cleaningCommand = new SqliteCommand("DROP TABLE IF EXISTS Companies;", connection);
                cleaningCommand.ExecuteNonQuery();
                string createTableSQL = "CREATE TABLE Companies(ID INTEGER , Title varchar(50), Country varchar(50), AddedDate Date, PRIMARY KEY(ID) )";
                SqliteCommand command = new SqliteCommand(createTableSQL, connection);
                command.ExecuteNonQuery();
                Console.WriteLine("\n2.Create table. SUCCESS!");

                //Data
                List<string[]> list = new List<string[]>();
                list.Add(new string[] { "NewtonIdeas", "Ukraine", "2016-07-20" });
                list.Add(new string[] { "GlobalLogic", "Ukraine", "2016-07-21" });
                list.Add(new string[] { "Netcracker", "Ukraine", "2016-07-21" });
                list.Add(new string[] { "Google", "USA", "2016-07-21" });
                list.Add(new string[] { "Microsoft", "USA", "2016-07-21" });
                list.Add(new string[] { "infopulse", "Ukraine", "2016-07-21" });
                list.Add(new string[] { "Ciklum", "Ukraine", "2016-07-21" });
                list.Add(new string[] { "Inform", "France", "2016-07-21" });
                list.Add(new string[] { "Pazzl", "Spain", "2016-07-21" });
                list.Add(new string[] { "Merkel", "Germany", "2016-07-21" });

                //Inserting Data
                foreach ( var item in list)
                {
                    ExecuteQuery(@"INSERT INTO Companies(Title,Country,AddedDate) VALUES(@Title,@Country,@AD)", connection, item);
                }
                Console.WriteLine("\n3.Insert 10 Companies. SUCCESS!");

                //Testing
                Console.WriteLine("\n4.Select company with max id:");
                using (SqliteCommand selectMaxCommand = new SqliteCommand("SELECT ID,Title FROM Companies WHERE ID=(SELECT MAX(ID) FROM Companies) ", connection))
                {
                    selectMaxCommand.ExecuteNonQuery();
                    SqliteDataReader reader = selectMaxCommand.ExecuteReader();
                    while (reader.Read())
                        Console.WriteLine("ID: " + reader["ID"] + " Title: " + reader["Title"]);
                }
                
                Console.WriteLine("\n5.Update country:");
                using (SqliteCommand updateCountryCommand = new SqliteCommand("UPDATE Companies SET Country=@NewCountry WHERE Country=@OldCountry", connection))
                {
                    updateCountryCommand.Parameters.AddWithValue("@NewCountry","USA");
                    updateCountryCommand.Parameters.AddWithValue("@OldCountry", "Ukraine");
                    updateCountryCommand.ExecuteNonQuery();
                }
                PrintDB(connection);

                Console.WriteLine("\n6.Delete all not USA companies:");
                using (SqliteCommand deleteCommand = new SqliteCommand("DELETE FROM Companies WHERE Country NOT IN(SELECT Country FROM Companies WHERE Country=@CountryToDelete)", connection))
                {
                    deleteCommand.Parameters.AddWithValue("@CountryToDelete", "USA");
                    deleteCommand.ExecuteNonQuery();
                }
                PrintDB(connection);

                Console.Write("\n7.Number of records: ");
                using (SqliteCommand countCommand = new SqliteCommand("SELECT COUNT(*) AS Amount FROM Companies", connection))
                {
                    countCommand.ExecuteNonQuery();
                    var reader = countCommand.ExecuteReader();
                    reader.Read();
                    Console.Write(reader["Amount"]);
                }

                Console.Write("\n\n8.Reading all records: \n");
                PrintDB(connection);
                
                Console.Write("\n\n9.User input: \n");
                SqliteTransaction transaction;
                Console.Write("Input some companies as JSON structure:\nExample: Title: \"Company\", Country: \"Ukraine\", Date:\"2016-01-01\"\n(Type 'q' to exit , 'p' to print full table)");
                Console.Write("\n->");
                var input = Console.ReadLine();

                while (true)
                {
                    SqliteCommand userTComand = new SqliteCommand(@"INSERT INTO Companies(Title, Country, AddedDate) VALUES(@Title, @Country, @AD)", connection);
                    transaction = connection.BeginTransaction();
                    userTComand.Connection = connection;
                    userTComand.Transaction = transaction;

                    switch (input[0])
                    {
                        case 'q':
                            transaction.Commit();
                            transaction.Dispose();
                            Console.WriteLine("Transaction committed.");
                            Environment.Exit(0);
                            break;
                        case 'p':
                            transaction.Dispose();
                            PrintDB(connection);
                            break;
                        case '{':
                            try
                            {
                                var c = JsonConvert.DeserializeObject<Company>(input);
                                    
                                userTComand.Parameters.AddWithValue("@Title", c.title);
                                userTComand.Parameters.AddWithValue("@Country", c.country);
                                userTComand.Parameters.AddWithValue("@AD", c.date);
                                userTComand.ExecuteNonQuery();
                                    
                                transaction.Commit();

                                Console.WriteLine("Inserted.");
                             }
                             catch (Exception ex)
                             {
                                Console.WriteLine("Commit Exception Type: {0}", ex.GetType());
                                Console.WriteLine("  Message: {0}", ex.Message);

                                try
                                {
                                    transaction.Rollback();
                                }
                                catch (Exception ex2)
                                {
                                    Console.WriteLine("Rollback Exception Type: {0}", ex2.GetType());
                                    Console.WriteLine("  Message: {0}", ex2.Message);
                                }
                             }
                             break;
                           default:
                            Console.WriteLine("Wrong input :( Try again later!");
                            break;
                        }
                    Console.Write("\n->");
                    input = Console.ReadLine();
                }
            }
            Console.WriteLine("\nSUCCESS!\nEnd.");
            Console.ReadLine();
        }

        class Company
        {
            public string title { get; set;}
            public string country { get; set; }
            public string date { get; set; }
            
            public Company(string T, string C, string D)
            {
                title = T; country = C; date = D;
            }
            public override string ToString()
            {
                return $"{title}, {country}, {date}";
            }
        }

        public static void ExecuteQuery( string query, SqliteConnection conn, string[] param)
        {
            SqliteCommand command = new SqliteCommand(query, conn);
            command.Parameters.AddWithValue("@Title", param[0]);
            command.Parameters.AddWithValue("@Country", param[1]);
            command.Parameters.AddWithValue("@AD", param[2]);
            command.ExecuteNonQuery();
        }

        public static void PrintDB(SqliteConnection conn)
        {
            SqliteCommand command = new SqliteCommand("SELECT * FROM Companies", conn);
            
            command.ExecuteNonQuery();

            SqliteDataReader reader = command.ExecuteReader();
            while (reader.Read())
                Console.WriteLine("ID: "+reader["ID"]+" Title: " + reader["Title"] + " Country: " + reader["Country"]+ " Date: "+ reader["AddedDate"]);
        }

    }
}
