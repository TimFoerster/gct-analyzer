using System;
using System.Collections.Generic;
using System.Configuration;
using MySql.Data.MySqlClient;

namespace Sim_Calc
{

    class Connection
    {
        private MySqlConnection connection;

        const int timeout = 600;


        //Constructor
        public Connection()
        {
            Initialize();
        }

        //Initialize values
        private void Initialize()
        {
            string database = "app";
#if DEBUG
            string server = "localhost";
            string uid = "root";
            string password = "";
            string port = "33306";
# else
            string server = "blesim";
            string uid = "root";
            string password = "mN5kXQb4tamEsuuXnlK3";
            string port = "3306";
# endif

            string connectionString;
            connectionString = "SERVER=" + server + "; Port=" + port  + "; DATABASE=" +
                database + "; UID=" + uid + "; PASSWORD=" + password + ";Connect Timeout=" + timeout;

            connection = new MySqlConnection(connectionString);
        }

        //open connection to database
        private bool OpenConnection()
        {
            try
            {
                if (connection.State != System.Data.ConnectionState.Open)
                    connection.Open();
                return true;
            }
            catch (MySqlException ex)
            {
                //When handling errors, you can your application's response based 
                //on the error number.
                //The two most common error numbers when connecting are as follows:
                //0: Cannot connect to server.
                //1045: Invalid user name and/or password.
                switch (ex.Number)
                {
                    case 0:
                        Console.WriteLine("Cannot connect to server.  Contact administrator");
                        break;

                    case 1045:
                        Console.WriteLine("Invalid username/password, please try again");
                        break;
                }
                return false;
            }
        }

        //Close connection
        public bool CloseConnection()
        {
            try
            {
                connection.Close();
                return true;
            }
            catch (MySqlException ex)
            {
                Console.WriteLine(ex.Message);
                return false;
            }
        }

        public MySqlDataReader? Query(string query)
        {
            if (this.OpenConnection() == true)
            {
                MySqlCommand cmd = new MySqlCommand(query, connection);
                //Create a data reader and Execute the command
                MySqlDataReader dataReader = cmd.ExecuteReader();
                cmd.CommandTimeout = timeout;

                return dataReader;
            }
            return null;
        }

        public void Execute(string text)
        {
            if (this.OpenConnection() == true)
            {
                MySqlCommand cmd = connection.CreateCommand();
                cmd.CommandTimeout = timeout;
                cmd.CommandText = text;
                cmd.ExecuteNonQuery();
            }
        }

        public ulong Insert(string text)
        {
            if (this.OpenConnection() == true)
            {
                MySqlCommand cmd = connection.CreateCommand();
                cmd.CommandText = text;
                cmd.CommandTimeout = timeout;
                cmd.ExecuteNonQuery();
                return (ulong)cmd.LastInsertedId;
            }

            return 0;
        }

        //Select statement
        public List<string> Select(string query)
        {
            //Create a list to store the result
            List<string> list = new List<string>();

            //Open connection
            if (this.OpenConnection() == true)
            {
                //Create Command
                MySqlCommand cmd = new MySqlCommand(query, connection);
                cmd.CommandTimeout = timeout;

                //Create a data reader and Execute the command
                MySqlDataReader dataReader = cmd.ExecuteReader();

                //Read the data and store them in the list
                while (dataReader.Read())
                {
                    list.Add(dataReader["id"] + "," + dataReader["datetime"] + "," + dataReader["api_temperature"]);
                }

                //close Data Reader
                dataReader.Close();

                //close Connection
                this.CloseConnection();

                //return list to be displayed
                return list;
            }
            else
            {
                return list;
            }
        }

        //Count statement
        public int Count(string query)
        {
            int Count = -1;

            //Open Connection
            if (this.OpenConnection() == true)
            {
                //Create Mysql Command
                MySqlCommand cmd = new MySqlCommand(query, connection);
                cmd.CommandTimeout = timeout;

                //ExecuteScalar will return one value
                Count = int.Parse(cmd.ExecuteScalar() + "");

                //close Connection
                this.CloseConnection();

                return Count;
            }
            else
            {
                return Count;
            }
        }
    }
}
