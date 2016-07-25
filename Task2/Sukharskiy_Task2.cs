using System;
using System.IO;
using Microsoft.Data.Sqlite;
using System.Data;

namespace NIAdoNetTwo
{
    public class Sukharskiy_Task2
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Testing START:\n");
            
            using (var connection = new SqliteConnection(@"Data Source="+Directory.GetCurrentDirectory()+@"\northwind.db"))
            {
                connection.Open();
                Console.WriteLine("Open DB connection. SUCCESS!");
                
                Console.WriteLine("\n1.Select all customers whose name starts with letter 'D' ");
                string selectDNames = @"SELECT ContactName From Customers WHERE ContactName LIKE 'D%' ";
                ExecuteQuery(connection, selectDNames);
                
                Console.WriteLine("\n2.Convert names of all customers to Upper Case ");
                string selectUpperNames = @"SELECT UPPER(ContactName) AS CN From Customers ";
                ExecuteQuery(connection, selectUpperNames);
                
                Console.WriteLine("\n3.Select distinct country from Customers ");
                string selectDistinctCountries = @"SELECT DISTINCT Country FROM Customers ";
                ExecuteQuery(connection, selectDistinctCountries);
                
                Console.WriteLine("\n4.Select Contact name from Customers Table from Lindon and title like 'Sales' ");
                string selectContacts = @"SELECT ContactName FROM Customers WHERE City='London' AND ContactTitle LIKE 'Sales%' ";
                ExecuteQuery(connection, selectContacts);
                
                Console.WriteLine("\n5.Select all orders id where was bought 'Tofu' ");
                string selectTofuOrders = @"SELECT OrderID FROM 'Order Details' WHERE ProductID IN 
                                                                        (SELECT ProductID FROM Products WHERE ProductName='Tofu') ";
                ExecuteQuery(connection, selectTofuOrders);
                
                Console.WriteLine("\n6.Select all product names that were shipped to Germany ");
                string selectGermanyOrders = @"SELECT ProductName FROM Products INNER JOIN ('Order Details' 
                                                            INNER JOIN Orders ON 'Order Details'.OrderID=Orders.OrderID 
                                                            )ON Products.ProductID = 'Order Details'.ProductID 
                                                            WHERE ShipCountry = 'Germany' ";
                ExecuteQuery(connection, selectGermanyOrders);
                
                Console.WriteLine("\n7.Select all customers that ordered 'Ikura' ");
                string selectIcuraOrders = @"SELECT ContactName FROM Customers INNER JOIN (Orders 
                                                            INNER JOIN ('Order Details'
                                                                INNER JOIN Products ON 'Order Details'.ProductID = Products.ProductID
                                                                )ON Orders.OrderID = 'Order Details'.OrderID
																)ON Customers.CustomerID = Orders.CustomerID 
                                                            WHERE Products.ProductName = 'Ikura' ";
                ExecuteQuery(connection, selectIcuraOrders);
                
                Console.WriteLine("\n8.Select all employees and any orders they might have ");
                string selectEmployeesAndAnyOrders = @"SELECT LastName, FirstName, OrderID FROM Employees INNER JOIN Orders ON Orders.EmployeeID = Employees.EmployeeID";
                ExecuteQuery(connection, selectEmployeesAndAnyOrders);
                
                //Запит не працює, оскільки FULL OUTER JOIN не підтримується, а щоб отриати усіх співробітників і усі замовлення потрібно використовувати саме його
                Console.WriteLine("\n9.Selects all employees, and all orders ");
                string selectEmployeesAndALLOrders = @"SELECT LastName, OrderID FROM Employees FULL OUTER JOIN Orders ON Orders.EmployeeID = Employees.EmployeeID";
                //ExecuteQuery(connection, selectEmployeesAndALLOrders);
                
                Console.WriteLine("\n10.Select all phones from Shippers and Suppliers ");
                string selectAllPhones = @"SELECT Shippers.Phone FROM Shippers UNION SELECT Suppliers.Phone FROM Suppliers ";
                ExecuteQuery(connection, selectAllPhones);
                
                Console.WriteLine("\n11.Count all customers grouped by city ");
                string countCitiesCustomers = @"SELECT City,COUNT(*) AS amount FROM Customers GROUP BY City";
                ExecuteQuery(connection, countCitiesCustomers);
                
                //Для заданих параметрів запит поверне 0 результатів. Якщо середнє UnitPrice поставити більше 17 результатів буде багато.
                Console.WriteLine("\n12.Select all customers that placed more than 10 orders with average Unit Price less than 17 ");
                string paramCustomers = @"SELECT ContactName FROM Customers
                                                            INNER JOIN (Orders INNER JOIN 'Order Details' 
                                                                               ON 'Order Details'.OrderID=Orders.OrderID
                                                                        ) ON Customers.CustomerID = Orders.CustomerID 
                                                            GROUP BY ContactName
                                                            HAVING COUNT(DISTINCT Orders.OrderID) > 10 AND AVG(UnitPrice) < 17";
                ExecuteQuery(connection, paramCustomers);
                
                Console.WriteLine("\n13.Select all customers with phone that has format ('NNNN-NNNN') ");
                string phoneFormat = @"SELECT ContactName FROM Customers WHERE Phone GLOB '????-????' ";
                ExecuteQuery(connection, phoneFormat);
                
                Console.WriteLine("\n14.Select customer that ordered the greatest amount of goods (not price) ");
                string bestCustomer = @"SELECT ContactName FROM Customers WHERE CustomerID =
                                        (
	                                        SELECT CustomerID From
	                                        (
		                                        SELECT CustomerID, MAX(num) FROM 
		                                        (
			                                        SELECT CustomerID, COUNT(Orders.CustomerID) AS num FROM Orders 	
			                                        GROUP BY Orders.CustomerID
		                                        )
	                                        )
                                        ) ";
                ExecuteQuery(connection, bestCustomer);
                
                //немає customers які замовляли абсолютно такі ж продукти. Є ті, що замовляли ті ж, але ще додатково інші.
                //Щоб знайти їх, необхідно забрати частину AND NIT EXISTS
                Console.WriteLine("\n15.Select only these customers that ordered the absolutely the same products as customer 'FAMIA' ");
                string sameAsFamia = @"SELECT CustomerID FROM Customers C
                                                   WHERE NOT EXISTS(
						                               SELECT DISTINCT ProductID FROM 'Order Details'
						                               WHERE OrderID IN (
							                                SELECT OrderID FROM Orders WHERE CustomerID = C.CustomerID
							                            )
							                            AND ProductID NOT IN(
								                            SELECT DISTINCT ProductID FROM 'Order Details' OD
							                                WHERE OrderID IN (
										                            SELECT OrderID FROM Orders WHERE CustomerID = 'FAMIA'
							                                )
						                                )
                                                    )
                                                    AND NOT EXISTS(
                                                        SELECT DISTINCT ProductID FROM 'Order Details' OD
							                                WHERE OrderID IN (
										                            SELECT OrderID FROM Orders WHERE CustomerID = 'FAMIA'
							                                )
							                            AND ProductID NOT IN(
                                                            SELECT DISTINCT ProductID FROM 'Order Details'
						                                    WHERE OrderID IN (
							                                    SELECT OrderID FROM Orders WHERE CustomerID = C.CustomerID
							                                )								                            
                                                        )
                                                    )
				                            AND CustomerID <> 'FAMIA'";
                ExecuteQuery(connection, sameAsFamia);
            }
                
            Console.WriteLine("\nSUCCESS!\nEnd.");
            Console.ReadLine();
        }

        public static void ExecuteQuery(SqliteConnection connection, string query)
        {
            using (SqliteCommand cmd = new SqliteCommand(query, connection))
            {
                cmd.ExecuteNonQuery();

                var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    ReadSingleRow((IDataRecord)reader);
                }
                reader.Dispose();
            }
        }

        private static void ReadSingleRow(IDataRecord record)
        {
            for (var i = 0; i < record.FieldCount; ++i) 
                Console.Write(String.Format(" {0} ", record[i]));
            Console.Write("\n");
        }
    }
}


