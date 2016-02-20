module dbconnect;

import std.stdio;
import std.file;
import std.string;
import std.array;
import std.conv;
import std.regex;
import std.path;
import std.range;
import std.format;
import std.algorithm;
import std.datetime;
import std.parallelism;
import std.zip;

import ddbc.all;
import parseconfig;


class DBConnect
{
	Statement stmt;
	ParseConfig parseconfig;

	this(ParseConfig parseconfig)
	{
		try
			{
			    this.parseconfig = parseconfig;
			    MySQLDriver driver = new MySQLDriver();
			    string url = MySQLDriver.generateUrl(parseconfig.dbhost, to!short(parseconfig.dbport), parseconfig.dbname);
			    string[string] params = MySQLDriver.setUserAndPassword(parseconfig.dbuser, parseconfig.dbpass);

				DataSource ds = new ConnectionPoolDataSourceImpl(driver, url, params);

				auto conn = ds.getConnection();
				scope(exit) conn.close();

				stmt = conn.createStatement();
				writefln("\n[Database connection OK]");

				// Before start we should check if DB is exists
				// No guarantee that it's have proper structure of tables!
				try
				{
					// SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'test'
					string dbname;
					auto dbExistsCheck = stmt.executeQuery("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '" ~ parseconfig.dbname ~ "'");
					while(dbExistsCheck.next())
					{
						dbname = dbExistsCheck.getString(1);
						if (dbname == parseconfig.dbname)
							writefln("Database with name (from config file): \"%s\" exist in MySQL", parseconfig.dbname);
						else
							writefln("[ERROR] Database with name (from config file): %s DO NOT exist in MySQL", parseconfig.dbname);
					}

				}

				catch (Exception ex)
				{
					writefln(ex.msg);
					writefln("Database: %s do not exists. Please check settings", parseconfig.dbname);
				}
			}
		catch (Exception ex)
		{
			writefln(ex.msg);
			writeln("Could not connect to DB. Please check settings");
			readln;
			return;
		}

	}	

	

}