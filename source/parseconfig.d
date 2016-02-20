module parseconfig;

import std.file;
import std.string;
import std.datetime;
import std.path;
import std.stdio;

import variantconfig;
import dbconnect;

class ParseConfig
{
	string dbname;
	string dbuser;
	string dbpass;
	string dbhost;
	string dbport;
	string HTMLroot;


this()
	{
		try
		{
			//getcwd do not return correct path if run from task shoulder
			string configPath = buildPath((thisExePath[0..((thisExePath.lastIndexOf("\\"))+1)]), "config.ini");
			//writefln(thisExePath[0..((thisExePath.lastIndexOf("\\"))+1)]); // get path without extention +1 is for getting last slash

			//string configPath = buildPath(thisExePath, "config.ini");
			if (!exists(configPath)) 
				{
					writeln("ERROR: config.ini do not exists");
				}
			auto config = VariantConfig(configPath);
			try
			{
				dbname = config["dbname"].toStr;
				dbuser = config["dbuser"].toStr;
				dbpass = config["dbpass"].toStr;
				dbhost = config["dbhost"].toStr;
				dbport = config["dbport"].toStr;
				HTMLroot = config["HTMLroot"].toStr;
			}
			catch (Exception msg)
			{
				writefln("ERROR: Can't parse config: %s");
			}		
		}
		catch(Exception msg)
		{
			writeln(msg.msg);
			core.thread.Thread.sleep( dur!("msecs")(1000));
		}	
	}


}