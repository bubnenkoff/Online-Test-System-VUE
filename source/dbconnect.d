module dbconnect;

import std.stdio;
import std.file;
import std.string;
import std.array;
import std.conv;
import std.path;
import std.range;
import std.format;
import std.algorithm;
import std.datetime;
import requests;

import parseconfig;


class DBConnect
{
	Config config;

	this(Config config)
	{
		this.config = config;
	}	

		bool checkServerStatus()
		{
		    
		    // https://docs.arangodb.com/HttpAdministrationAndMonitoring/index.html
		    try
		    {
		       string url = "http://" ~ config.dbhost ~ ":" ~ config.dbport ~ "/_admin/log";
		       //string url = "http://ya.ru";
		       import std.experimental.logger;
		       globalLogLevel(LogLevel.info);
		       auto rq = Request();
		       auto rs = rq.exec!"HEAD"(url);
		       if (rs.code == 200)
		           return true;
		    }

		    catch (Exception e)
		    {
		        writeln(e.msg);
		    }
 			return false;
		}

}