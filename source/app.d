import vibe.d;
import std.stdio;
import std.conv;
import std.algorithm;
import std.datetime;
import std.path;
import std.file;

import parseconfig;
import dbconnect;
import users;

import requests.http;

//DateTime currentdt;
//static this()
//{
//    DateTime currentdt = cast(DateTime)(Clock.currTime()); // It's better to declarate globally
//    string datestamp = currentdt.toISOExtString;
//}

string roothtml;
static this()
{
    
    roothtml = buildPath(getcwd, "html") ~ "\\";
    if(!roothtml.exists)
       writeln("[ERROR] HTML dir do not exists");     
}

Config config;

void main()
{
    config = new Config();
    DBConnect dbconnect = new DBConnect(config);
    if (dbconnect.checkServerStatus())
    {
        writeln("Server alive!");
    }

    else
    {
        writeln("Could not connect to server");
        return;
    }

    foo();

    auto router = new URLRouter;
    router.get("/*", serveStaticFiles(roothtml ~ "\\"));    
    router.get("*", serveStaticFiles(roothtml ~ "static\\"));
    
    router.any("*", &accControl);

    router.any("/checkAuthorization", &checkAuthorization);
    router.any("/login", &login);
    router.post("/logout", &logout);

    router.any("/test", &test);    

    bool isAuthorizated = false;
    bool isAdmin = false;

    auto settings = new HTTPServerSettings;
    settings.port = 8080;
    settings.bindAddresses = ["::", "127.0.0.1"];
    settings.sessionStore = new MemorySessionStore; // SESSION


    writeln(config.allow_voting_for_unauthorized);
    writeln("\nHOST: ", config.dbhost);

    writeln("--------sending data---------");

    listenHTTP(settings, router);
    runEventLoop();
}

void accControl(HTTPServerRequest req, HTTPServerResponse res)
{
    res.headers["Access-Control-Allow-Origin"] = "*";
}

AuthInfo _auth;

void foo()
{
   // List of documents in collection
    string baseURL = "http://localhost:8529";
    string url = "http://localhost:8529/_db/testdb/_api/document/?collection=users"; 
    import std.experimental.logger;
    globalLogLevel(LogLevel.error);

    Json mycollection = Json.emptyObject;
  
     auto rq = Request();
     auto rs = rq.get(url);
     //writeln(rs.responseBody.data!string);
     mycollection = parseJsonString(rs.responseBody.data!string);
     if (rs.code != 200)
        writeln("Can't get list of documents in collection");

     foreach(Json d;mycollection["documents"])
     {
       // writeln(baseURL ~ d);
        auto rs = rq.get(baseURL ~ to!string(d).replace(`"`,``));
        //writeln(rs.responseBody.data!string);
     }


    /*
{
    "login": "dima",
    "password": "123",
    "type": "user",
    "firstname": "",
    "lastname": "",
    "organization": "",
    "lastvisit": "",
    "tests" : 
        {
            "allowed": [],
            "passed": []
        }
}
    */
}



void checkAuthorization(HTTPServerRequest req, HTTPServerResponse res)
{
    logInfo("-----checkAuthorization START-----");
    Json responseStatus = Json.emptyObject;
    Json responseBody = Json.emptyObject;  //should be _in_ responseStatus
    //if user already on site
    if (req.session)
    {
        logInfo("user already have active session");
        if(_auth.isAuthorized) //only user authorizate
        {
            responseStatus["status"] = "success";
            responseBody["isAuthorized"] = true;
            responseBody["isAdmin"] = false;
            responseBody["username"] = _auth.user.username;
            responseStatus["login"] = responseBody;
            if(_auth.isAdmin)
            {
                responseBody["isAdmin"] = true;
            }

            res.writeJsonBody(responseStatus);
            logInfo(responseStatus.toString); // include responseBody
        }
    //example: {"login":{"isAuthorized":true,"isAdmin":false,"username":"test"},"status":"success"}

    }
    // Login info we should check only with /login here
    else
    {
        responseStatus["status"] = "fail"; // unauthorized user
        res.writeJsonBody(responseStatus);
    }
    logInfo("-----checkAuthorization END-------");


}


void test(HTTPServerRequest req, HTTPServerResponse res)
{
    if (req.session)
        res.writeBody("Hello, World!", "text/plain");
}


void login(HTTPServerRequest req, HTTPServerResponse res)
{
    Json request = req.json;
    //writeln(to!string(request["username"]));
    writeln("-------------JSON OBJECT from site:-------------");
    writeln(request);
    writeln("^-----------------------------------------------^");
    //readln;

    try
    {      
        string dbpassword;
        string dbuser;

        // response. responseBody should be nested in "success" or "fail" block. Like {"fail": {...}}
        Json responseStatus = Json.emptyObject;
        Json responseBody = Json.emptyObject;  //should be _in_
       

            if (dbuser == request["username"].to!string && dbpassword != request["password"].to!string)
            {
                ////////USER OK PASSWORD WRONG///////////
                    responseStatus["status"] = "fail"; // user exists in DB, password NO
                    responseBody["password"] = "wrongPassword"; // user exists in DB, password NO
                    responseBody["isAuthorized"] = false;
                    responseStatus["login"] = responseBody;
                    logInfo("-------------------------------------------------------------------------------");
                    logInfo(responseStatus.toString); // include responseBody
                    logInfo("^-----------------------------------------------------------------------------^");                              
                    logWarn("WRONG password for USER: %s", request["username"]); //getting username from request
                //output: {"login":{"isAuthorized":false,"password":"wrongPassword"},"status":"fail"}
            }


            if (dbuser == request["username"].to!string && dbpassword == request["password"].to!string)
            {
                ////////ALL RIGHT///////////
                 logInfo("DB-User: %s | DB-Password: %s", dbuser, dbpassword);
                 
                 if (!req.session) //if no session start one
                    {
                        req.session = res.startSession();
                    }    

                /* we should set this fields: 
                    _auth.isAdmin 
                    _auth.user.username 
                   to get /checkAuthorization work! */
                _auth.isAuthorized = true; 
                if(dbuser == "admin") // admin name hardcoded
                {
                try
                  {
                       _auth.isAdmin = true; 
                       _auth.user.username = "admin"; 
                       //req.session.set("username", "admin"); //ditto
                       req.session.set!string("username", "admin");

                       responseStatus["status"] = "success";
                       responseBody["isAuthorized"] = true;
                       responseBody["isAdmin"] = true;
                       responseBody["username"] = dbuser; // admin!
                       responseStatus["login"] = responseBody;

                       logInfo("-------------------------------------------------------------------------------");
                       logInfo(responseStatus.toString); // include responseBody
                       logInfo("^-----------------------------------------------------------------------------^");
                       logInfo("Admin session for user: %s started", dbuser);
                       // {"login":{"isAuthorized":true,"isAdmin":true,"username":"admin"},"status":"success"}
                    }

                    catch(Exception e)
                    {
                        writeln("Error during admin login:");
                        writeln(e.msg);
                    }
                }
                if(dbuser != "admin") // start user session
                {
                    try
                    {
                       req.session.set("username", dbuser); //set current username in parameter of session name
                        _auth.user.username = dbuser; //set field

                       responseStatus["status"] = "success";
                       responseBody["isAuthorized"] = true;
                       responseBody["isAdmin"] = false;
                       responseBody["username"] = dbuser; // user!
                       responseStatus["login"] = responseBody;

                       logInfo("-------------------------------------------------------------------------------");
                       logInfo(responseStatus.toString); // include responseBody
                       logInfo("^-----------------------------------------------------------------------------^");
                       logInfo("User session for user: %s started", dbuser);
                   // {"login":{"isAuthorized":true,"isAdmin":false,"username":"test"},"status":"success"}
                    }

                    catch (Exception e)
                    {
                        writeln("Error during user login:");
                        writeln(e.msg);
                    }
                }
 
            }

          
       

        else // userDoNotExists
        {
            logInfo("User: %s do not exists in DB", dbuser);
            responseStatus["status"] = "fail"; // user exists in DB, password NO
            responseBody["username"] = "userDoNotExists"; // user exists in DB, password NO
            responseBody["isAuthorized"] = false;
            responseStatus["login"] = responseBody;
            logInfo("-------------------------------------------------------------------------------");
            logInfo(responseStatus.toString); // include responseBody
            logInfo("^-----------------------------------------------------------------------------^");                              
            logWarn("User %s DO NOT exist in DB", request["username"]); //getting username from request

        }

        res.writeJsonBody(responseStatus); //Final answer to server. Must be at the end

    }

    catch(Exception e)
    {
        writeln("Can't process select from DB otest.myusers");
        writeln(e.msg);
    }



}


void logout(HTTPServerRequest req, HTTPServerResponse res)
{
    try
    {
        logInfo("Logout section");
        Json request = req.json;
        Json responseBody = Json.emptyObject; // function duplicate from login

        if (req.session) // if user have active session
        {
            res.terminateSession();
            responseBody["status"] = "success";
            responseBody["isAuthorized"] = false;
            res.writeJsonBody(responseBody);
            logInfo("-------------------------------------------------------------------------------");
            logInfo(responseBody.toString);
            logInfo("^-----------------------------------------------------------------------------^");                              
            logInfo("User %s logout", request["username"]); //
        }

        else
        {
            responseBody["status"] = "fail"; // user do not have active session?
            logInfo("User do not have active session"); 
            res.writeJsonBody(responseBody);
        }
    res.writeJsonBody(responseBody);
    }

    catch (Exception e)
    {
        logInfo(e.msg);
    }
}
