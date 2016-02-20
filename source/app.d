import vibe.d;
import std.stdio;
import std.conv;
import std.algorithm;
import std.datetime;
import std.path;
import std.file;

import ddbc.all;
import parseconfig;
import dbconnect;
import users;

DBConnect db;

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

void main()
{
  
    auto router = new URLRouter;
    router.get("/*", serveStaticFiles(roothtml ~ "\\"));    
    router.get("*", serveStaticFiles(roothtml ~ "static\\"));
    router.get("/admin/*", &adminpage);
    
    router.any("*", &accControl);
    router.any("/my", &action);
    router.any("/stat", &statistic);

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

    ParseConfig parseconfig = new ParseConfig();
    writeln("\nHOST: ", parseconfig.dbhost);
    db = new DBConnect(parseconfig);
    getNumberOfQID(); // questionID

    writeln("--------sending data---------");

    listenHTTP(settings, router);
    runEventLoop();
}

void accControl(HTTPServerRequest req, HTTPServerResponse res)
{
    res.headers["Access-Control-Allow-Origin"] = "*";
}

AuthInfo _auth;


void adminpage(HTTPServerRequest req, HTTPServerResponse res)
{
    if (req.session)
    {
        serveStaticFile(roothtml ~ "admin\\stat.html")(req,res);
    }
    else
    {
        res.writeBody("Access Deny", "text/plain");
    }

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


void action(HTTPServerRequest req, HTTPServerResponse res)
{
    // data-stamp for every request
    DateTime currentdt = cast(DateTime)(Clock.currTime()); // It's better to declarate globally
    string datestamp = currentdt.toISOExtString;
   
    // how get string from POST request here. And how get JSON object, if server send it.
    //Json my = req.json;
    Json results;
    try
    {
        results = req.json;
    }
    catch (Exception e)
    {
        writeln("Can't parse incoming JSON string");
        writeln(e.msg);
    }
    //writeln(result["QID"]);
    writeln(results);
    //writeln(results["MaxArea"]);
    //writeln(to!string(results).lastIndexOf("MaxArea"));

    // looking MinArea and MaxArea section
    string MinArea;
    string MaxArea;
    foreach(section;results)
    {
        if(to!string(section).canFind("MinArea"))
        {
            writeln("MinArea: ", section["MinArea"].to!string);
            MinArea = section["MinArea"].to!string;
        }

        if(to!string(section).canFind("MaxArea"))
        {
            writeln("MaxArea: ", section["MaxArea"].to!string);
            MaxArea = section["MaxArea"].to!string;
        }
    }

    try
    {
        //Area Data would have QID = 100
        string result_ = ("INSERT INTO otest.mytest (`MyDate`, `QID`, `MinArea`, `MaxArea`) VALUES (" ~"'" ~ datestamp ~ "', " ~ "100" ~ "," ~ MinArea ~ "," ~ MaxArea ~");");
        db.stmt.executeUpdate(result_);
    }

    catch(Exception e) 
    {
        writeln("Can't insert MinArea and MaxArea");
        writeln(e.msg);
    }   

    foreach (result; results)
    {
        //writeln(_key);
        foreach(_k; result)
        {
            // _rk -- проверяем строку, но потом если в этой строке есть вхождение,
            // то смотрим уже _k так как это JSON

            string _rk = to!string(_k);
            if (_rk.canFind("QID"))
            {
                try
                {
                    string result = ("INSERT INTO otest.mytest (`MyDate`, `QID`, `AID`) VALUES (" ~"'" ~ datestamp ~ "', " ~ to!string(_k["QID"]) ~ "," ~ to!string(_k["AID"]) ~ ");");
                    db.stmt.executeUpdate(result);     
                }

                catch (Exception e)
                {
                    writeln("Can't insert in DB", e.msg);
                }
            }


        }
    }

}

//we need to get total number of QID that storage in DB
int [] getNumberOfQID()
{
    // чтобы минимальная и максимальная площадь вставлялась в БД один раз мы ей идентификатор 100 присвоили, и выборку по нему
    // лучше сделать потом 
    auto rs = db.stmt.executeQuery("SELECT DISTINCT QID FROM otest.mytest WHERE QID != 100");
    int [] result;
    while (rs.next())
    {
        result ~= to!int(rs.getString(1));
    }
    //writeln("==> ", result);
    return result;
}

void statistic(HTTPServerRequest req, HTTPServerResponse res)
{
   string result_json;

    foreach(i, QID; getNumberOfQID) // now we need iterate all QID
    {

        i++;
        string query_string = "SELECT AID FROM otest.mytest WHERE QID=" ~ to!string(QID);
        auto rs = db.stmt.executeQuery(query_string);
        int [] result;
        while (rs.next())
        {
            result ~= to!int(rs.getString(1));
            //writeln(result);
        }

        string single_QID = "{" ~ `"` ~ to!string(QID) ~ `":` ~ to!string(result) ~ "}";
       // writeln(single_QID);
        result_json ~= single_QID ~ ",";
        //writeln(result_json);
        //writeln;
        
        string result_json1;
        result_json1 ~= ("[" ~ result_json ~ "]").replace("},]","}]");

        // _Very_ dirty hack to send JSON array of QID and their result at _last_ iteration! 
        if((i == getNumberOfQID.length - 1))
        {
            writeln(result_json1);
            res.writeBody(to!string(result_json1));
        }
        
    } 

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
        string query_string = (`SELECT user, password FROM otest.myusers where user = ` ~ `'` ~ request["username"].to!string ~ `';`);
        auto rs = db.stmt.executeQuery(query_string);

        string dbpassword;
        string dbuser;

        // response. responseBody should be nested in "success" or "fail" block. Like {"fail": {...}}
        Json responseStatus = Json.emptyObject;
        Json responseBody = Json.emptyObject;  //should be _in_
       
        //writeln("rs.next() --> ", rs.next());
        /*
        if(!rs.next()) // user do not exists in DB
        {
            responseBody["status"] = "userDoNotExists"; // user exists in DB, password NO
            responseBody["isAuthorized"] = false;
            logInfo("-------------------------------------------------------------------------------");
            logInfo(responseBody.toString);
            logInfo("-------------------------------------------------------------------------------");                              
            logWarn("User: %s DO NOT exists in DB!", request["username"]); //getting username from request    
        }   
        writeln(query_string);      
        */
        // ISSUE: return false if DB have ONE element with same name!
        if (rs.next()) //work only if user exists in DB
        {
            writeln("we are here");
            dbuser = rs.getString(1);
            dbpassword = rs.getString(2);    
            

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
