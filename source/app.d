import vibe.d;
import std.stdio;
import std.conv;
import std.algorithm;
import std.datetime;
import std.path;
import std.file;
import std.experimental.logger;


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

string baseURL = "http://localhost:8529";
string collectionVisitorsUrl = "http://localhost:8529/_db/otest/_api/document/?collection=visitors"; // info about passed test for everyone who press
string cursorURL = "http://localhost:8529/_db/otest/_api/cursor";

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

    getUsersFromDB();

    auto router = new URLRouter;
    router.get("/*", serveStaticFiles(roothtml ~ "\\"));    
    router.get("*", serveStaticFiles(roothtml ~ "static\\"));
    
    router.any("*", &accControl);

    router.any("/checkAuthorization", &checkAuthorization);
    router.any("/login", &login);
    router.post("/logout", &logout);

    router.any("/test", &test);    

    router.any("/questions", &questions);

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

Tuple!(string, "login", string, "password") [] usersInDB;

void getUsersFromDB()
{
   
try
{
   // List of documents in collection
    string url = "http://localhost:8529/_db/otest/_api/document/?collection=users"; 
    import std.experimental.logger;
    globalLogLevel(LogLevel.error);


    Json mycollection = Json.emptyObject;
  
     auto rq = Request();
     auto rs = rq.get(url);
     writeln(rs.responseBody.data!string);
     mycollection = parseJsonString(rs.responseBody.data!string);
     if (rs.code != 200)
        writeln("Can't get list of documents in collection");
     if (rs.code == 404)
        writeln("URL do not exists: ", url);

     string [] listOfDocumentsInUsers;   
   
     foreach(Json d;mycollection["documents"])
     {
        listOfDocumentsInUsers ~= baseURL ~ to!string(d).replace(`"`,``);

     }


     Tuple!(string, "login", string, "password") user;

     Json userJson = Json.emptyObject;
     auto rq1 = Request();
     foreach(link; listOfDocumentsInUsers)
     {
        auto rs1 = rq1.get(link);
        //userJson = parseJsonString(rs1.responseBody.data!string);
        userJson = parseJsonString(rs1.responseBody.data!string);
        user.login = to!string(userJson["login"]).replace(`"`,"");
        user.password = to!string(userJson["password"]).replace(`"`,"");
        usersInDB ~= user;
     }
}

    catch (Exception e)
    {
        writeln(e.msg);
        writeln("Can't connect to ArangoDB");
    }

/*
{
 USERS table: 
{
    "login": "admin",
    "password": "123",
    "type": "user",
    "firstname": "",
    "lastname": "",
    "organization": "",
    "lastvisit": "",
    "ip": "",
    "tests" : 
        {
            "allowed": [],
            "passed": []
        }
}


//same as string:
//    db.users.save({"login":"admin","password":"123","type":"user","firstname":"","lastname":"","organization":"","lastvisit":"","ip":"","tests":{"allowed":[],"passed":[]}})

//-----------------

VISITORS table:
db.visitors.save({"ip": "", "date": "", "cookie": "", "referal": "", "location": "", "language" : "", "browser" : "", "passedtests" : []})


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

            string query = `{"query" : "FOR v in visitors FILTER v.ip == '` ~ req.peer ~ `' return {guid: v.guid, ip: v.ip, passedtests: v.passedtests}"}`;
            auto rq = Request();
            auto rs = rq.post(cursorURL, query , "application/json"); // тут у нас не [] а {} поэтому можно без key
            
            Json visitorsInfo = Json.emptyObject; // JSON array of data for current IP: {guid: v.guid, ip: v.ip, passedtests: v.passedtests}
            visitorsInfo = parseJsonString(rs.responseBody.data!string);

            // filling passed tests for this IP
            //incoming JSON is:
            // {"hasMore":false,"result":[{"ip":"127.0.0.1","passedtests":"[firsttest8,firsttest8,firsttest8,firsttest8]","guid":""}],"code":201,"extra":{"stats":{"writesIgnored":0,"scannedIndex":0,"scannedFull":1,"executionTime":0,"filtered":0,"writesExecuted":0},"warnings":[]},"error":false,"cached":false}
            // result is array, so [0] it's first element
            Json passedtestsJson = visitorsInfo["result"][0]["passedtests"]; // "[firsttest1,firsttest2]"
            responseStatus["passedtests"] = passedtestsJson.get!(string); // [firsttest1,firsttest2] // passedtests that we will send to client

            res.writeJsonBody(responseStatus);
            logInfo(responseStatus.toString); // include responseBody
        }
    //example: {"login":{"isAuthorized":true,"isAdmin":false,"username":"test"},"status":"success"}

    }
    // Login info we should check only with /login
    else
    {
        // checkAuthorization запрашивается при каждом обращении к сайту
        // для неавторизованных пользователей нужно проверять в коллекции visitors какие тесты были пройдены
        // при старте теста когда коллеция пустая вернется пустой result поэтому нужно ниже проверять не пуст ли он прежде чем брать элементы
        string query = `{"query" : "FOR v in visitors FILTER v.ip == '` ~ req.peer ~ `' return {guid: v.guid, ip: v.ip, passedtests: v.passedtests}"}`;
        auto rq = Request();
        auto rs = rq.post(cursorURL, query , "application/json"); // тут у нас не [] а {} поэтому можно без key

        Json visitorsInfo = Json.emptyObject; // JSON array of data for current IP: {guid: v.guid, ip: v.ip, passedtests: v.passedtests}
        visitorsInfo = parseJsonString(rs.responseBody.data!string);
  
        // filling passed tests for this IP
        //incoming JSON is:
        // {"hasMore":false,"result":[{"ip":"127.0.0.1","passedtests":"[firsttest8,firsttest8,firsttest8,firsttest8]","guid":""}],"code":201,"extra":{"stats":{"writesIgnored":0,"scannedIndex":0,"scannedFull":1,"executionTime":0,"filtered":0,"writesExecuted":0},"warnings":[]},"error":false,"cached":false}
        // result is array, so [0] it's first element
        
        // FIXME Падает при пустой коллекци visitors

        //if(visitorsInfo["result"].empty && visitorsInfo["result"] == `[]`)
        //{
        //    writeln("0000000000");

        //}
        //FIXME смотри выше

        //writeln(visitorsInfo["result"][0]["passedtests"]);
        writeln(visitorsInfo["result"]);
        if (visitorsInfo["result"].toString != `[]`) // FIXME нужно как-то покрасивее это сделать
        {
            Json passedtestsJson = visitorsInfo["result"][0]["passedtests"]; // "[firsttest1,firsttest2]"
            responseStatus["passedtests"] = passedtestsJson.get!(string); // [firsttest1,firsttest2] // passedtests that we will send to client    
        }
        else
        {
            responseStatus["passedtests"] = `[]`;
            //FIXME при первом запуске если коллекция visitors пустая в консоли вылетает ошибка 500 потом дальше норм.
        }


        responseStatus["status"] = "fail"; // unauthorized user
        writeln(responseStatus);
        res.writeJsonBody(responseStatus);

        // {"passedtests":"[firsttest8,firsttest8,firsttest8,firsttest8,firsttest9]","status":"fail"}

    }
    logInfo("-----checkAuthorization END-------");


}


void test(HTTPServerRequest req, HTTPServerResponse res)
{
    if (req.session)
        res.writeBody("Hello, World!", "text/plain");
}



void questions(HTTPServerRequest req, HTTPServerResponse res)
{
    Json questions = req.json;
    res.writeBody("Hello, World!", "text/plain");  // FIXME

    runTask(toDelegate(&sendVisitorInformationToArangoDB), req, res); // для каждого кто нажал кнопку отправить сохраняем всю инфу, чтобы потом не показывать пройденный тест
    runTask(toDelegate(&sendQuestionsToArangoDB), questions); // сами ответы пользователей

}


void sendVisitorInformationToArangoDB(HTTPServerRequest req, HTTPServerResponse res)  // только для тех кто прошел тест по идее. Нужно убедиться что только их инфа будет храниться
{
    writeln("sendVisitorInformationToArangoDB function");
    // {"ip": "", "date": "", "cookie": "", "referal": "", "location": "", "language" : "", "browser" : "", "passedtests" : []}
    // req.peer - IP as string

    // Перед тем добавлять данные можно попробовать в начале получить список ИП, чтобы потом для данного ИП обновить тесты которые он прошел
    // Можно без этого, но чисто теоретически статистика будет чуть удобнее
    
    string collectionUrl = "http://localhost:8529/_db/otest/_api/document/?collection=visitors"; // info about passed test for everyone who press
    
    import std.uuid; 

    struct VisitorData
    {
        string guid;
        string ip;
        string date;
        string cookie;
        string referal;
        string location;
        string language;
        string browser;
        string [] passedtests; // В БД хранится массив тестов для каждого ИП адреса. 
    }

    VisitorData visitordata;


   // visitordata.guid = to!string(randomUUID()); 
    visitordata.ip = req.peer;

    // Из прилетевшего Question нужно взять еще взять имя пройденного теста и заполнить VisitorData
       Json request = req.json;

       string passedtestFromCurrentQuestion; // пройденный тест из текущего JSON. В БД у нас массив тестов

       foreach(Json x; request)
       {
         passedtestFromCurrentQuestion = to!string(x["testname"]).replace(`"`,``);  // из запроса получаем имя теста из пришедшего Question. Потом имя этого теста нужно отправить в Визиторс
       }

        string query = `{"query" : "FOR v in visitors return {guid: v.guid, ip: v.ip, passedtests: v.passedtests}"}`;  // делаем выборку гуидов и IP из коллекции
        string aqlUrl = "http://localhost:8529/_db/otest/_api/cursor";
        auto rq2 = Request();
        auto rs2 = rq2.post(aqlUrl, query , "application/json"); // тут у нас не [] а {} поэтому можно без key
        
        //writeln(rs2.responseBody.data!string);
        //writeln("^^^^^^^^^^");

        Json visitorsInfo = Json.emptyObject;
        visitorsInfo = parseJsonString(rs2.responseBody.data!string);
        //writeln(visitorsInfo["result"]);
        writeln(visitorsInfo);
        

        // foreach не будет работать если это первое обращение с данного ИП и в БД по нему пусто
        if(visitorsInfo["result"].toString == "[]")
        {
            writeln(visitorsInfo["result"]);

            visitordata.passedtests ~= passedtestFromCurrentQuestion;
            writeln(visitordata.passedtests);

            Json myJson = visitordata.serializeToJson();

            string qy = myJson.toString;

            import std.net.curl;
            auto content = post("http://localhost:8529/_db/otest/_api/document?collection=visitors", qy);
            return; // чтобы не делать foreach выходим

        }

        foreach(Json v;visitorsInfo["result"])
        {
           if (to!string(v["ip"]).canFind(visitordata.ip)) // среди тестов уже есть данный ИП то нам нужно получить его гуид и обновить для него список пройденных тестов
           {
                writeln(to!string(v["passedtests"]));
                //writeln(v["guid"]);
                //writeln("passedtestFromCurrentQuestion: ", passedtestFromCurrentQuestion);
                //writeln("______________________________________________");

                // тут патчим пройденные тесты! к текущему значению passedtests прибавляем значение passedtestFromCurrentQuestion полученное выше
                // FIXME to!string тут возможно не идеальное, но рабочее решение, иначе массив JSON не получается перебрать
                // replace заменит ПОЛНОСТЬЮ документ поэтому нам нужен UPDATE
                if( canFind(to!string(v["passedtests"]), passedtestFromCurrentQuestion || v["passedtests"] == "")) // если тест пуст то нужно чтобы else сработал
                {
                    // если данный тест уже значится как пройденный
                    writeln("test already passed");
                    //readln;
                }


                else
                {
                    // Создаем массив тестов с пройденными тестами из БД для данного пользователя
                    // FIXME Почему то по нормальному не прибавляет, поэтому HACK
                   // visitordata.passedtests ~= (v["passedtests"]).toString ~ `,` ~ passedtestFromCurrentQuestion;
                        visitordata.passedtests ~= (v["passedtests"]).toString.replace(`[\"`,``).replace(`\"]`,``);
                        if(!(v["passedtests"]).toString.canFind(passedtestFromCurrentQuestion))
                        {
                            visitordata.passedtests ~= passedtestFromCurrentQuestion;    
                        }
                        
                   string result_test;

                   
                   if(v["passedtests"] == Json.emptyArray) // []
                   {
                        result_test = `[` ~ passedtestFromCurrentQuestion ~`]`;
        
                   }

                   if(v["passedtests"] != Json.emptyArray)
                   {
                       result_test = to!string(v["passedtests"]).replace(`]`, `,` ~ passedtestFromCurrentQuestion ~ `]`);
                   }

                    result_test = result_test.replace(`"`,``);

                   
                   writeln(result_test);
                   writeln("=====================");
                   //readln;

                  
                    // Записываем в коллекцию тест который был пройден с данного IP чтобы больше его не показывать
                   // writeln("--> ", result_test);//replace(`"`,``));
                   // auto x = visitordata.passedtests.map!(x => ""x.writeln);

                    string queryUpdate = `{"query" : "FOR v in visitors FILTER v.ip == '` ~ visitordata.ip ~ `' UPDATE v WITH {passedtests: '` ~ result_test ~ `'} IN visitors"}`; 
                    writeln(queryUpdate);
                   // readln;

                    import std.net.curl;
                    auto content = post(aqlUrl, queryUpdate);
                    writeln("check DB");
                    //readln;
                    
                    // очищаем строку
                    result_test = [];

                   res.writeJsonBody(Json.emptyArray);
                    writeln("DONE!!!!!!!!!!");

                }
           }
        }


    writeln("VISITORS INFO SENDED!");

}



void sendQuestionsToArangoDB(Json questions) // само тело вопроса
{
    
    string collectionUrl = "http://localhost:8529/_db/otest/_api/document/?collection=sitetestanswers"; // вот сюда переслать запрос надо

    auto rq = Request();
    rq.verbosity = 2;
    auto rs = rq.post(collectionUrl, `{"question":` ~ to!string(questions) ~ `}`, "application/json"); // просто массив пулять нелья
    writeln("Question JSON was send to DB");

    writeln(questions);

}



void onlinetestdata(HTTPServerRequest req, HTTPServerResponse res)
{
    if (req.session)
        res.writeBody("request passed!", "text/plain");
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

        // response. responseBody should be nested in "success" or "fail" block. Like {"fail": {...}}
        Json responseStatus = Json.emptyObject;
        Json responseBody = Json.emptyObject;  //should be _in_
       

        foreach(dbuser; usersInDB)
        {

            if (dbuser.login == request["username"].to!string && dbuser.password != request["password"].to!string)
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


            if (dbuser.login == request["username"].to!string && dbuser.password == request["password"].to!string)
            {
                ////////ALL RIGHT///////////
                 logInfo("DB-User: %s | DB-Password: %s", dbuser.login, dbuser.password);
                 
                 if (!req.session) //if no session start one
                    {
                        req.session = res.startSession();
                    }    

                /* we should set this fields: 
                    _auth.isAdmin 
                    _auth.user.username 
                   to get /checkAuthorization work! */
                _auth.isAuthorized = true; 
                if(dbuser.login == "admin") // admin name hardcoded
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
                       responseBody["username"] = dbuser.login; // admin!
                       responseStatus["login"] = responseBody;

                       logInfo("-------------------------------------------------------------------------------");
                       logInfo(responseStatus.toString); // include responseBody
                       logInfo("^-----------------------------------------------------------------------------^");
                       logInfo("Admin session for user: %s started", dbuser.login);
                       // {"login":{"isAuthorized":true,"isAdmin":true,"username":"admin"},"status":"success"}
                    }

                    catch(Exception e)
                    {
                        writeln("Error during admin login:");
                        writeln(e.msg);
                    }
                }
                if(dbuser.login != "admin") // start user session
                {
                    try
                    {
                       req.session.set("username", dbuser.login); //set current username in parameter of session name
                        _auth.user.username = dbuser.login; //set field

                       responseStatus["status"] = "success";
                       responseBody["isAuthorized"] = true;
                       responseBody["isAdmin"] = false;
                       responseBody["username"] = dbuser.login; // user!
                       responseStatus["login"] = responseBody;

                       logInfo("-------------------------------------------------------------------------------");
                       logInfo(responseStatus.toString); // include responseBody
                       logInfo("^-----------------------------------------------------------------------------^");
                       logInfo("User session for user: %s started", dbuser.login);
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
                logInfo("User: %s do not exists in DB", dbuser.login);
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
