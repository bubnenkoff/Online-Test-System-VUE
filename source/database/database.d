/*
 Copyright: Copyright Piotr Półtorak 2015-2016.
 License: $(WEB boost.org/LICENSE_1_0.txt, Boost License 1.0).
 Authors: Piotr Półtorak
 */

module draft.database.database;

import draft.database.collection;
import draft.database.storage;
import std.algorithm;
import std.array;

debug 
{
    import std.experimental.logger;
    static this()
    {
        globalLogLevel(LogLevel.off);
    }
}

struct DataBase
{
    /**
     * DataBase constructor. Creates an object representing database file 
     * based on provided path and parameters. 
     * 
     * 
     * Params: 
     *      path = Path to database file. If file doesn't exist an empty one is created
     *      params = Custom parameters representing fundamental database properties
     */
    this(string path, DbParams params = DbParams.init)
    {
        debug trace("Opening database: ", path);

        mStorage = new DbStorage(path, params);
        if ( mStorage.isEmpty())
        {
            debug trace("Initializing database: ", path);
            mStorage.initializeStorage();
            initializeDataBase();
        }

    }

    /**
     * Creates new collection in database. If a collection with the same name already
     * exists ans assersion is thrown
     * 
     * Params: 
     *      name = String representation identifying collection is database
     * Returns: 
     *      Newly created collection
     * 
     */
    Collection!T createCollection(T)(string name)
    {
        debug trace("Creating collection: ", name);
        auto masterTable = Collection!DbTableInfo(mStorage, PageNo.master);
        auto found = masterTable.filter!(a => a.name == name);
        Collection!T collection;
        if (found.empty)
        {
            //create new table
            collection = Collection!T(mStorage, PageNo.invalid);
            auto rootPageId = collection.createCollection(PageType.root);
            masterTable.put(DbTableInfo(name,rootPageId));
        }
        else
        {
            assert(false, "Collection already exists");
        }
        return collection;
    }

    /**
     * Removes collection from database.
     * 
     * Params: 
     *      name = String representation identifying collection is database
     */
    void removeCollection(T)(string name)
    {
        auto tables = Collection!TableInfo(mStorage, PageNo.Master);
        mStorage.dropTable(tables.front.pageId);
    }

    /**
     * Returns collection from database by its string identifier. If the provided  
     * collection name is not found and  autoCreate argument is set to true a new 
     * collection is created. Otherwise assertion is thrown.
     * 
     * Params: 
     *      name = String representation identifying collection is database
     *      autoCreate = If set to true then a new collection is created 
     *                   in the case when colletction with provided name
     *                   doesn't exist
     * Returns: 
     *      Collection
     *
     */
    Collection!T collection(T) (string name, bool autoCreate = false)
    {
        debug trace("Opening collection: ", name);
        auto masterTable = Collection!DbTableInfo(mStorage, PageNo.master);

        Collection!T collection;
        if(name == "_Internal.MasterTable")
        {
            collection = Collection!(T)(mStorage, PageNo.master);
        }
        else
        {
            auto found = masterTable.filter!(a => a.name == name);
            if (!found.empty)
            {
                collection = Collection!T(mStorage, found.front.pageNo);
            }
            else if (autoCreate)
            {
                //create new table            
                collection = Collection!T(mStorage, PageNo.invalid);
                auto rootPageId = collection.createCollection(PageType.root);
                masterTable.put(DbTableInfo(name,rootPageId));

            }
            else
            {
                assert(true, "Collection doesn't exist");
            }
        }
        return collection;
    }

    /**
     * Returns collection names present in database
     * 
     */
    const(string[]) getCollections() 
    {
        import std.array;
        auto masterTable = Collection!DbTableInfo(mStorage, PageNo.master);
        return masterTable.map!(item => item.name).array;
    }

private:

    /**
     * Returns collection names present in database
     * 
     */
    void initializeDataBase()
    {   
        //create Master Table (root page only)
        auto masterTable = Collection!DbTableInfo(mStorage, PageNo.invalid);
        masterTable.createCollection(PageType.master);
        // For now rootPageId and collectionId is the same value
        auto rootPageId =  masterTable.collectionId;
        assert (rootPageId == PageNo.master);
        //insert master table info Item to table
        masterTable.put(DbTableInfo("_Internal.MasterTable",rootPageId));
    }

    DbStorage* mStorage;

}


unittest
{

    debug info("Unittest [database] Test 1 start");
    
    static struct A
    {
        int a;
        short b;
    }
    
    static struct B
    {
        uint a;
        long b;
        float c;
    }
    
    static struct C
    {
        int a;
        string name;
    }
    
    
    static struct D
    {
        string city;
        string country;
    }
    
    DataBase db = DataBase("",DbParams(128, DbFlags.memoryStorage));
    auto collA = db.createCollection!A("SmallIntegers");
    auto collB = db.createCollection!B("Numbers");
    auto collC = db.createCollection!C("Mixed");
    auto collD = db.createCollection!D("Strings");
    
    int[] smallIntegers = [40,41,42,43];
    short c =1;
    smallIntegers
        .map!(a => A(a,c++))
            .copy(collA);
    
    auto numbers = [B(1,-1,0.2), B(100_000_000,-1_000_000_000_000,8.71234), B(9_876,5_123_456_789_012,-0.2)];
    numbers.copy(collB);
    
    auto mixed = [C(1,"test"), C(2,"Hello World!"), C(-1,"Boom"), C(0,"P")];
    mixed.copy(collC);
    
    auto strings = [D("Poznan","Poland"), D("Budapest","Hungary"), D("Warsaw","Poland"), D("Phobos","Mars")];
    strings.copy(collD);
    
    auto data = db.collection!A("SmallIntegers");
    assert(data.array == [A(40,1),A(41,2), A(42,3), A(43,4)]);
    
    auto data2 = db.collection!B("Numbers");
    assert(data2.array == numbers);
    data2.removeItem(B(9_876,5_123_456_789_012,-0.2));
    data2.put(B(9,9,9));
    data2.removeItem(B(1,-1,0.2));
    data2.put(B(1,-2,-3));
    assert(data2[0] == B(9,9,9));
    assert(data2.array.length == numbers.length - 1 + 1 - 1 + 1);
    
    auto data3 = db.collection!C("Mixed");
    assert(data3.array == mixed);
    
    auto data4 = db.collection!D("Strings").filter!(s => (s.country == "Poland" || s.city.canFind("dap")));
    assert(data4.array == [D("Poznan","Poland"), D("Budapest","Hungary"), D("Warsaw","Poland")]);
    
    debug info("Unittest [database] Test 1 passed!");
    
}

unittest
{
    //debug globalLogLevel(LogLevel.trace);
    //debug scope(exit)globalLogLevel(LogLevel.info);
    debug info("Unittest [database] Test 2 start!");
    
    static struct A
    {
        string a;
        int[] b;
    }
    
    DataBase db = DataBase(null,DbParams(128, DbFlags.memoryStorage));
    auto collectionA = db.createCollection!A("Scores");
    
    [A("James",[1,2,3]), A("Hero 2", [345,-94321,0,6621]), A("R2D2",[13,-987654321])].copy(collectionA);
    collectionA = db.collection!A("Scores");
    auto found = collectionA.filter!(a => a.a.canFind("2"));
    foreach(oldItem;found)
    {
        A newItem = {oldItem.a ~ " <updated>", [3]};
        
        collectionA.update(oldItem,newItem);
    }
    assert(collectionA.array == [A("James",[1,2,3]), A("Hero 2 <updated>", [3]), A("R2D2 <updated>",[3])]);
    
    debug info("Unittest [database] Test 2 passed!");
}

unittest
{
    debug info("Unittest [database] Test 3 start");
    static struct A
    {
        string a;
        int[] b;
        ubyte c;
    }
    
    DataBase db = DataBase(null,DbParams(1024,DbFlags.memoryStorage));
    auto collectionA = db.createCollection!A("Many");
    
    for (int i = 1 ; i <= 100_000; ++i)
    {
        import std.conv;
        auto a = A((i+1000).to!string(),[i+1,i+2,i+3,i+4], cast(ubyte)i);
        collectionA.put(a);
    }
    
    collectionA = db.collection!A("Many");
    collectionA.update(A("1002",[3,4,5,6], 2), A("Boooom",[3,4,5,6], 2));
    assert(collectionA[1] == A("Boooom",[3,4,5,6], 2));
    debug info("Unittest [database] Test 3 passed!");
}

/*
 unittest
 {
 writeln("Test 4 start");
 import std.datetime;

 static struct Author
 {
 string name;
 }
 static struct Post
 {
 string text;
 Author author;
 DateTime date;
 }
 
 static struct ForumThread
 {
 Author author;
 string title;
 Post[] posts;
 }

 DataBase db = DataBase(null,128);

 auto forum = db.collection!Post("C.posts");
 forum.put(Post());
 writeln("Test 4 passed!");
 }

 */

/*
 unittest
 {
 writeln("Test 5 start");

 static struct City
 {
 string name;
 }

 static struct Address
 {
 @DbReference("Cities");
 City city;
 string street;
 }

 DataBase db = DataBase(null,128);
 auto cities = db.createCollection!(City)("Cities");
 auto addresses = db.createCollection!(Address)("Addresses");

 writeln("Test 5 passed!");
 }
 */
