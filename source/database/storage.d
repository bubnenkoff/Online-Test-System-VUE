/*
 Copyright: Copyright Piotr Półtorak 2015-2016.
 License: $(WEB boost.org/LICENSE_1_0.txt, Boost License 1.0).
 Authors: Piotr Półtorak
 */

module draft.database.storage;

import std.traits;
import std.file;
import std.bitmanip;

debug 
{
    import std.experimental.logger;
    static this()
    {
        globalLogLevel(LogLevel.info);
    }
}

enum magicString = "DLDb";
enum minimalPageSize = 64;
enum minimalBinSize = 32;

immutable dbAllocResolution = 4; //in bytes

enum PageNo: uint
{ 
    invalid = 0, 
    master = 1 
}

enum PageType: ubyte
{ 
    unknown,
    master,
    root,
    lookup,
    slot,
    data
}

enum DbItemFlags: ubyte
{ 
    embedded        = 1<<0,
    fragmented      = 1<<1, 
    compressed      = 1<<2
}

enum DbType: ubyte 
{ 
    Char, Wchar, Dchar,
    Ubyte, Byte, Ushort, Short, Uint, Int, Ulong, Long,
    Float, Double,
    Array, StructStart, StructEnd, DbReferece
}

enum DbFlags: uint
{ 
    memoryStorage = 1 << 0
}

struct DbParams
{
    uint pageSize = 1024;
    uint flags; // DbFlags
}

struct DbTableInfo
{
    string name;
    uint pageNo;
    DbType[] typeInfoArray;
}

struct VarArray
{
    ubyte length;
    ubyte[9] data;
}

struct DbMasterHeader  
{
align(1):
    immutable(char)[4] magicString= .magicString; 
    uint pageSize = 0;
    uint freePage = PageNo.invalid;
}

struct DbTableHeader  
{
align(1):
    ulong itemCount = 0;
    DbPointer freeDataPtr;
    uint binSize = minimalBinSize;
    uint dataInfoPageNo = PageNo.invalid;
}

struct DbPointer
{
    ulong rawData = 0;
    
    ulong cellData()
    {
        // get lower 7 bytes
        return rawData & 0x00FF_FFFF_FFFF_FFFF;
    }
    
    void cellData(ulong cellData)
    {
        // get lower 7 bytes
        rawData = rawData & 0xFF00_0000_0000_0000;
        rawData = rawData | cellData;
    }
    
    uint offset()
    {
        // get higher 4 bytes with cleared flag byte
        return cast(uint)(rawData >> 32) & 0x00FF_FFFF ;
    }
    
    void offset(uint offset)
    {
        rawData = rawData & 0xFF00_0000_FFFF_FFFF;
        rawData = rawData | (cast(ulong)offset << 32);
    }
    
    uint pageNo()
    {
        // get lower 4 bytes
        return cast(uint)rawData;
    }

    void pageNo(uint pageNo)
    {
        rawData = rawData & 0xFFFF_FFFF_0000_0000;
        rawData = rawData | pageNo;
    }
    
    ubyte flags()
    {
        // get the highest byte
        return cast(ubyte)(rawData >> 56);
    }
    
    void flags(ubyte flags)
    {
        // get the highest byte
        
        rawData = rawData & 0x00FF_FFFF_FFFF_FFFF;
        rawData =  rawData | (cast(ulong)flags << 56);
    }
    
    string toString()
    {
        import std.conv;
        return "{flags=" ~ flags.to!string ~ " offset=" ~ offset.to!string ~ " pageNo=" ~ pageNo.to!string ~ "}";
    }
    
}

struct DbPage
{
    PageType mPageType = PageType.unknown;
    uint mTableHeaderOffset = 0;
    uint mDbHeaderOffset = 0;
    uint mPayloadOffset = 0;
    uint mPageNo = PageNo.invalid;
    ubyte[] mRawBytes = null;

    this (PageType pageType, uint pageNo, uint pageSize)
    {
        mPageType = pageType;
        switch (pageType)
        {
            case PageType.master:
                mTableHeaderOffset = DbMasterHeader.sizeof;
                mPayloadOffset = cast(uint)(mTableHeaderOffset + DbTableHeader.sizeof);
                break;
            case PageType.root:
                mPayloadOffset = DbTableHeader.sizeof;
                break;
            default:
                break;
        }
        mRawBytes.length = pageSize;
        mPageNo = pageNo;
    }

    void writeMasterHeader(DbMasterHeader dbHeader)
    {
        assert(mPageType == PageType.master, "Invalid Page Type!");
        mRawBytes[0..DbMasterHeader.sizeof] = cast(ubyte[])(cast(void*)&dbHeader)[0..DbMasterHeader.sizeof];
    }

    DbMasterHeader readMasterHeader()
    {
        assert(mPageType == PageType.master, "Invalid Page Type!");
        return *(cast(DbMasterHeader*)cast(void*)mRawBytes.ptr);
    }

    void writeTableHeader(DbTableHeader tableHeader)
    {
        assert(mPageType == PageType.master || mPageType == PageType.root, "Invalid Page Type!");

        mRawBytes[mTableHeaderOffset..mTableHeaderOffset+DbTableHeader.sizeof] = cast(ubyte[])(cast(void*)&tableHeader)[0..DbTableHeader.sizeof];
    }

    DbTableHeader readTableHeader()
    {
        assert(mPageType == PageType.master || mPageType == PageType.root, "Invalid Page Type!");

        return *(cast(DbTableHeader*)cast(void*)mRawBytes[mTableHeaderOffset..mTableHeaderOffset+DbTableHeader.sizeof]);
    }

    void writeSlot(uint index, DbPointer pointer)
    {
        uint offset = index * cast(uint)DbPointer.sizeof;
        ulong rawPointer = pointer.rawData;
        mRawBytes[offset..offset + DbPointer.sizeof] = cast(ubyte[])(cast(void*)&rawPointer)[0..DbPointer.sizeof];
    }

    DbPointer readSlot(uint index, int size)
    {
        uint offset = index * cast(uint)DbPointer.sizeof;
        DbPointer pointer = DbPointer(*cast(ulong*)(cast(void*)(&mRawBytes[offset])));
        return pointer;
    }

    void writeLookupPointer(uint index, uint pointer)
    {
        auto offset = mPayloadOffset + index * uint.sizeof;
        mRawBytes[offset..offset + pointer.sizeof] = cast(ubyte[])(cast(void*)(&pointer))[0..pointer.sizeof];
    }

    uint readLookupPointer(uint index)
    {
        auto offset = mPayloadOffset + index * uint.sizeof;
        return *(cast(uint*)cast(void*)mRawBytes[offset..offset+uint.sizeof]);
    }

    void writeBytes(uint offset, ubyte[] data)
    {
        mRawBytes[offset..offset + data.length] = data;
    }

    ubyte[] readBytes(uint offset, ulong count)
    {
        return mRawBytes[offset..offset + cast(size_t)count];
    }

    void dump(int bytesPerLine = 8)
    {
        assert (mPageNo);
        import std.range;
        import std.stdio;
        int lineNo;
        writefln( "-------------------- Page %2d --------------------------", mPageNo);
        foreach(line; chunks(cast(ubyte[]) mRawBytes,bytesPerLine))
        {
            writef( "Offset %4d:", lineNo*bytesPerLine);
            foreach(byteOfData; line)
            {
                writef("%4s ",byteOfData);
            }
            writeln();
            ++lineNo;
        }
        writefln( "-------------------------------------------------------", mPageNo);
    }

}

struct DbCell
{
    ubyte[] data;

    this(ubyte[] cellData)
    {
        data = cellData;
    }
    
    this(ulong cellData)
    {
        data.length = cellData.sizeof;
        data[0..cellData.sizeof] = cast(ubyte[])((cast(void*)&cellData)[0..cellData.sizeof]);
    }

    void from (T)(T item)
    {
        data.reserve = 256;
        static if(is(T == struct))
        {
            foreach(idx, memberType; FieldTypeTuple!(T))
            {
                static if(isArray!memberType)
                {
                    alias ElementType = typeof(item.tupleof[idx][0]);
                    static if(isBasicType!ElementType)
                    {
                        ulong length = item.tupleof[idx].length;
                        data ~= (cast(ubyte*)(&length))[0..8];

                        foreach (el ; item.tupleof[idx])
                        {
                            data  ~= (cast(ubyte*)&el)[0..ElementType.sizeof];
                        }
                    }
                    else
                    {
                        assert(false);
                    }
                }
                else static if(isBasicType!memberType)
                {
                    data ~= (cast(ubyte*)(&item.tupleof[idx]))[0..memberType.sizeof];
                }
                else
                {
                    assert(false);
                }
            }
        }
        else
        {
            assert(false);
        }
    }

    T to (T)()
    {
        T item;
        assert(data, "Invalid cell data");
        static if(is(T == struct))
        {
            foreach(idx, memberType; FieldTypeTuple!(T))
            {
                static if(isArray!memberType)
                {
                    alias ElementType = typeof(item.tupleof[idx][0]);
                    static if(isBasicType!ElementType)
                    {
                        item.tupleof[idx].length = *cast(size_t*)data[0..ulong.sizeof];
                        data = data[ulong.sizeof..$];
                        
                        foreach (i, el ; item.tupleof[idx])
                        {
                            cast(Unqual!ElementType)item.tupleof[idx][i] = *cast(ElementType*)data[0..ElementType.sizeof];
                            data = data[ElementType.sizeof..$];
                        }
                    }
                    else
                    {
                        assert(false);
                    }
                }
                else static if(isBasicType!memberType)
                {
                    alias typeof(item.tupleof[idx]) targetType;
                    item.tupleof[idx] = *cast(targetType*)data[0..targetType.sizeof];
                    data = data[targetType.sizeof..$];
                }
                else
                {
                    assert(false);
                }
            }
        }
        else
        {
            assert(false);
        }

        return item;
    }

    unittest
    {
        debug info("Unittest [Cell] start");

        static struct B
        {
            byte a;
            int b;
            uint c;
            ulong d;
            double e;
            float f;
        }

        static struct C
        {
            int a;
            char[] chars;
            string str;
        }

        static struct D
        {
            string str;
            int[] a;
        }

        B b = B(110,-5_441_697,3_456_924_743,7_648_136_946_296, -4.2, 10.125 );
        DbCell cellB;
        cellB.from(b);

        C c = C(-1_345_429_012, ['h','e','l','l','o'], "world");
        DbCell cellC;
        cellC.from(c);

        D d = D("D Programming Language", [-1_345_429_012, -1, 0, 1, 1_345_429_012]);
        DbCell cellD;
        cellD.from(d);

        DbCell cellB2 = DbCell(cellB.data);
        B b2 = cellB2.to!B();
        assert(b2 == b);

        DbCell cellC2 = DbCell(cellC.data);
        C c2 = cellC2.to!C();
        assert(c2 == c);

        DbCell cellD2 = DbCell(cellD.data);
        D d2 = cellD2.to!D();
        assert(d2 == d);
        debug info("Unittest [Cell] passed!");
    }
    
    /**
     * Function converts ulong for array of nine bytes
     * In each byte of array there is 7 bits for information 
     * from ulong, One bit is for information if next byte is needed.
     * Params:
     *     number =  ulong to be converted to array
     * Returns: Array to of with encoded value
     */
    static VarArray convertUlongToArray(ulong number)
    {
        ubyte i = 0;
        VarArray result;
        result.data[i] = number & 0x7F;
        ubyte bits = 7;
        while(number >> bits)
        {
            result.data[i] |= 0x80;
            ++i;

            if(i == 8)
            {
                result.data[i] = cast(ubyte)(number >> bits);
                break;
            }
            result.data[i] = cast(ubyte)((number >> bits) & 0x7F);
            bits += 7;
        }

        result.length  = ++i;
        return result;
    }

    /**
     * Function converts array to ulong
     * 7 bits of every field in array is ulong information
     * 1 bit is informaton if in next field of array there still more
     * data for ulong
     * Params: 
     *      table = Array of bytes with information of ulong number
     * Returns: ulong number
     */
    static ulong convertArrayToUlong(ubyte[] table)
    {
        ubyte i = 0;
        ulong result = table[0] & 0x7F;
        while(table[i] & 0x80)
        {
            ++i;
            if(i==8)
            {
                result |= cast(ulong)table[i] << (7 * 8);
                break;
            }
            result |= cast(ulong)(table[i] & 0x7F) << (7 * i);
        }
        return result;
    }

    unittest
    {
        import std.conv;

        debug info("Unittest [convertUlongToArray] start");
        assert( convertUlongToArray(0) == VarArray(1,[0x0,0x0,0x0,0x0,0x0,0x0,0x0,0x0,0x0]) );
        assert( convertUlongToArray(0x1) == VarArray(1,[0x1,0x0,0x0,0x0,0x0,0x0,0x0,0x0,0x0]));
        assert( convertUlongToArray(0x7F) == VarArray(1,[0x7F,0x0,0x0,0x0,0x0,0x0,0x0,0x0,0x0]));
        assert( convertUlongToArray(0x80) == VarArray(2,[0x80,0x01,0x0,0x0,0x0,0x0,0x0,0x0,0x0]));
        assert( convertUlongToArray(0xFF) == VarArray(2,[0xFF,0x01,0x0,0x0,0x0,0x0,0x0,0x0,0x0]));
        assert( convertUlongToArray(0x3FFF) == VarArray(2,[0xFF,0x7F,0x0,0x0,0x0,0x0,0x0,0x0,0x0]));
        assert( convertUlongToArray(0x4000) == VarArray(3,[0x80,0x80,0x01,0x0,0x0,0x0,0x0,0x0,0x0]));
        assert( convertUlongToArray(0x1FFFFF) == VarArray(3,[0xFF,0xFF,0x7F,0x0,0x0,0x0,0x0,0x0,0x0]));
        assert( convertUlongToArray(0x200000) == VarArray(4,[0x80,0x80,0x80,0x01,0x0,0x0,0x0,0x0,0x0]));
        assert( convertUlongToArray(0x0FFFFFFF) == VarArray(4,[0xFF,0xFF,0xFF,0x7F,0x0,0x0,0x0,0x0,0x0]));
        assert( convertUlongToArray(0x10000000) == VarArray(5,[0x80,0x80,0x80,0x80,0x01,0x0,0x0,0x0,0x0]));
        assert( convertUlongToArray(0x7FFFFFFFF) == VarArray(5,[0xFF,0xFF,0xFF,0xFF,0x7F,0x0,0x0,0x0,0x0]));
        assert( convertUlongToArray(0x800000000) == VarArray(6,[0x80,0x80,0x80,0x80,0x80,0x01,0x0,0x0,0x0]));
        assert( convertUlongToArray(0x3FFFFFFFFFF) == VarArray(6,[0xFF,0xFF,0xFF,0xFF,0xFF,0x7F,0x0,0x0,0x0]));
        assert( convertUlongToArray(0x40000000000) == VarArray(7,[0x80,0x80,0x80,0x80,0x80,0x80,0x01,0x0,0x0]));
        assert( convertUlongToArray(0x1FFFFFFFFFFFF) == VarArray(7,[0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0x7F,0x0,0x0]));
        assert( convertUlongToArray(0x2000000000000) == VarArray(8,[0x80,0x80,0x80,0x80,0x80,0x80,0x80,0x01,0x0]));
        assert( convertUlongToArray(0x0FFFFFFFFFFFFFF) == VarArray(8,[0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0x7F,0x0]));
        assert( convertUlongToArray(0x100000000000000) == VarArray(9,[0x80,0x80,0x80,0x80,0x80,0x80,0x80,0x80,0x01]));
        assert( convertUlongToArray(0x7FFFFFFFFFFFFFFF) == VarArray(9,[0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0x7F]));
        assert( convertUlongToArray(0x8000000000000000) == VarArray(9,[0x80,0x80,0x80,0x80,0x80,0x80,0x80,0x80,0x80]));
        assert( convertUlongToArray(0xFFFFFFFFFFFFFFFF) == VarArray(9,[0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF]));
        assert( convertUlongToArray(0xAFFFFFFFFFFFFFFF) == VarArray(9,[0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xAF]));
        assert( convertUlongToArray(0xF1FFFFFFFFFFFF80) == VarArray(9,[0x80,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xF1]));
        debug info("Unittest [convertUlongToArray] passed!");
        
        debug info("Unittest [convertArrayToUlong] start");
        assert( 0x0 == convertArrayToUlong([0x0]));
        assert( 0x1 == convertArrayToUlong([0x1]));
        assert( 0x7F == convertArrayToUlong([0x7F]));
        assert( 0x80 == convertArrayToUlong([0x80,0x01]));
        assert( 0xFF == convertArrayToUlong([0xFF,0x01]));
        assert( 0x3FFF == convertArrayToUlong([0xFF,0x7F]));
        assert( 0x4000 == convertArrayToUlong([0x80,0x80,0x01]));
        assert( 0x1FFFFF == convertArrayToUlong([0xFF,0xFF,0x7F]));
        assert( 0x200000 == convertArrayToUlong([0x80,0x80,0x80,0x01]));
        assert( 0x0FFFFFFF == convertArrayToUlong([0xFF,0xFF,0xFF,0x7F]));
        assert( 0x10000000 == convertArrayToUlong([0x80,0x80,0x80,0x80,0x01]));
        assert( 0x7FFFFFFFF == convertArrayToUlong([0xFF,0xFF,0xFF,0xFF,0x7F]));
        assert( 0x800000000 == convertArrayToUlong([0x80,0x80,0x80,0x80,0x80,0x01]));
        assert( 0x3FFFFFFFFFF == convertArrayToUlong([0xFF,0xFF,0xFF,0xFF,0xFF,0x7F]));
        assert( 0x40000000000 == convertArrayToUlong([0x80,0x80,0x80,0x80,0x80,0x80,0x01]));
        assert( 0x1FFFFFFFFFFFF == convertArrayToUlong([0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0x7F]));
        assert( 0x2000000000000 == convertArrayToUlong([0x80,0x80,0x80,0x80,0x80,0x80,0x80,0x01]));
        assert( 0x0FFFFFFFFFFFFFF == convertArrayToUlong([0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0x7F]));
        assert( 0x100000000000000 == convertArrayToUlong([0x80,0x80,0x80,0x80,0x80,0x80,0x80,0x80,0x01]));
        assert( 0x7FFFFFFFFFFFFFFF == convertArrayToUlong([0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0x7F]));
        assert( 0xFFFFFFFFFFFFFFFF == convertArrayToUlong([0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF]));
        assert( 0xAFFFFFFFFFFFFFFF == convertArrayToUlong([0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xAF]));
        assert( 0xF1FFFFFFFFFFFF80 == convertArrayToUlong([0x80,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xF1]));
        assert( 0x8000000000000000 == convertArrayToUlong([0x80,0x80,0x80,0x80,0x80,0x80,0x80,0x80,0x80]));
        
        assert(1234 == convertArrayToUlong(convertUlongToArray(1234).data));
        assert(123_456_789_123 == convertArrayToUlong(convertUlongToArray(123_456_789_123).data));
        assert(-123_456_789_123 == convertArrayToUlong(convertUlongToArray(-123_456_789_123).data));
        assert(0 == convertArrayToUlong(convertUlongToArray(0).data));
        assert(-1 == convertArrayToUlong(convertUlongToArray(-1).data));
        
        debug info("Unittest [convertArrayToUlong] passed!");

    }
}

struct DbFile
{
    string mFilePath;
    ubyte[] mBuffer;
    uint mPageSize;
    uint mPageCount;
    bool mMemoryStorage;

    this(string path, DbParams params)
    {
        mFilePath = path;
        mMemoryStorage  = params.flags & DbFlags.memoryStorage;
        if (mMemoryStorage)
        {
            mPageSize = params.pageSize;
        }
        else
        {
            import std.stdio;
            if (!exists(path))
            {
                debug trace("Creating new database: ", path);

                File(path,"w");
                mPageSize = params.pageSize;
            }
            else
            {
                File dbFile = File(path,"r");
                ubyte[32]  buffer;
                dbFile.rawRead(buffer);
                mPageSize = littleEndianToNative!uint(buffer[4..8]);
                mPageCount = cast(uint)(dbFile.size / mPageSize);

            }
        }


    }

    DbPage loadPage(PageType pageType, uint pageNo)
    {
        DbPage page = DbPage(pageType, pageNo, mPageSize);

        if(mMemoryStorage & DbFlags.memoryStorage)
        {
            page.mRawBytes = mBuffer[cast(size_t)(pageNo-1) * mPageSize .. cast(size_t)pageNo * mPageSize].dup;
        }
        else
        {
            import std.stdio;
            auto file = File(mFilePath);
            file.seek( (pageNo-1) * mPageSize);
            file.rawRead(page.mRawBytes);
        }
        return page;
    }

    void storePage(uint pageNo, ubyte[] pageData)
    {
        if(mMemoryStorage & DbFlags.memoryStorage)
        {
            mBuffer[cast(size_t)(pageNo-1)*mPageSize..cast(size_t)pageNo*mPageSize] = pageData;
        }
        else
        {
            import std.stdio;
            ulong offset = (pageNo-1) * mPageSize;
            assert (pageNo <= mPageCount, "Writing page over file size"); 
            auto file = File(mFilePath, "r+");
            file.seek( (pageNo-1) * mPageSize);
            file.rawWrite(pageData);
        }
    }

    uint appendPages(uint count)
    {
        mPageCount += count;
        if (mMemoryStorage & DbFlags.memoryStorage)
        {
            mBuffer.length = mPageSize*mPageCount;
        }
        else
        {
            import std.stdio;
            auto file = File(mFilePath, "r+");
            file.seek(mPageSize*mPageCount-1);
            file.rawWrite([cast(byte)0]);
        }

        return mPageCount;
    }

    uint slotsPerPage()
    {
        return mPageSize / DbPointer.sizeof;
    }

    uint lookupPointersPerPage()
    {
        return mPageSize / uint.sizeof;
    }

    void dump()
    {
        if(mMemoryStorage & DbFlags.memoryStorage)
        {
            for(int i=1; i <= (mBuffer.length / mPageSize); ++i)
            {
                loadPage(PageType.unknown, i).dump;
            }
        }
    }

    unittest
    {
        import std.file;
        scope (exit) remove("test.db");

        debug info("Unittest [DbFile] start");
        auto dbFile = DbFile("test.db", DbParams(128));
        dbFile.appendPages(1);
        dbFile.storePage(PageNo.master,DbPage(PageType.master,1,128).mRawBytes);
        debug info("Unittest [DbFile] passed!");
    }
}

struct DbStorage
{
    DbFile mDbFile;
    DbNavigator mNavigator;
    DbDataAllocator mDataAllocator;
    DbPageAllocator mPageAllocator;

    this(string path, DbParams params)
    {
        mDbFile = DbFile(path,params);
        mPageAllocator = DbPageAllocator(&mDbFile);
        mNavigator = DbNavigator(&mDbFile, &mPageAllocator);
        mDataAllocator = DbDataAllocator(&mDbFile, &mPageAllocator);
    }

    this(DbFile dbFile)
    {
        mDbFile = dbFile;
        mPageAllocator = DbPageAllocator(&mDbFile);
        mNavigator = DbNavigator(&mDbFile, &mPageAllocator);
        mDataAllocator = DbDataAllocator(&mDbFile, &mPageAllocator);
    }

    bool isEmpty()
    {
        return mDbFile.mPageCount == 0;
    }

    uint initializeStorage()
    {

        uint pageNo = mPageAllocator.reserveFreePage;

        assert (pageNo == PageNo.master);
        DbPage page = mDbFile.loadPage(PageType.master, pageNo);
        DbMasterHeader dbHeader;
        dbHeader.pageSize = mDbFile.mPageSize;
        page.writeMasterHeader(dbHeader);

        page.writeTableHeader(DbTableHeader());
        mDbFile.storePage(pageNo, page.mRawBytes);
        return pageNo;
    }

    uint createTable(PageType pageType)
    {
        uint pageNo = PageNo.invalid;
        if( pageType != PageType.master)
        {
            pageNo = mPageAllocator.reserveFreePage;
        }
        else
        {
            pageNo = PageNo.master;
        }

        assert(pageNo != PageNo.invalid);
        DbPage page = mDbFile.loadPage(pageType, pageNo);
        page.writeTableHeader(DbTableHeader());
        mDbFile.storePage(pageNo, page.mRawBytes);
        return pageNo;
    }

    void addItem(T)(uint rootPageNo, T item)
    {
        debug trace("rootPageNo=", rootPageNo, " item=", item);
        PageType pageType = (rootPageNo == PageNo.master) ? PageType.master : PageType.root;
        DbPage tableRootPage = mDbFile.loadPage(pageType, rootPageNo);

        DbTableHeader tableHeader = tableRootPage.readTableHeader;
        ulong itemCount = tableHeader.itemCount;

        DbCell cell;
        cell.from(item);

        // find slot for a new pointer in slotPage
        // from time to time new lookup pages need to be added
        DbPage slotPage = mNavigator.slotPageForNewItem(tableRootPage, itemCount+1);

        auto slotPageIndex = (itemCount) % mDbFile.slotsPerPage;

        if (cell.data.length > (DbPointer.sizeof - DbItemFlags.sizeof))
        {
            // we need a separate storage
            DbPointer dataPtr = mDataAllocator.allocateData(tableRootPage, cell.data);
            slotPage.writeSlot(cast(uint)slotPageIndex, dataPtr);
            debug trace("data in DataPages cell.data.length=", cell.data.length, " dataPtr=", dataPtr);

        }
        else
        {
            // data fits in the slot
            DbPointer pointer;
            pointer.flags = pointer.flags | DbItemFlags.embedded;
            pointer.cellData(*cast(ulong*)(cast(void*)cell.data));
            slotPage.writeSlot(cast(uint)slotPageIndex,pointer);
            debug trace("data in DbPointer cell.data.length=", cell.data.length, "pointer=", pointer );

        }


        //update table header
        tableHeader = tableRootPage.readTableHeader;
        tableHeader.itemCount++;
        tableRootPage.writeTableHeader(tableHeader);

        mDbFile.storePage(slotPage.mPageNo, slotPage.mRawBytes);
        mDbFile.storePage(rootPageNo, tableRootPage.mRawBytes);
    }
    unittest
    {
        debug info("Unittest [addItem] start");
        import std.range, std.array, std.conv;
        
        static struct C
        {
            int a;
            string name;
        }
        
        DbStorage storage = DbStorage("",DbParams(256, DbFlags.memoryStorage));
        uint masterTablePageNo = storage.initializeStorage();
        
        uint regularTablePageNo = storage.createTable(PageType.root);
        
        auto bigItem = C(5, "123456789 ".cycle.take(200).array.to!string);

        //TODO remove comments
        //storage.addItem(regularTablePageNo,bigItem); 
        //assert(storage.fetchDbItem!C(regularTablePageNo, 1) == bigItem);

        debug info("Unittest [addItem] passed!");
    }
    
    ulong nextDbItemId(uint rootPageNo, ulong id)
    {
        ++id;
        PageType pageType = (rootPageNo == PageNo.master) ? PageType.master : PageType.root;
        DbPage page = mDbFile.loadPage(pageType, rootPageNo);
        DbTableHeader tableHeader = page.readTableHeader;
        debug trace ("itemCount=", tableHeader.itemCount);
        return (id <= tableHeader.itemCount) ? id :  0;
    }

    T fetchDbItem(T)(uint rootPageNo, ulong id)
    {
        debug trace("rootPageNo=", rootPageNo, " id=", id);
        PageType pageType = (rootPageNo == PageNo.master) ? PageType.master : PageType.root;
        DbPage rootPage = mDbFile.loadPage(pageType, rootPageNo);
        DbTableHeader tableHeader = rootPage.readTableHeader;
        DbPage slotPage = mNavigator.findDbSlotPage(rootPage, id);

        uint slotIndex = cast(uint)((id-1) % mDbFile.slotsPerPage);
        DbPointer pointer = slotPage.readSlot(slotIndex,DbPointer.sizeof);

        DbCell cell;
        //Check id data is embedded in the pointer

        if (pointer.flags & DbItemFlags.embedded)
        {
            cell = DbCell(pointer.cellData);
        }
        else
        {
            cell = DbCell(mDataAllocator.readCellData(pointer,tableHeader.binSize));
        }
        T item = cell.to!T;
        debug trace("Fetched=", item);
        return item;
    }

    void updateItem(T)(uint rootPageNo, ulong itemId, T item)
    {
        debug trace("rootPageNo=", rootPageNo, " itemId=", itemId, " item=", item);

        PageType pageType = (rootPageNo == PageNo.master) ? PageType.master : PageType.root;
        // read old item to release its storage
        debug trace("Loading page=", rootPageNo);

        DbPage rootPage = mDbFile.loadPage(pageType, rootPageNo);
        DbPage slotPage = mNavigator.findDbSlotPage(rootPage, itemId);
        uint slotIndex = cast(uint)((itemId-1) % mDbFile.slotsPerPage);
        debug trace("Slot found page=", slotPage.mPageNo, " slotIndex=", slotIndex);

        DbPointer itemPointer = slotPage.readSlot(slotIndex,DbPointer.sizeof);
        debug trace("DataPointer=", itemPointer);

        DbCell cell;
        cell.from(item);


        if (cell.data.length > (DbPointer.sizeof - DbItemFlags.sizeof))
        {
            //Check if the previous data is embedded into DbPointer or not
            if (itemPointer.flags & DbItemFlags.embedded)
            {
                // new storage needed
                DbPointer dataPtr = mDataAllocator.allocateData(rootPage,cell.data);
                debug trace("new storage dataPtr=", dataPtr);
                slotPage.writeSlot(cast(uint)slotIndex, dataPtr);
            }
            else
            {
                DbPointer dataPtr = mDataAllocator.replaceData(rootPage, itemPointer, cell.data);
                debug trace("replace dataPtr=", dataPtr);
                slotPage.writeSlot(cast(uint)slotIndex, dataPtr);
            }

        }
        else
        {
            debug trace("data in pointer cell.data.length=", cell.data.length);

            // data fits in the slot
            DbPointer newPointer;
            newPointer.flags = newPointer.flags | DbItemFlags.embedded;
            newPointer.cellData(*cast(ulong*)(cast(void*)cell.data));
            slotPage.writeSlot(slotIndex,newPointer);
            if (!(itemPointer.flags & DbItemFlags.embedded))
            {
                // free unused space
                mDataAllocator.deallocateData(rootPage, itemPointer);
            }
        }

        mDbFile.storePage(slotPage.mPageNo, slotPage.mRawBytes);
        mDbFile.storePage(rootPageNo, rootPage.mRawBytes);
    }

    void removeItem(uint rootPageNo, ulong itemId)
    {
        debug trace("rootPageNo=", rootPageNo, " itemId=", itemId);

        PageType pageType = (rootPageNo == PageNo.master) ? PageType.master : PageType.root;

        DbPage rootPage = mDbFile.loadPage(pageType, rootPageNo);
        DbTableHeader dbTableHeader = rootPage.readTableHeader;
        ulong currentItemCount = dbTableHeader.itemCount;

        DbPage slotPageDel = mNavigator.findDbSlotPage(rootPage, itemId);
        uint slotIndexDel = cast(uint)((itemId-1) % mDbFile.slotsPerPage);
        debug trace("slotIndexDel=", slotIndexDel);

        DbPointer itemPointer = slotPageDel.readSlot(slotIndexDel,DbPointer.sizeof);

        mDataAllocator.deallocateData(rootPage, itemPointer); 

        // put the last item in the place of the removed one
        DbPage slotPageLast = mNavigator.findDbSlotPage(rootPage, currentItemCount);

        uint slotIndexLast = cast(uint)((currentItemCount-1) % mDbFile.slotsPerPage);
        debug trace("slotIndexLast=", slotIndexLast);

        DbPointer lastItemPointer = slotPageLast.readSlot(slotIndexLast, DbPointer.sizeof);
        slotPageDel.writeSlot(slotIndexDel, lastItemPointer);
        mDbFile.storePage(slotPageDel.mPageNo, slotPageDel.mRawBytes);

        // check if lookup tree needs update
        if(mNavigator.isSlotPageDeallocNeeded(currentItemCount-1))
        {
            mNavigator.collapseLookupTree(rootPage, currentItemCount-1);
            mPageAllocator.releasePage(slotPageDel.mPageNo);
        }

        // Update item count and save root page
        dbTableHeader = rootPage.readTableHeader;
        --dbTableHeader.itemCount;
        rootPage.writeTableHeader(dbTableHeader);
        mDbFile.storePage(rootPage.mPageNo, rootPage.mRawBytes);
    }

    void dropTable(ulong pageNo)
    {
        assert(0);
    }
}

struct DbPageAllocator
{
    DbFile* mDbFile;

    uint reserveFreePage()
    {
        debug trace("reserveFreePage current mPageCount=", mDbFile.mPageCount);
        debug trace("reserveFreePage current mPageSize=", mDbFile.mPageSize);
        if (mDbFile.mPageCount == 0) return mDbFile.appendPages(1);
        DbPage masterPage = mDbFile.loadPage(PageType.master, PageNo.master);
        DbMasterHeader masterHeader = masterPage.readMasterHeader();
        if (masterHeader.freePage == PageNo.invalid)
        {
            return mDbFile.appendPages(1);
        }
        else
        {
            auto oldNo = masterHeader.freePage;
            DbPage freePage = mDbFile.loadPage(PageType.unknown, masterHeader.freePage);
            ubyte[4] nextFreePageBytes = freePage.readBytes(0, 4);
            masterHeader.freePage = littleEndianToNative!(uint,4)(nextFreePageBytes);
            masterPage.writeMasterHeader(masterHeader);
            mDbFile.storePage(PageNo.master, masterPage.mRawBytes);
            return oldNo;
        }
    }
    unittest
    {
        debug info("Unittest [reserveFreePage] start");

        DbPageAllocator pageAllocator = prepareDbPageAllocator(64);
        auto pageNo = pageAllocator.reserveFreePage();
        assert(pageNo == 3);        
        debug info("Unittest [reserveFreePage] passed!");
    }

    void releasePage(uint pageNo)
    {
        DbPage releasedPage = mDbFile.loadPage(PageType.unknown, pageNo);
        DbPage masterPage = mDbFile.loadPage(PageType.master, PageNo.master);
        DbMasterHeader masterHeader = masterPage.readMasterHeader();
        masterHeader.freePage = pageNo;
        masterPage.writeMasterHeader(masterHeader);
        releasedPage.writeBytes(0, nativeToLittleEndian(masterHeader.freePage));

        mDbFile.storePage(PageNo.master, masterPage.mRawBytes);
        mDbFile.storePage(pageNo, releasedPage.mRawBytes);
    }
    unittest
    {
        debug info("Unittest [releasePage] start");

        DbPageAllocator pageAllocator = prepareDbPageAllocator(64);
        DbPage masterPage = pageAllocator.mDbFile.loadPage(PageType.master, PageNo.master);
        DbMasterHeader masterHeader = masterPage.readMasterHeader;
        assert (masterHeader.freePage == PageNo.invalid);
        auto pageNo = 2;
        pageAllocator.releasePage(pageNo);
        masterPage = pageAllocator.mDbFile.loadPage(PageType.master, PageNo.master);
        DbMasterHeader masterHeader2 = masterPage.readMasterHeader;
        assert (masterHeader2.freePage == 2);
        debug info("Unittest [releasePage] passed!");

    }

}

struct DbDataAllocator
{
    DbFile *mDbFile;
    DbPageAllocator *mPageAllocator;

    DbPointer allocateData(DbPage rootPage, ubyte[] cellData)
    {
        debug trace("Allocating data rootPage=", rootPage.mPageNo, " cellData.length=",  cellData.length);

        DbTableHeader tableHeader = rootPage.readTableHeader;
        int binSize = tableHeader.binSize;
        assert(cellData.length > 0);

        if (tableHeader.freeDataPtr.pageNo == PageNo.invalid)
        {
            tableHeader.freeDataPtr = makeNewHeapPage(binSize);
        }
        DbPointer dataPtr = tableHeader.freeDataPtr;
        if ( cellData.length + ulong.sizeof /* cell length bytes */ > binSize)
        {
            dataPtr.flags = DbItemFlags.fragmented;
        }
        tableHeader.freeDataPtr = writeCellData(dataPtr, tableHeader.binSize, cellData);
        if (tableHeader.freeDataPtr.pageNo == PageNo.invalid)
        {
            tableHeader.freeDataPtr = makeNewHeapPage(binSize);
        }

        rootPage.writeTableHeader(tableHeader);
        mDbFile.storePage(rootPage.mPageNo, rootPage.mRawBytes);

        return dataPtr;
    }

    unittest
    {
        debug info("Unittest [allocateData] start");

        DbDataAllocator allocator = prepareDbDataAllocator(128);
        DbPage rootPage = allocator.mDbFile.loadPage(PageType.root,2);
        import std.range, std.conv, std.algorithm;
        ubyte[] cellData = 123.repeat.take(200).map!(x=>x.to!ubyte).array;
        DbPointer dataPtr = rootPage.readTableHeader.freeDataPtr;
        allocator.allocateData(rootPage,cellData);
        dataPtr.flags = DbItemFlags.fragmented;
        assert (allocator.readCellData(dataPtr,32) == cellData);
        debug info("Unittest [allocateData] passed!");

    }

    void deallocateData(DbPage rootPage, DbPointer dataPtr)
    {
        debug trace("Deallocating rootPageNo=", rootPage.mPageNo, " dataPtr=", dataPtr);

        DbTableHeader tableHeader = rootPage.readTableHeader;
        DbPointer oldFreeDataPtr = tableHeader.freeDataPtr;
        tableHeader.freeDataPtr = dataPtr;
        rootPage.writeTableHeader(tableHeader);
        DbPage dataPage = mDbFile.loadPage(PageType.data, dataPtr.pageNo);
        ubyte[] bytes = nativeToLittleEndian(oldFreeDataPtr.rawData);
        while (dataPtr.flags & DbItemFlags.fragmented)
        {
            debug trace("Loading next dataPtr=", dataPtr);
            ubyte[] nextPtrBytes = dataPage.readBytes(dataPtr.offset, DbPointer.sizeof);
            dataPtr = *cast(DbPointer*)(nextPtrBytes.ptr);
            debug trace("Updating dataPtr=", dataPtr);
            dataPage = mDbFile.loadPage(PageType.data, dataPtr.pageNo);


        }
        debug trace("Updating last page=", dataPage.mPageNo);
        dataPage.writeBytes(dataPtr.offset, bytes);
        mDbFile.storePage(dataPage.mPageNo,dataPage.mRawBytes);
        mDbFile.storePage(rootPage.mPageNo,rootPage.mRawBytes);

    }
    unittest
    {
        debug info("Unittest [deallocateData] start");

        DbDataAllocator allocator = prepareDbDataAllocator(128);
        DbPage rootPage = allocator.mDbFile.loadPage(PageType.root,2);
        DbPointer toDelete;
        toDelete.offset(0);
        toDelete.pageNo(3);
        toDelete.flags(DbItemFlags.fragmented);

        allocator.deallocateData(rootPage,toDelete);

        DbTableHeader tableHeader = rootPage.readTableHeader;
        DbPage dataPage = allocator.mDbFile.loadPage(PageType.data,3);
        DbPointer ptr = *cast(DbPointer*)dataPage.readBytes(32, 8).ptr;
        assert(tableHeader.freeDataPtr == toDelete);

        DbPointer freePtr;
        freePtr.offset(0);
        freePtr.pageNo(3);
        assert(ptr == freePtr);
        debug info("Unittest [deallocateData] passed!");
    }

    DbPointer replaceData(DbPage rootPage, DbPointer itemPointer, ubyte[] data)
    {
        debug trace("Replaceing rootPageNo=", rootPage.mPageNo, " itemPointer=", itemPointer, " data.length=", data.length);
        deallocateData(rootPage, itemPointer);
        return allocateData(rootPage, data);
    }
    unittest
    {

    }

    DbPointer makeNewHeapPage(int binSize)
    {
        uint pageNo = mPageAllocator.reserveFreePage();
        DbPointer beginPointer;
        beginPointer.pageNo = pageNo;
        DbPage newHeapPage = mDbFile.loadPage(PageType.data, pageNo);
        for (int offset = 0; offset < mDbFile.mPageSize ; offset+= binSize)
        {
            DbPointer binPointer;

            uint pointerOffset = offset+ binSize;
            if ( pointerOffset <= (mDbFile.mPageSize -binSize))
            {
                binPointer.offset = pointerOffset;
                binPointer.pageNo = pageNo;
            }

            newHeapPage.writeBytes(offset, cast(ubyte[])(cast(void*)&binPointer.rawData)[0..DbPointer.sizeof]);
        }
        mDbFile.storePage(newHeapPage.mPageNo, newHeapPage.mRawBytes);
        return beginPointer;
    }

    unittest
    {
        debug info("Unittest [makeNewHeapPage] start");
        
        debug info("Unittest [makeNewHeapPage] passed!");
    }

    DbPointer writeCellData(DbPointer heapDataPtr, uint binSize, ubyte[] data)
    {
        debug trace ("1 heapDataPtr=", heapDataPtr, " data.length=", data.length);
        debug trace ("2 Loading data pageNo=", heapDataPtr.pageNo);
        DbPage currHeapPage = mDbFile.loadPage(PageType.data, heapDataPtr.pageNo);
        ubyte[] nextFreePtrBytes = currHeapPage.readBytes(heapDataPtr.offset, DbPointer.sizeof);
        DbPointer nextFreePtr = DbPointer( *cast(ulong*)cast(void*)nextFreePtrBytes.ptr);
        debug trace ("3 nextFreePtr=", nextFreePtr);

        ubyte[8] cellLengthBytes = nativeToLittleEndian!ulong(data.length);
        const ulong bytesToWrite = data.length + cellLengthBytes.length;
        size_t dataBytesLeft = bytesToWrite;
        debug trace ("4 bytesToWrite=", bytesToWrite, " binSize=", binSize);

        if (bytesToWrite <= binSize)
        {
            debug trace ("5 Can be written in one bin");
            currHeapPage.writeBytes(heapDataPtr.offset, cellLengthBytes);
            currHeapPage.writeBytes(heapDataPtr.offset+ cast(uint)cellLengthBytes.length, data);
            debug trace ("6 Storing pageNo=currHeapPage.mPageNo");
            mDbFile.storePage(currHeapPage.mPageNo, currHeapPage.mRawBytes);
        }
        else
        {
            debug trace ("7 Must be fragmented");

            //data needs to be fragmented. It's bigger than binSize.
            size_t dataOffset = 0;
            size_t dataSegmentSize = binSize - DbPointer.sizeof - cellLengthBytes.length;

            assert (dataSegmentSize <= bytesToWrite, "dataSegmentSize to large");

            debug trace ("8 dataSegmentSize=", dataSegmentSize);

            // store cell size
            debug trace (" 9 Storing cellLengthBytes at=", heapDataPtr.offset+cast(uint)DbPointer.sizeof);
            currHeapPage.writeBytes(heapDataPtr.offset+cast(uint)DbPointer.sizeof,cellLengthBytes);

            uint dataInBinOffset = cast(uint)(heapDataPtr.offset+ cast(uint)DbPointer.sizeof + cellLengthBytes.length);
            dataBytesLeft -= cellLengthBytes.length;
            debug trace ("10 leftDataBytes=", dataBytesLeft);

            while(true)
            {
                debug trace ("11 Fragment from dataOffset=", dataOffset);

                // actual data is written after the pointer and in the case of the first segment, also cell size
                ubyte[] dataPortion = data[dataOffset..dataOffset+dataSegmentSize];
                currHeapPage.writeBytes(dataInBinOffset, dataPortion);
                debug trace ("12 Wrining fragment at dataInBinOffset=", dataInBinOffset);


                dataOffset += dataSegmentSize;

                //  dataSegmentSize should be always adapted to the actual data length
                dataBytesLeft -= dataSegmentSize;
                debug trace ("13 leftDataBytes=", dataBytesLeft, " and dataSegmentSize=", dataSegmentSize);
                if (dataBytesLeft > binSize)
                {
                    nextFreePtr.flags = nextFreePtr.flags | DbItemFlags.fragmented;
                    currHeapPage.writeBytes(heapDataPtr.offset, nativeToLittleEndian(nextFreePtr.rawData));
                    debug trace ("14 Writing at=",heapDataPtr.offset, " nextFreePtr (fragmented)=", nextFreePtr);
                }
                mDbFile.storePage(heapDataPtr.pageNo, currHeapPage.mRawBytes);

                // follow free list
                dataSegmentSize = (dataBytesLeft<=binSize) ? dataBytesLeft : binSize - DbPointer.sizeof;
                heapDataPtr = nextFreePtr;
                dataInBinOffset = heapDataPtr.offset + cast(uint)DbPointer.sizeof;
                debug trace ("15 Loading currHeapPage from nextFreePtr.pageNo=", nextFreePtr.pageNo);
                currHeapPage = mDbFile.loadPage(PageType.data, nextFreePtr.pageNo);
                uint prevOffset = nextFreePtr.offset;
                nextFreePtrBytes = currHeapPage.readBytes(nextFreePtr.offset, DbPointer.sizeof);
                nextFreePtr = DbPointer( *cast(ulong*)cast(void*)nextFreePtrBytes.ptr);
                debug trace ("16 Updating  nextFreePtr=", nextFreePtr);

                if(nextFreePtr.pageNo == PageNo.invalid)
                {
                    nextFreePtr = makeNewHeapPage(binSize);
                    debug trace ("17 Creating new HeapPage nextFreePtr=", nextFreePtr);

                    currHeapPage.writeBytes(prevOffset, nativeToLittleEndian(nextFreePtr.rawData)); 
                    debug trace ("18 Wrinting nextFreePtr at prevOffset=", prevOffset);

                }
                if (dataBytesLeft <= binSize)
                {
                    debug trace ("19 Writing last segment for dataOffset=", dataOffset);

                    //write the last portion
                    dataPortion = data[dataOffset..dataOffset+dataSegmentSize];
                    debug trace ("20 Writing last segment at heapDataPtr=", heapDataPtr);
                    currHeapPage.writeBytes(heapDataPtr.offset, dataPortion);
                    mDbFile.storePage(currHeapPage.mPageNo, currHeapPage.mRawBytes);
                    break;
                }
            }
        }
        if(nextFreePtr.pageNo == PageNo.invalid)
        {
            debug trace ("Creating new HeapPage after cell write, no pointer update");
            nextFreePtr = makeNewHeapPage(binSize);
        }
        assert(nextFreePtr.rawData, "No free bin available");
        return nextFreePtr;
    }

    unittest
    {
        debug info("Unittest [writeCellData] start");
        
        DbDataAllocator allocator = prepareDbDataAllocator(128);
        DbPage rootPage = allocator.mDbFile.loadPage(PageType.root,2);
        import std.range, std.conv, std.algorithm;
        ubyte[] cellData = 123.repeat.take(2000).map!(x=>x.to!ubyte).array;
        DbPointer dataPtr = rootPage.readTableHeader.freeDataPtr;
        dataPtr.flags = DbItemFlags.fragmented;
        allocator.writeCellData(dataPtr, rootPage.readTableHeader.binSize, cellData);
        ubyte[] cellDataOut = allocator.readCellData(dataPtr,32);
        assert(cellDataOut == cellData);
        debug info("Unittest [writeCellData] passed!");
    }

    ubyte[] readCellData(DbPointer dataPtr, uint binSize)
    {
        debug trace ("Reading at dataPtr=", dataPtr, " binSize=", binSize);

        ubyte[] cellData;
        debug trace ("Loading data pageNo=", dataPtr.pageNo);

        DbPage dataPage = mDbFile.loadPage(PageType.data, dataPtr.pageNo);

        uint lengthBytesOffset;
        if (dataPtr.flags & DbItemFlags.fragmented)
        {
            lengthBytesOffset = dataPtr.offset + cast(uint)DbPointer.sizeof;
        }
        else
        {
            lengthBytesOffset = dataPtr.offset;
        }

        ubyte[8] lengthBytes = dataPage.readBytes(lengthBytesOffset, 8);
        const size_t bytesToRead = cast(size_t)littleEndianToNative!ulong(lengthBytes);
        debug trace ("bytesToRead=", bytesToRead, " lengthBytesOffset=",lengthBytesOffset);

        size_t dataLeft = bytesToRead;
        cellData.length = bytesToRead;
        if ( (bytesToRead + lengthBytes.length) <= binSize)
        {
            debug trace ("Reading unfragmented at", dataPtr.offset + cast(uint)lengthBytes.length);
            cellData = dataPage.readBytes(dataPtr.offset + cast(uint)lengthBytes.length, bytesToRead);
        }
        else
        {
            ubyte[] nextDataPtrBytes = dataPage.readBytes(dataPtr.offset, DbPointer.sizeof);

            DbPointer nextPtr = DbPointer( *cast(ulong*)cast(void*)nextDataPtrBytes.ptr);
            debug trace ("Updating nextPtr=",nextPtr);

            size_t dataOffset = 0;
            // if data length is bigger than binSize then dataOffsetEnd should be inside data array
            size_t dataSegmentSize = binSize - DbPointer.sizeof - lengthBytes.length;
            dataPtr.offset = cast(uint)(dataPtr.offset + lengthBytes.length);
            while(true)
            {
                debug trace ("Reading bytes from  dataOffset=",dataOffset, " at dataPtr=", dataPtr.offset+cast(uint)DbPointer.sizeof, " with dataSegmentSize=", dataSegmentSize);

                cellData[dataOffset..dataOffset+dataSegmentSize] = dataPage.readBytes(dataPtr.offset+cast(uint)DbPointer.sizeof, cast(uint)dataSegmentSize);
                debug trace("Reading bytes=", cellData[dataOffset..dataOffset+dataSegmentSize]);
                dataOffset += dataSegmentSize;
                dataLeft -= dataSegmentSize;

                dataPtr = nextPtr;
                debug trace ("Loading page dataPtr.pageNo=",dataPtr.pageNo);
                dataPage = mDbFile.loadPage(PageType.data, dataPtr.pageNo);
                nextDataPtrBytes = dataPage.readBytes(dataPtr.offset, DbPointer.sizeof);
                nextPtr = DbPointer( *cast(ulong*)cast(void*)nextDataPtrBytes.ptr);
                debug trace ("Updating nextPtr=",nextPtr);

                if (dataLeft < binSize)
                {
                    debug trace ("Loading last page dataPtr=",dataPtr);

                    dataPage = mDbFile.loadPage(PageType.data, dataPtr.pageNo);
                    cellData[dataOffset..dataOffset+dataLeft] = dataPage.readBytes(dataPtr.offset, cast(uint)dataLeft);
                    break;
                }
                dataSegmentSize = binSize - DbPointer.sizeof;
            }
        }

        return cellData;
    }
    unittest
    {
        debug info("Unittest [readCellData] start");
        DbDataAllocator allocator = prepareDbDataAllocator(128);
        DbPage rootPage = allocator.mDbFile.loadPage(PageType.root,2);
        import std.range, std.conv, std.algorithm;
        ubyte[] cellData = 123.repeat.take(300).map!(x=>x.to!ubyte).array;
        DbPointer dataPtr = rootPage.readTableHeader.freeDataPtr;
        dataPtr.flags = DbItemFlags.fragmented;
        allocator.writeCellData(dataPtr, rootPage.readTableHeader.binSize, cellData);

        ubyte[] cellDataOut = allocator.readCellData(dataPtr,rootPage.readTableHeader.binSize);
        assert(cellDataOut == cellData);
        debug info("Unittest [readCellData] passed!");
    }
}

struct DbNavigator
{
    DbFile* mDbFile;
    DbPageAllocator* mPageAllocator;

    DbPage slotPageForNewItem(DbPage rootPage, ulong itemId)
    {
        assert(itemId);
        uint slotPageNo = 0;
        uint lookupLevels = lookupTreeLevelCount(itemId);
        uint lookupIdx = lookupIndex(itemId,lookupLevels,lookupLevels);

        DbPage leafLookupPage;

        //Firstly check if a new slot page is needed.
        if (isSlotPageAllocNeeded(itemId))
        {
            debug trace("isSlotPageAllocNeeded==true");

            slotPageNo = mPageAllocator.reserveFreePage();
            debug trace("slotPageNo=", slotPageNo);
            leafLookupPage = expandLookupTree(rootPage, itemId);
            debug trace("leafLookupPageNo=", leafLookupPage.mPageNo);
            leafLookupPage.writeLookupPointer(lookupIdx,slotPageNo);
            mDbFile.storePage(leafLookupPage.mPageNo, leafLookupPage.mRawBytes);
        }
        else
        {
            leafLookupPage = findLeafLookupPage(rootPage, itemId, lookupLevels);
            slotPageNo = leafLookupPage.readLookupPointer(lookupIdx);
        }
        debug trace("rootPage=", rootPage.mPageNo," slotPageNo=", slotPageNo, " itemId=", itemId);

        return mDbFile.loadPage(PageType.slot, slotPageNo);
    }
    unittest
    {
        debug info("Unittest [slotPageForNewItem] start");

        // subtest 1
        {
            //prepare
            ulong itemId = 128;
            ulong itemCount =  128;
            DbNavigator navigator = prepareDbNavigator(1024, false, itemCount, itemId);
            DbPage rootPage = navigator.mDbFile.loadPage(PageType.root, 2);
            assert(navigator.mDbFile.mPageCount == 3);
            //test
            auto page = navigator.slotPageForNewItem(rootPage,itemId+1);
            assert(navigator.mDbFile.mPageCount == 4);
        }

        // subtest 2
        {
            //prepare
            ulong itemCount =  32000;
            ulong itemId = 32000;
            
            DbNavigator navigator = prepareDbNavigator(1024, false, itemCount, itemId);
            DbPage rootPage = navigator.mDbFile.loadPage(PageType.root, 2);
            assert(navigator.mDbFile.mPageCount == 3);
            //test
            auto page = navigator.slotPageForNewItem(rootPage,itemId+1);
            assert(page.mPageNo == 4);
            assert(navigator.mDbFile.mPageCount == 5);
        }

        // subtest 3 32769
        {
            ulong itemCount =  32768;
            ulong itemId = 32768;
            DbNavigator navigator = prepareDbNavigator(1024, false, itemCount, itemId);
            auto rootPage = navigator.mDbFile.loadPage(PageType.root, 2);
            //test
            auto page = navigator.slotPageForNewItem(rootPage,itemId+1);
            
            assert(page.mPageNo == 5);
        }

        debug info("Unittest [slotPageForNewItem] passed!");
    }

private:

    DbPage expandLookupTree(DbPage rootPage, ulong itemCount)
    {
        if (itemCount == 1) return rootPage;

        auto currentLevels = lookupTreeLevelCount(itemCount-1);
        auto newLevels = lookupTreeLevelCount(itemCount);

        if (newLevels > currentLevels)
        {
            uint newLookupPageNo = mPageAllocator.reserveFreePage();
            DbPage newLookupPage = mDbFile.loadPage(PageType.lookup, newLookupPageNo);

            auto endIndex = lookupIndex(itemCount-1,1,currentLevels) +1;
            newLookupPage.mRawBytes[0..endIndex*uint.sizeof] = 
                rootPage.mRawBytes[rootPage.mPayloadOffset..rootPage.mPayloadOffset+endIndex*uint.sizeof];
            
            mDbFile.storePage(newLookupPage.mPageNo, newLookupPage.mRawBytes);

            rootPage.writeLookupPointer(0,newLookupPageNo);
            mDbFile.storePage(rootPage.mPageNo, rootPage.mRawBytes);
        }

        auto levelForUpdate = startingUpdateLevel(itemCount, newLevels);
        DbPage newLeafLookupPage;

        if(levelForUpdate > 0)
        {
            // first step - no new page is needed
            DbPage lookupPageToChange = findLookupPage(rootPage, itemCount, levelForUpdate, newLevels);
            newLeafLookupPage = lookupPageToChange;
            ++levelForUpdate;

            while (levelForUpdate <= newLevels)
            {
                auto previousLookupIndex = lookupIndex(itemCount,levelForUpdate-1,newLevels);
                auto pageNo = mPageAllocator.reserveFreePage();
                //always lookup page type as the root page is excluded by incrementing "levelForUpdate"
                newLeafLookupPage = mDbFile.loadPage(PageType.lookup, pageNo);
                lookupPageToChange.writeLookupPointer(cast(uint)previousLookupIndex, pageNo);
                mDbFile.storePage(lookupPageToChange.mPageNo, lookupPageToChange.mRawBytes);
                lookupPageToChange = newLeafLookupPage;
                ++levelForUpdate;
            }
        }
        else
        {
            newLeafLookupPage = findLeafLookupPage(rootPage, itemCount, currentLevels);
        }
        return newLeafLookupPage;    
    }
    unittest
    {
        debug info("Unittest [expandLookupTree] start");

        //subtest 1
        {
            ulong itemCount =  128;
            ulong itemId = 129;
            DbNavigator navigator = prepareDbNavigator(1024, false, itemCount, itemId);
            auto rootPage = navigator.mDbFile.loadPage(PageType.root, 2);
            //test
            auto page = navigator.expandLookupTree(rootPage,itemId);
            assert(page.mPageNo == 2);
        }

        // subtest 2 
        {
            ulong itemCount =  32000;
            ulong itemId = 32000;
            DbNavigator navigator = prepareDbNavigator(1024, false, itemCount, itemId);
            auto rootPage = navigator.mDbFile.loadPage(PageType.root, 2);
            //test
            auto page = navigator.expandLookupTree(rootPage,itemId+1);

            assert(page.mPageNo == 4);
        }

        // subtest 3 32385
        {
            ulong itemCount =  32128;
            ulong itemId = 32128;
            DbNavigator navigator = prepareDbNavigator(1024, false, itemCount, itemId);
            auto rootPage = navigator.mDbFile.loadPage(PageType.root, 2);
            //test
            auto page = navigator.expandLookupTree(rootPage,itemId+1);
            
            assert(page.mPageNo == 3);
        }

        // subtest 4 32769
        {
            ulong itemCount =  32768;
            ulong itemId = 32768;
            DbNavigator navigator = prepareDbNavigator(1024, false, itemCount, itemId);
            auto rootPage = navigator.mDbFile.loadPage(PageType.root, 2);
            //test
            auto page = navigator.expandLookupTree(rootPage,itemId+1);
            
            assert(page.mPageNo == 5);
        }

        debug info("Unittest [expandLookupTree] passed!");
    }

    void collapseLookupTree(DbPage rootPage, ulong newItemCount)
    {
        auto currentItemCount = newItemCount+1;
        auto currentLevels = lookupTreeLevelCount(currentItemCount);

        auto newLevels = lookupTreeLevelCount(newItemCount);
        auto index = lookupIndex(currentItemCount,1,currentLevels);
        uint pageNoToRelease = PageNo.invalid;

        if (newLevels < currentLevels)
        {
            auto endIndex = lookupIndex(newItemCount,2,currentLevels) + 1;
            uint delLookupPageNo = rootPage.readLookupPointer(0);
            DbPage delLookupPage = mDbFile.loadPage(PageType.lookup, delLookupPageNo);

            rootPage.mRawBytes[rootPage.mPayloadOffset..rootPage.mPayloadOffset+endIndex*uint.sizeof] = 
                delLookupPage.mRawBytes[0..endIndex*uint.sizeof];

            mDbFile.storePage(rootPage.mPageNo, rootPage.mRawBytes);
            pageNoToRelease = delLookupPage.readLookupPointer(lookupIndex(currentItemCount,2,currentLevels));

            mPageAllocator.releasePage(delLookupPage.mPageNo);
        }

        auto levelForUpdate = startingUpdateLevel(newItemCount, newLevels, false);
        if(levelForUpdate > 0)
        {
            DbPage page = findLookupPage(rootPage,currentItemCount,levelForUpdate,currentLevels);
            while (levelForUpdate < newLevels)
            {
                if(pageNoToRelease == PageNo.invalid)
                {
                    pageNoToRelease = page.readLookupPointer(lookupIndex(currentItemCount, levelForUpdate, currentLevels));
                }
                page = mDbFile.loadPage(PageType.lookup, pageNoToRelease);
                mPageAllocator.releasePage(pageNoToRelease);
                pageNoToRelease = PageNo.invalid;
                ++levelForUpdate;
            }
        }
    }
    unittest
    {
        debug info("Unittest [collapseLookupTree] start");
        
        //subtest 1
        {
            ulong itemCount =  417;
            ulong itemId = 417;
            DbNavigator navigator = prepareDbNavigator(128, false, itemCount, itemId);
            DbPage rootPage = navigator.mDbFile.loadPage(PageType.root, 2);

            // Write some values in lookup page for verification
            DbPage lookupPageLevel2 = navigator.mDbFile.loadPage(PageType.lookup, 3);
            lookupPageLevel2.writeLookupPointer(navigator.lookupIndex(1,2,2),77);
            lookupPageLevel2.writeLookupPointer(navigator.lookupIndex(416,2,2),88);
            navigator.mDbFile.storePage(3,lookupPageLevel2.mRawBytes);
            navigator.collapseLookupTree(rootPage,itemId-1);

            rootPage = navigator.mDbFile.loadPage(PageType.root, 2);
            assert(rootPage.readLookupPointer(navigator.lookupIndex(416,2,2)) == 88);
        }

        
        //subtest 2
        {          
            ulong itemCount =  1281;
            ulong itemId = 1281;
            DbNavigator navigator = prepareDbNavigator(64, false, itemCount, itemId);
            auto rootPage = navigator.mDbFile.loadPage(PageType.root, 2);
            
            // Write some values in lookup page for verification
            DbPage lookupPageLevel2 = navigator.mDbFile.loadPage(PageType.lookup, 3);
            lookupPageLevel2.writeLookupPointer(navigator.lookupIndex(1,2,3),77);
            lookupPageLevel2.writeLookupPointer(navigator.lookupIndex(1280,2,3),88);
            navigator.mDbFile.storePage(3,lookupPageLevel2.mRawBytes);
            navigator.collapseLookupTree(rootPage,itemId-1);

            rootPage = navigator.mDbFile.loadPage(PageType.root, 2);
            assert(rootPage.readLookupPointer(navigator.lookupIndex(1280,1,2)) == 88);
        }

        //subtest 3
        {           
            ulong itemCount =  1665;
            ulong itemId = 1665;
            DbNavigator navigator = prepareDbNavigator(64, false, itemCount, itemId);
            auto rootPage = navigator.mDbFile.loadPage(PageType.root, 2);
            
            // Write some values in lookup page for verification
            DbPage lookupPageLevel2 = navigator.mDbFile.loadPage(PageType.lookup, 3);
            lookupPageLevel2.writeLookupPointer(navigator.lookupIndex(1,2,3),77);
            lookupPageLevel2.writeLookupPointer(navigator.lookupIndex(1664,2,3),88);
            navigator.mDbFile.storePage(3,lookupPageLevel2.mRawBytes);
            navigator.collapseLookupTree(rootPage,itemId-1);
            lookupPageLevel2 = navigator.mDbFile.loadPage(PageType.lookup, 3);
            assert(lookupPageLevel2.readLookupPointer(navigator.lookupIndex(1664,2,3)) == 88);
        }

        debug info("Unittest [collapseLookupTree] passed!");
    }

    
    DbPage findDbSlotPage(DbPage rootPage, ulong itemId)
    {

        uint slotPageIndex = cast(uint)((itemId-1) / mDbFile.slotsPerPage) % mDbFile.lookupPointersPerPage;
        DbTableHeader tableHeader = rootPage.readTableHeader;
        auto levels = lookupTreeLevelCount(tableHeader.itemCount);

        DbPage leafLookupPage = findLeafLookupPage(rootPage, itemId, levels);

        auto slotPage = leafLookupPage.readLookupPointer(slotPageIndex);

        return mDbFile.loadPage(PageType.slot, slotPage);
    }

    unittest
    {
        debug info("Unittest [findDbSlotPage] start");
        // subtest 1
        {
            ulong itemCount =  32257;
            ulong itemId = 32257;
            DbNavigator navigator = prepareDbNavigator(1024, false, itemCount, itemId);
            auto rootPage = navigator.mDbFile.loadPage(PageType.root, 2);
            //test
            auto page = navigator.findDbSlotPage(rootPage,itemId);

            assert(page.mPageNo == 4);
        }

        
        // subtest 1
        {
            ulong itemCount =  32257;
            ulong itemId = 1;
            DbNavigator navigator = prepareDbNavigator(1024, false, itemCount, itemId);
            auto rootPage = navigator.mDbFile.loadPage(PageType.root, 2);
            //test
            auto page = navigator.findDbSlotPage(rootPage,1);
            
            assert(page.mPageNo == 4);
        }

        // subtest 3
        {
            ulong itemCount =  32769;
            ulong itemId = 32769;
            DbNavigator navigator = prepareDbNavigator(1024, false, itemCount, itemId);
            auto rootPage = navigator.mDbFile.loadPage(PageType.root, 2);
            //test
            auto page = navigator.findDbSlotPage(rootPage,itemId);
            
            assert(page.mPageNo == 4);
        }

        debug info("Unittest [findDbSlotPage] passed!");
    }

    DbPage findLeafLookupPage(DbPage rootPage, ulong itemId, uint levels)
    {
        assert(itemId);
        return findLookupPage(rootPage,itemId,levels,levels);
    }

    DbPage findLookupPage(DbPage rootPage, ulong itemId, uint level,uint levels)
    {  
        DbPage lookupPage = rootPage;
        uint currentLevel = 1;
        while(currentLevel < level)
        {
            auto lookupIndex = lookupIndex(itemId,currentLevel,levels);
            auto lookupPageNo = lookupPage.readLookupPointer(lookupIndex);
            lookupPage = mDbFile.loadPage(PageType.lookup,lookupPageNo);
            ++currentLevel;
        }
        
        return lookupPage;
    }
    unittest
    {
        debug info("Unittest [findLookupPage] start");

        //prepare
        ulong itemId = 1;
        ulong itemCount =  512;
        DbNavigator navigator = prepareDbNavigator(128, true, itemCount, itemId);
        auto masterPage = navigator.mDbFile.loadPage(PageType.master, PageNo.master);
        auto levels = 2;

        
        //test
        auto page = navigator.findLookupPage(masterPage,itemId,1,levels);
        assert(page.mPageNo == PageNo.master);

        page = navigator.findLookupPage(masterPage,itemId,2,levels);
        assert(page.mPageNo == 2);

        debug info("Unittest [findLookupPage] passed!");
    }

    /**
     * Computes the index of lookup pointer at specific tree level
     * 
     * Params: 
     *      itemId = Item for which the lookup index is computed
     *      level = Tree level for which the lookup index is computed
     *      levelCount = Level depth of the whole LookupTree
     * Returns: 
     *      Index of lookup pointer for specific item on selected tree level
     */
    uint lookupIndex(ulong itemId, uint level, uint levelCount)
    {
        return cast(uint)(((itemId-1) / (mDbFile.slotsPerPage*(mDbFile.lookupPointersPerPage^^(levelCount-level)))) % mDbFile.lookupPointersPerPage);
    }
    unittest
    {
        debug info("Unittest [lookupIndex] start");
        {
            DbNavigator navigator;
            navigator.mDbFile = new DbFile("", DbParams(128, DbFlags.memoryStorage));

            assert(navigator.lookupIndex(1,1,2)==0);
            assert(navigator.lookupIndex(512,3,3)==31);
            assert(navigator.lookupIndex(512,2,3)==0);
            assert(navigator.lookupIndex(512,1,3)==0);

            assert(navigator.lookupIndex(513,3,3)==0);
            assert(navigator.lookupIndex(513,2,3)==1);

            assert(navigator.lookupIndex(14337,1,3)==0);
            assert(navigator.lookupIndex(14337,2,3)==28);
            assert(navigator.lookupIndex(14337,3,3)==0);
        }

        {
            DbNavigator navigator;
            navigator.mDbFile = new DbFile("", DbParams(1024, DbFlags.memoryStorage));
            assert(navigator.lookupIndex(128,1,1)==0);
            assert(navigator.lookupIndex(129,1,1)==1);

        }
        debug info("Unittest [lookupIndex] passed!");
    }

    uint lookupTreeLevelCount(ulong itemCount)
    {
        if(itemCount==0) return 1;
        auto rest = (itemCount - 1)/mDbFile.slotsPerPage;
        auto levels = 0;
        auto spaceForLookupPointers = mDbFile.mPageSize - DbTableHeader.sizeof;
        auto maxPointers = spaceForLookupPointers / uint.sizeof;
        do
        {
            levels++;
            // rootPage has smaller number of lookup pointers
            if( ((rest+1) > maxPointers) && (rest < mDbFile.lookupPointersPerPage))
            {
                levels++;
            }
            rest /= mDbFile.lookupPointersPerPage;
        }while (rest);

        return levels;
    }
    unittest
    {
        debug info("Unittest [lookupTreeLevelCount] start");

        {
            DbNavigator navigator;
            navigator.mDbFile = new DbFile("", DbParams(128, DbFlags.memoryStorage));
            assert(navigator.lookupTreeLevelCount(1)==1);
            assert(navigator.lookupTreeLevelCount(416)==1);
            assert(navigator.lookupTreeLevelCount(417)==2);
            assert(navigator.lookupTreeLevelCount(512)==2);
            assert(navigator.lookupTreeLevelCount(513)==2);
            assert(navigator.lookupTreeLevelCount(13312)==2);
            assert(navigator.lookupTreeLevelCount(13313)==3);
            assert(navigator.lookupTreeLevelCount(16384)==3);
            assert(navigator.lookupTreeLevelCount(16385)==3);
        }

        {
            DbNavigator navigator;
            navigator.mDbFile = new DbFile("", DbParams(1024, DbFlags.memoryStorage));
            assert(navigator.lookupTreeLevelCount(1)==1);
            assert(navigator.lookupTreeLevelCount(31999)==1);
            assert(navigator.lookupTreeLevelCount(32000)==1);
            assert(navigator.lookupTreeLevelCount(32001)==2);
            assert(navigator.lookupTreeLevelCount(32768)==2);
            assert(navigator.lookupTreeLevelCount(32769)==2);
            assert(navigator.lookupTreeLevelCount(32770)==2);
        }
        debug info("Unittest [lookupTreeLevelCount] passed!");

    }

    /**
     * Computes the starting level of LookupTree at which changes needs to be done.
     * Lower levels should be adapted as well
     * 
     * Params: 
     *      newItemCount = Target number of items
     *      treeLevels = Number of LookupTree levels
     *      growth = Indicates if it is addition or deletion of item
     * Returns: 
     *      LookupTree level for which changes needs to be applied
     */
    int startingUpdateLevel(ulong newItemCount, uint treeLevels, bool growth = true)
    {
        assert(treeLevels);
        auto subTreeSize = mDbFile.slotsPerPage*mDbFile.lookupPointersPerPage^^(treeLevels-1);
        auto updateLevel = 0;
        auto rest = (growth) ? 1 : 0;
        for (auto level =1; level <= treeLevels; ++level)
        {
            if ((newItemCount % subTreeSize) == rest)
            {
                updateLevel=level;
                break;
            }
            subTreeSize /= mDbFile.lookupPointersPerPage;
        }
        
        return updateLevel;
    }
    unittest
    {
        debug info("Unittest [startingUpdateLevel] start");

        {
            DbNavigator navigator;
            navigator.mDbFile = new DbFile("", DbParams(128, DbFlags.memoryStorage));

            assert(navigator.startingUpdateLevel(1,1)==1);
            assert(navigator.startingUpdateLevel(2,1)==0);
            assert(navigator.startingUpdateLevel(17,1)==1);
            assert(navigator.startingUpdateLevel(449,2)==2);
            assert(navigator.startingUpdateLevel(497,2)==2);
            assert(navigator.startingUpdateLevel(513,2)==1);
            assert(navigator.startingUpdateLevel(514,2)==0);
        }

        {
            DbNavigator navigator;
            navigator.mDbFile = new DbFile("", DbParams(1024, DbFlags.memoryStorage));

            assert(navigator.startingUpdateLevel(1,1)==1);
            assert(navigator.startingUpdateLevel(32384,2)==0);
            assert(navigator.startingUpdateLevel(32385,2)==2);
        }
        debug info("Unittest [startingUpdateLevel] passed!");

    }

    bool isSlotPageAllocNeeded(ulong itemId)
    {
        return (itemId % (mDbFile.slotsPerPage) == 1);
    }

    bool isSlotPageDeallocNeeded(ulong itemId)
    {
        return (itemId % (mDbFile.slotsPerPage) == 0);
    }
}

version(unittest)
{

    DbNavigator prepareDbNavigator(uint pageSize, bool isMasterTable, ulong itemCount, ulong pathItemId)
    {
        DbFile* dbFile = new DbFile("", DbParams(pageSize, DbFlags.memoryStorage));
        DbPageAllocator* pageAllocator = new DbPageAllocator(dbFile);
        DbNavigator navigator = DbNavigator(dbFile, pageAllocator);

        auto levels = navigator.lookupTreeLevelCount(itemCount);

        //Master page
        auto currentPageNo = pageAllocator.reserveFreePage();
        assert(currentPageNo == 1);
        DbPage masterPage = dbFile.loadPage(PageType.master, currentPageNo);
        masterPage.writeMasterHeader(DbMasterHeader(magicString,pageSize,PageNo.invalid));
        masterPage.writeTableHeader(DbTableHeader(itemCount));
        dbFile.storePage(masterPage.mPageNo, masterPage.mRawBytes);

        if(!isMasterTable)
        {
            currentPageNo = pageAllocator.reserveFreePage();
            DbPage rootPage = dbFile.loadPage(PageType.root, currentPageNo);
            rootPage.writeTableHeader(DbTableHeader(itemCount,DbPointer(0xFFFF_FFFF_FFFF_FFFF)));
            dbFile.storePage(rootPage.mPageNo, rootPage.mRawBytes);
        }

        // Create lookup path for requested itemCount
        for(auto i=1; i <= levels; ++i)
        {
            auto pageType = (currentPageNo==1) ? PageType.master : PageType.root;
            if (i > 1)
            {
                pageType = PageType.lookup;
            }

            DbPage page = dbFile.loadPage(pageType, currentPageNo);

            auto nextPageNo = pageAllocator.reserveFreePage();
            auto index = navigator.lookupIndex(pathItemId, i, levels);
            page.writeLookupPointer(index, nextPageNo);

            dbFile.storePage(currentPageNo, page.mRawBytes);
            currentPageNo = nextPageNo;
        }

        return navigator;
    }

    DbPageAllocator prepareDbPageAllocator(uint pageSize)
    {
        DbFile* dbFile = new DbFile("", DbParams(pageSize, DbFlags.memoryStorage));
        dbFile.appendPages(2);
        DbPageAllocator allocator = DbPageAllocator(dbFile);
        return allocator;
    }

    DbDataAllocator prepareDbDataAllocator(uint pageSize)
    {
        //page:1=master, page:2=root, page:3=data
        debug trace ("Creating DbFile in memory");
        DbFile *dbFile = new DbFile("", DbParams(pageSize, DbFlags.memoryStorage));
        debug trace ("Appending 2 pages");
        dbFile.appendPages(2);

        debug trace ("Creating DbPageAllocator");
        DbPageAllocator *pageAlloc = new DbPageAllocator(dbFile);
        debug trace ("Creating DbDataAllocator");
        DbDataAllocator dataAlloc = DbDataAllocator(dbFile, pageAlloc);


        DbPointer dataHeapPointer = dataAlloc.makeNewHeapPage(minimalBinSize);
        debug trace("Creating makeNewHeapPage at DbPointer=", dataHeapPointer);

        DbPage rootPage = dbFile.loadPage(PageType.root, 2);
        debug trace ("Updateing  root page no. 2 with freeDataPtr=(offset=32,pageNo=6)");
        //DbPointer pointer;
        //pointer.offset(32);
        //pointer.pageNo(6);
        DbTableHeader tableHeader;
        tableHeader.freeDataPtr = dataHeapPointer; // pageNo=3,offset=0
        rootPage.writeTableHeader(tableHeader);
        dbFile.storePage(rootPage.mPageNo, rootPage.mRawBytes);

        return dataAlloc;
    }
}