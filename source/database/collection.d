/*
 Copyright: Copyright Piotr Półtorak 2015-2016.
 License: $(WEB boost.org/LICENSE_1_0.txt, Boost License 1.0).
 Authors: Piotr Półtorak
 */
module draft.database.collection;

import draft.database.storage;


struct Collection(T)
{
    /**
     * A constructor of a Collection object. If new collection is needed zero
     * (invalid) page number should be provided. In that case an actual collection 
     * should be created by invoking createCollection implicitly.
     * 
     * Params: 
     *      dbStorage = a pointer to storage object
     *      tableRootPage = id number of exsiting collection or PageNo.Null otherwise
     */
    this(DbStorage *dbStorage, uint tableRootPage)
    {
        mDbStorage = dbStorage;
        mTableRootPage = tableRootPage;

        if (tableRootPage != PageNo.invalid)
        {
            mCurrentId = dbStorage.nextDbItemId(mTableRootPage,mCurrentId);
        }
    }

    /**
     * Creates a  new collection. Must be invoked if a Collection's constructor
     * was called with tableRootPage = PageNo.Null
     * 
     * 
     * Params: 
     *      tableRootPage = id number of exsiting collection or PageNo.Null otherwise
     * Returns: 
     *      Table root page id
     */
    uint createCollection(PageType pageType)
    {
        import std.stdio;

        mTableRootPage = mDbStorage.createTable(pageType);

        return mTableRootPage;
    }

    /**
     * A range primitive "empty". Checks if a Collection is empty.
     * 
     * Params: 
     *      tableRootPage = id number of exsiting collection or PageNo.Null otherwise
     * Returns: 
     *      True if collection has no items
     */
    bool empty()
    {
        return (mCurrentId == 0);
    }

    /**
     * A range primitive "popFront". Removes the first item from the range
     */
    void popFront()
    {
        mCurrentId = mDbStorage.nextDbItemId(mTableRootPage, mCurrentId);
    }


    /**
     * A range primitive "front". Returns the item from the front of range
     * 
     * Returns: 
     *       item
     */
    T front()
    {
        return mDbStorage.fetchDbItem!T(mTableRootPage, mCurrentId);
    }


    /**
     * A range primitive "put". Puts an item into the collection.
     * 
     * Params: 
     *       item
     */
    void put(T item)
    {
        mDbStorage.addItem!T(mTableRootPage, item);
        if (!mCurrentId) ++mCurrentId;
    }

    /**
     * Updates items with given values in the collection. If no item matches
     * the oldItem, no items are updated.
     * 
     * Params: 
     *      oldItem = Old item value
     *      newItem = Old item value
     * Returns: 
     *       Number of updated items
     */
    ulong update(T oldItem, T newItem)
    {
        ulong itemId = 1;
        ulong changeCount = 0;

        while (itemId != 0)
        {
            auto item = mDbStorage.fetchDbItem!T(mTableRootPage, itemId);
            if (item == oldItem)
            {
                mDbStorage.updateItem(mTableRootPage, itemId, newItem);
                ++changeCount;
            }
            itemId = mDbStorage.nextDbItemId(mTableRootPage, itemId);
        }
        return changeCount;
    }

    /**
     * Removes items with given values from the collection. If no item matches
     * the given item value, no items are removed.
     * 
     * Params: 
     *      item = item to be removed
     * Returns: 
     *       number of removed items
     */
    ulong removeItem(T item)
    {
        if (this.empty) return 0;

        ulong removeCount = 0;

        for (ulong itemId = 1;;)
        {
            auto currItem = mDbStorage.fetchDbItem!T(mTableRootPage, itemId);
            if (currItem == item)
            {
                mDbStorage.removeItem(mTableRootPage, itemId);
                ++removeCount;
                --itemId;
            }
            itemId = mDbStorage.nextDbItemId(mTableRootPage, itemId);

            if (!itemId) break;
        }

        mCurrentId = mDbStorage.nextDbItemId(mTableRootPage, PageNo.invalid);

        return removeCount;
    }

    // TODO This method needs to be properly defined
    T opIndex(int i)
    {
        return mDbStorage.fetchDbItem!T(mTableRootPage, i+1);
    }

    /**
     * Provides the id number of the collection.
     * 
     * Returns: 
     *       collection id number
     */
    uint collectionId()
    {
        // TODO separate collectionId and rootPageId
        return mTableRootPage;
    }

private:
    uint mTableRootPage = PageNo.invalid;
    DbStorage * mDbStorage;
    long mCurrentId = 0;

}

version (none)
{
    struct Tool
    {

        static string[] memberTypes(T)()
        {
            import std.traits;

            string[] types;
            static if(is(T == struct))
            {
                foreach(idx, memberType; FieldTypeTuple!(T))
                {
                    types ~= memberType.stringof;
                }
            }
            return types;
        }
    }
        
    alias Member = Tuple!(string,DbMember);
    alias DbMember = Algebraic!(int, long, string, DbReference);
}