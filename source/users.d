module users;

struct AuthInfo
{
    User user; //User structure
    bool isAuthorized;
    bool isAdmin;
    bool passwordOK; //set to true if password from login == password from DB
    struct User 
	{
		int id;
		string username;
		string organization;
		string email;
	}
}

