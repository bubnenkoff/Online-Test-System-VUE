module users;

struct User 
{
	int id;
	string login;
	string password;
	string usergroup;
	string organization;
	string email;
	string [] passedtests;
}

struct AuthInfo
{
    User user; //User structure

    bool isAuthorized;
    bool isAdmin;
    bool passwordOK; //set to true if password from login == password from DB

}

