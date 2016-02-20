function sendLoginInfo()
  {
      var loginData = new Object();
      //data that we take from user input
      loginData["username"] = this.username;
      loginData["password"] = this.password;

      console.log("site login/password: ", loginData); 

      this.$http.post('http://127.0.0.1:8080/login', loginData).then(function (response) {
        console.log("server response: ", response.data)

        if(response.data["status"] == "success") // process only if we got status=success
        {
	        //Admin
	        if(response.data.login.isAuthorized == true && response.data.login.isAdmin == true)
	        {
	          console.log("We are Authorized: isAuthorized == true");
	          App.topMenuView = 'adminmenu'; //Change current view!
	          App.contentView = 'admincontent';
	          userLoginNotification("Welcome, " + loginData["username"], "Login success"); // notificate admin
	        }
	        //User
	        else if(response.data.login.isAuthorized == true && response.data.login.isAdmin == false)
	        {
	          console.log("User is Authorized: isAuthorized == true");
	          App.topMenuView = 'usermenu' //Change current view!
	          App.contentView = 'usercontent';
	          userLoginNotification("Welcome, " + loginData["username"], "Login success"); // notificate user
	        }
	     }

	    if(response.data.status == "fail")
        {
          if (response.data.login.username == "userDoNotExists")
          {
          	console.log("User: " + loginData["username"] + " do not exists");
          	userLoginNotification(`User "` + loginData["username"] + `" do not exists `, `Login failed`);	
          }

          else if (response.data.login.password == "wrongPassword")
          {
          	console.log("Wrong password for user: " + loginData["username"]);
          	userLoginNotification("Wrong password for user: " + loginData["username"], "Login failed");	
          }

        } 


	    },

	      function(response)
	      {
	        console.log("Error on server code: ", response.status) // from server, not JSON code
	      }
	 );
		

  }


function checkAuth()
{
	// we should NOT send any data like: loginData because after refreshing page
	// all filds are empty and we need to ask server if he have authorize session

  console.log("Checking if user already have active session"); 

	this.$http.post('http://127.0.0.1:8080/checkAuthorization').then(function (response) {
	     console.log("server response: ", response.data)

	     if(response.data["status"] == "success") // process only if we got status=success
	     {
	        //Admin
	        if(response.data.login.isAuthorized == true && response.data.login.isAdmin == true)
	        {
	          console.log("We are already authorized on site as admin (F5 even accure)");
	          App.topMenuView = 'adminmenu' //Change current view!
	          App.contentView = 'admincontent';
	          this.username = response.data.login.username; // get user name from response and set it to {{username}}
	        }
	        //User
	        else if(response.data.login.isAuthorized == true && response.data.login.isAdmin == false)
	        {
	          console.log("We are already authorized on site as user (F5 even accure)");
	          App.topMenuView = 'usermenu' //Change current view!
	          App.contentView = 'usercontent';
	          this.username = response.data.login.username; // get user name from response and set it to {{username}}
	        }
	     }

	    if(response.data.status == "fail")
	     {
	       console.log("User do not Authorizated!");
	     } 

	    },

	      function(response)
	      {
	        console.log("Error on server code: ", response.status) // from server, not JSON code
	      }
	 );

}


  function logout()
  {
  	  var loginData = new Object();
      //data that we take from user input
      loginData["username"] = this.username; // username more then enough
	  console.log("Logout username -> " + loginData["username"]);
	  console.log(loginData);
	  console.log("-------------------------");

	this.$http.post('http://127.0.0.1:8080/logout', loginData).then(function (response) {
	    console.log("server response: ", response.data)
	    if(response.data["isAuthorized"] == false)
	    {
	      console.log("Logout from site success");
	      App.topMenuView = 'guestmenu' //Change current view!
	      userLoginNotification("Goodbye, " + loginData["username"], "User Logout"); // notificate user
	    }
	});

  }