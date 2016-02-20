window.onload = function() {

Vue.use(VueResource); // ?
// var onSiteUserGroupName = 'guestmenu';

var GuestMenu = Vue.extend({
    // props : ['username','password'],
      template: `
        <div id="auth">
            <form class="form-inline pull-right">
                <div class="form-group">
                    <label class="sr-only" for="UserName">User name</label>
                  <input type="username" v-model="username" class="form-control" id="UserName" placeholder="username">
                </div>
                <div class="form-group">
                  <label class="sr-only" for="Password">Password</label>
                  <input type="password" v-model="password" class="form-control" id="Password" placeholder="Password">
                </div>
              <button type="submit" class="btn btn-default" v-on:click="sendLoginInfo()">Войти</button>
            </form>
        </div>`,
        data: function()
        {
          return{
              username: "",
              password: ""
            };
        },
        methods: {

          sendLoginInfo()
          {
              var loginData = new Object();
              //data that we take from input
              loginData["username"] = this.username;
              loginData["password"] = this.password;

              console.log("site login/password: ", loginData); 

              this.$http.post('http://127.0.0.1:8080/login', loginData).then(function (response) {
                console.log("server response: ", response.data)
                if(response.data["isAuthorized"] == true && response.data["isAdmin"] == false)
                {
                  console.log("We are Authorized --> isAuthorized == true");
                  // onSiteUserGroupName = "usermenu";
                  App.topMenuView = 'usermenu' //Change current view!
                  userLoginNotification("Welcome, " + loginData["username"], "Login success"); // notificate user
                }

                else if(response.data["isAuthorized"] == true && response.data["isAdmin"] == true)
                {
                  console.log("We are Authorized --> isAuthorized == true");
                  // onSiteUserGroupName = "usermenu";
                  App.topMenuView = 'adminmenu' //Change current view!
                  userLoginNotification("Welcome, " + loginData["username"], "Login success"); // notificate admin
                }

                else if(response.data["status"] == "wrongPassword")
                {
                  console.log("User: " + loginData["username"] + " have wrong password");
                  // onSiteUserGroupName = "usermenu";
                  userLoginNotification("Wrong password for user: " + loginData["username"], "Login failed");
                }                

                else if(response.data["status"] == "userDoNotExists")
                {
                  console.log("User: " + loginData["username"] + " do not exists in DB");
                  // onSiteUserGroupName = "usermenu";
                  userLoginNotification("User: " + loginData["username"] + " do not exists on DB", "Login failed");
                }

                else
                {
                  console.log("Server status: " + response.data["status"]);
                  userLoginNotification("Server status: " + response.data["status"], "Alert!");
                }

              },

              function(response)
              {
                console.log("Error on server code: ", response.status)
              }
              );

          }
        }


          });

 var UserMenu = Vue.extend({
      template: `
              <ul class="nav nav-tabs">
                <li role="user" class="active"><a href="#">USER</a></li>
                <li role="user"><a href="#">USER</a></li>
                <li role="user"><a href="#">USER</a></li>
                <li class="form-inline pull-right"><button type="submit" class="btn btn-default" v-on:click="logout()">Выйти</button> </li>
              </ul> 

          `});     

 var AdminMenu = Vue.extend({
      template: `
              <ul class="nav nav-tabs">
                <li role="admin" class="active"><a href="#">Admin</a></li>
                <li role="admin"><a href="#">Admin</a></li>
                <li role="admin"><a href="#">Messages</a></li>
                <li class="form-inline pull-right"><button type="submit" class="btn btn-default" v-on:click="logout()">Выйти</button> </li>
              </ul>
               
          `});

            
Vue.component('guestmenu', GuestMenu);
Vue.component('usermenu', UserMenu);
Vue.component('adminmenu', AdminMenu);


var App = new Vue ({
   el: '#app',
  // template: '<usermenu></usermenu>',
  data: 
    {
      topMenuView: "guestmenu",
    }
  })




// var router = new VueRouter();




// router.map({
//   '/foo': {
//     component: GuestMenu
//   },
//   '/bar': {
//     component: UserMenu
//   }
// })


// router.start(App, '#app')


// Vue.http.get('http://127.0.0.1:8080/login').then(function (response) {});


}