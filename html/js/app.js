var App = null; // it's global because function behind will overwrite it's with Vue App instance
window.onload = function() {

Vue.use(VueResource); // ?

var GuestMenu = Vue.extend({
     props : ['username','password'],
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
              <button type="submit" class="btn btn-default" v-on:click.prevent="sendLoginInfo()">Войти</button>
              <button type="submit" class="btn btn-default" v-on:click.prevent="checkAuth()">проверка входа</button>
            </form>
        </div>`,

        methods: { //hash key-value
          sendLoginInfo : sendLoginInfo, // key (anyname) | value -> calling function name (from separate file) 
          //calling without brackets because we do need return from function, we need just function

          checkAuth: checkAuth // restore authorization after refresh page if user already have session!
        },
        ready()
        {
           this.checkAuth()
        }

          });

 var UserMenu = Vue.extend({
  props : ['username'],
      template: `
              <ul class="nav nav-tabs">
                <li role="user" class="active"><a href="#">USER</a></li>
                <li role="user"><a href="#">USER</a></li>
                <li role="user"><a href="#">USER</a></li>
                <li class="form-inline pull-right"><button type="submit" class="btn btn-default" v-on:click="logout()">Выйти</button> </li>
                <li style="line-height: 35px; margin-right: 10px;" class="pull-right">Hello, <strong>{{username}}</strong></li> 
              </ul> 
          `,

        methods: { //hash key-value
          logout : logout // key (anyname) | value -> calling function name (from separate file) 
          //calling without brackets because we do need return from function, we need just function
        }

        });     

 var AdminMenu = Vue.extend({
  props : ['username'],
      template: `
              <ul class="nav nav-tabs">
                <li role="admin" class="active"><a href="#">Admin</a></li>
                <li role="admin"><a href="#">Admin</a></li>
                <li role="admin"><a href="#">Messages</a></li>
                <li class="form-inline pull-right"><button type="submit" class="btn btn-default" v-on:click="logout()">Выйти</button> </li>
                <li style="line-height: 35px; margin-right: 10px;" class="pull-right">Hello, <strong>admin!</strong></li> 
              </ul>`,

        methods: { //hash key-value
          logout : logout // key (anyname) | value -> calling function name (from separate file) 
          //calling without brackets because we do need return from function, we need just function
        }
               
          });

////////////////////////////////
/*
var UserContent = Vue.extend({
      template: `
             <div>
            <p>USER CONTENT</p>
             </div>
          `});
*/          
/////////////
            
Vue.component('guestmenu', GuestMenu);
Vue.component('usermenu', UserMenu);
Vue.component('adminmenu', AdminMenu);

Vue.component('guestcontent', guestContent);
Vue.component('usercontent', userContent);
Vue.component('admincontent', adminContent);

Vue.component('endPage', endPage);


App = new Vue ({ // App -- is need for overwrite global var. Global var need declarated abobe all function, because some it's function is calling from outside
   el: '#app',
  // template: '<usermenu></usermenu>',
  data: 
    {
      topMenuView: "guestmenu",
      contentView: "guestcontent",
      username: "",
      password: "",
      passedtests: [], // from DB for this IP
      currenttestName: "", // current test name
      testPassed : false

    },

  methods: {
 
  },

  ready: function()
  {
    checkAuth.call(this) //binding with stand-alone function placed in authorization.js. Without binding _checkAuth_ will not see _data_

  }

  }


  )




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