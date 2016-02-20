//window.onload = function() {

function userLoginNotification(theBody,theTitle)
{
	Notification.requestPermission();

	function spawnNotification() {
	  var options = {
		  body: theBody,
		  // icon: theIcon
	  }
	  var n = new Notification(theTitle,options);
	}

	spawnNotification("theBody", "theTitle");

}

//}