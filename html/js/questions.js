// seems it's not calling. there is dublicate in pagecontent
function getQuestionsContent() {
    
    // global Vue object
    Vue.http.get('http://127.0.0.1:8080/js/questions.json').then(function(response)  {
    // also this function should fill name of current test in App
	console.log("---------------------------------------------------------------");
    console.log(response.data.username);
	console.log("---------------------------------------------------------------");
    //console.log(response.data)
    return response.data;
    }
    	);  
    
}
