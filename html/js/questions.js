function getQuestionsContent() {
    
    // global Vue object
    Vue.http.get('http://127.0.0.1:8080/js/questions.json').then(function(response)  {
    //console.log(response.data)
    return response.data;
    }
    	);  
    
}
