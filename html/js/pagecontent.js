var guestContent = Vue.extend({
      template: `

        <p>Guest content</p>
      <div v-for="question in questions">
       <template v-if="question.isEnabled">
          <h3 v-if="question.isEnabled">{{question.question}}</h3>
            <div v-for="answers in question.answers">
              <h4>{{answers.answer}}</h4>
            </div>
         </template>
      </div>  
 
    </li>

          `,
        data: function ()  {
          return {
             questions: []
          }

          },
          ready() 
          { 
            this.getQuestionsContent()
          },
          methods: 
          {
             
             getQuestionsContent()
             {
                this.$http.get('http://127.0.0.1:8080/js/questions.json').then(function(response)
                {
                  this.questions = response.data;

                }); 
              
             }

          }


        }
        );

var userContent = Vue.extend({
      template: `
              <p>SOME USER CONTENT TEST</p>
          `
        });

var adminContent = Vue.extend({
      template: `
              <p>ADMIN CONTENT TEST</p>
          `
        });