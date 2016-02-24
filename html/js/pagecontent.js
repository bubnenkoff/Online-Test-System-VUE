var guestContent = Vue.extend({
      template: `

        <p>Guest content</p>
      <div v-for="question in questions">
       <template v-if="question.isEnabled">
          <h3 v-if="question.isEnabled">{{question.question}}</h3>
            <div v-for="answers in question.answers">
              <label v-if="!question.isRadioButton"><span class="answers"><input type="checkbox" class="big-checkbox"/>{{answers.answer}}</span></label>
              <label v-if="question.isRadioButton"><span class="answers"><input type="radio" class="big-checkbox" name="myvalue"/>{{answers.answer}}</span></label>
              <span v-if="answers.isTextInput"><input type="text"/></span>
                <div v-for="answers in answers.answers">
                    <label v-if="!answers.isRadioButton"><span class="subanswers"><input type="checkbox" class="big-checkbox"/>{{answers.answer}}</span></label>
                    <label v-if="answers.isRadioButton"><span class="subanswers"><input type="checkbox" class="big-checkbox"/>{{answers.answer}}</span></label>
                             <div v-for="sub in answers.suba">
                                <label v-if="!sub.isRadioButton"><span class="subsubanswers"><input type="checkbox" class="big-checkbox"/>{{sub.answer}}</span></label>
                                <label v-if="sub.isRadioButton"><span class="subsubanswers"><input type="checkbox" class="big-checkbox"/>{{sub.answer}}</span></label>                             
                             </div>

                </div>
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