var guestContent = Vue.extend({
      template: `

        <p>Guest content</p>
      <div v-for="question in questions">
       <template v-if="question.isEnabled">
          <h3 v-if="question.isEnabled">{{question.question}}</h3>

            <!-- First Level -->
            <div v-for="firstLevelAnswer in question.answers">                
              <label v-if="!question.isRadioButton"><span class="firstLevelAnswer"><input type="checkbox" class="big-checkbox"/>{{firstLevelAnswer.answer}}</span></label>
              <label v-if="question.isRadioButton"><span class="firstLevelAnswer"><input type="radio" class="big-checkbox" name="myvalue"/>{{firstLevelAnswer.answer}}</span></label>
              <span v-if="firstLevelAnswer.isTextInput"><input type="text"/></span>
                
                  <!-- Second Level -->
                         <div v-for="secondLevelAnswer in firstLevelAnswer.answers">                         
                            <label v-if="!secondLevelAnswer.isRadioButton"><span class="secondLevelAnswer"><input type="checkbox" class="big-checkbox"/>{{secondLevelAnswer.answer}}</span></label>
                            <label v-if="secondLevelAnswer.isRadioButton"><span class="secondLevelAnswer"><input type="checkbox" class="big-checkbox"/>{{secondLevelAnswer.answer}}</span></label>                       
                               
                               <!-- Third Level -->
                               <div v-if="secondLevelAnswer.isSelected">  
                                   <div v-for="thirdLevelAnswer in secondLevelAnswer.answers">
                                      <label v-if="!thirdLevelAnswer.isRadioButton"><span class="thirdLevelAnswer"><input type="checkbox" class="big-checkbox"/>{{thirdLevelAnswer.answer}}</span></label>
                                      <label v-if="thirdLevelAnswer.isRadioButton"><span class="thirdLevelAnswer"><input type="checkbox" class="big-checkbox"/>{{thirdLevelAnswer.answer}}</span></label>                             
                                   </div>
                                </div>    
                         </div>
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