"use strict";

var guestContent = Vue.extend({
      template: `

        <p>Guest content</p>
      <div v-for="question in questions">
       <template v-if="question.isEnabled">
          <h3 v-if="question.isEnabled">{{question.question}}</h3>

          <!-- First Level -->
            <div v-for="firstLevelAnswer in question.answers">                
              <label v-if="!question.isRadioButton"><span class="firstLevelAnswer"><input type="checkbox" class="big-checkbox" v-model="firstLevelAnswer.isSelected"/>{{firstLevelAnswer.answer}}</span></label>
              <label v-if="question.isRadioButton"><span class="firstLevelAnswer"><input type="radio" class="big-checkbox" name="myvalue"/>{{firstLevelAnswer.answer}}</span></label>
              <span v-if="firstLevelAnswer.isTextInput"><input type="text"/></span>
                   |  firstLevelAnswer.isSelected: {{firstLevelAnswer.isSelected}}  

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
          var x=1;

          },
          ready() 
          { 
            this.getQuestionsContent()
            
          },

          watch: 
          {
              questions : 
             {
                handler: function(val, oldVal) {
                  this.foo();
                },
                  deep: true
                
              },
            
          },


          methods: 
          {
             
             getQuestionsContent()
             {
                this.$http.get('http://127.0.0.1:8080/js/questions.json').then(function(response)
                {
                  this.questions = response.data;
                 // this.setIsSelectedToTrue();
                }); 
              
              },

              
              foo()
              {
                console.log("dfd");
                console.log("dfdsss");
              },

              setIsSelectedToTrue()
              {
                //console.log(this.$get('questions'));
                for (var question of this.questions)
                {
                    //console.log(question.answers); // array of answers
                    for(var answer of question.answers)
                    {
                      //this.$set('answer.isSelected', true);
                      console.log('Before: ', answer.isSelected);
                     // this.$set('answer.isSelected', true); //warining: You are setting a non-existent path "answer.isSelected"
                      answer.isSelected = true;  // so look like I should to do like this to change value
                      console.log('After: ', answer.isSelected);
                    }
                }
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