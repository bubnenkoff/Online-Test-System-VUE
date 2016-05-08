"use strict";

var guestContent = Vue.extend({
  props: ['value','min','max','step'],
      template: `

        <p>Guest content</p>
      <div v-for="question in questions">
       <template v-if="question.isEnabled">
          <h3 v-if="question.isEnabled">{{question.question}}</h3>

          <!-- First Level -->
            <div v-for="firstLevelAnswer in question.answers"> 
             
              <label v-if="!question.isRadioButton"><span class="firstLevelAnswer"><input type="checkbox" class="big-checkbox" :disabled="firstLevelAnswer.isDisabled" v-model="firstLevelAnswer.isSelected" />{{firstLevelAnswer.answer}}</span></label> 
              <label v-if="question.isRadioButton"><span class="firstLevelAnswer"><input type="radio" class="big-checkbox" name="myvalue"/>{{firstLevelAnswer.answer}}</span></label>
               
                 <div style="display: inline-block;" v-if="firstLevelAnswer.isTextBox"> 
                    <span v-if="firstLevelAnswer.isSelected"><input type="text" name="myvalue" v-model="firstLevelAnswer.userTextInputValue"/>+ {{firstLevelAnswer.userTextInputValue}}</span>                      
                  </div>                
             
              <span v-if="firstLevelAnswer.isTextInput"><input type="text"/></span>
                   |  firstLevelAnswer.isSelected: {{firstLevelAnswer.isSelected}} 

                   <!-- Second Level -->
                         <div v-if="firstLevelAnswer.isSelected" v-for="secondLevelAnswer in firstLevelAnswer.answers">                          
                            <label v-if="!secondLevelAnswer.isRadioButton"><span class="secondLevelAnswer"><input type="checkbox" class="big-checkbox" v-model="secondLevelAnswer.isSelected" />{{secondLevelAnswer.answer}}</span></label>
                            <label v-if="secondLevelAnswer.isRadioButton"><span class="secondLevelAnswer"><input type="checkbox" class="big-checkbox" v-model="secondLevelAnswer.isSelected" />{{secondLevelAnswer.answer}}</span></label>                       
                           |  secondLevelAnswer.isSelected: {{secondLevelAnswer.isSelected}} 

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

      <!-- Slider section -->
     <div v-if="question.isSlider" class="firstLevelSlider"> 
       <div class="v-range-slider">
          <slot name="left">L</slot>
          <input type="range" v-model="question.value" :value.sync="question.value" :min="min" :max="max" :step="step" :name="name">
          <slot name="right">H</slot>
        </div>
      </div>

         </template>
   </div>  
   
  <div class="sendButton"> 
    <button v-on:click="postQuestionsContent()" :disabled="sendButtonDisable" type="button" class="btn btn-success">Отправить</button>
    {{sendButtonDisable}}
  </div>

          `,
        data: function ()  {
          return {
             questions: [],
             sendButtonDisable : false
          }

          },
          ready() 
          { 
            this.getQuestionsContent()
            checkAuth()

          },

          watch: 
          {
             questions : //what we are watching
             {
                handler: function(val, oldVal) {
                  this.calculateIsSelectedAnswers();
                },
                  deep: true
              },
            
          },


          methods: 
          { 
             getQuestionsContent : function()
             {
                this.$http.get('http://127.0.0.1:8080/js/questions.json').then(function(response)
                {
                  this.questions = response.data;
                }, function(response)
                {
                  console.log("Can't get questions.json from server");
                  console.log("Server response: ", response.status);
                }
                ); 
              },

             postQuestionsContent : function()
             {
                this.sendButtonDisable = true;
                this.$http.post('http://127.0.0.1:8080/questions', JSON.stringify(this.questions)).then(function(response)
                {
                   console.log("Server response: ", response.status);
                   this.sendButtonDisable = true;

                }, function(response)
                  {
                    console.log("Server report that it can't process request");
                    if(response.status == 401)
                    {
                      console.log("User not authorized! ", response.status);
                    }
                  }
                ); 
              },

              calculateIsSelectedAnswers : function() 
              {                
               // console.log(App.username); // имя полученное в переменную username: "",
                for (var question of this.questions)
                { 

                   if(question.question) // small hack to process only questions. We also have {user: name}
                   {
                    this.processOneQuestion(question);
                   }
                   else // походу если нет элемента question то это данные о юзере
                   {
                    this.calculateUserInfo(question);
                   }

                }
              },

              calculateUserInfo : function(question) 
              {
                   console.log("calc user info");
                   console.log(this.questions);
                  // checkAuth();
                  for (var question of this.questions)
                  {
      
                    if (question.username)
                    {
                       console.log("Logined user: ", question.username);
                    }

                  }
                  
                   
                //  
              },


              processOneQuestion: function (question)
              {
                var isSelectedCount = 0;

                for(var answer of question.answers) // just calculation
                {
                  if(answer.isSelected)
                  {
                    isSelectedCount++;
                  }
                  console.log("isSelectedCount --> ", isSelectedCount);

                }

                for(var answer of question.answers) // Enabling/Disabling
                {
                  
                    if(isSelectedCount >= question.MaxAllowedChoice)
                    {
                      console.log("isSelectedCount : " + isSelectedCount + " | question.MaxAllowedChoice: " + question.MaxAllowedChoice);
                      if(!answer.isSelected) // disable unselected
                      {
                        answer.isDisabled = true;
                        console.log("answer.isDisabled = true");
                      }

                    }

                    if(isSelectedCount !== question.MaxAllowedChoice ) // if MaxAllowedChoice less then isSelectedCount set iterated iterated answers isDisabled to false
                    {
                       if(!answer.isSelected) // disable unselected
                       {
                        answer.isDisabled = false;
                        console.log("I hope that answer.isDisabled = false was added")

                       }
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