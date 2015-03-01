$(document).ready(function(){
  console.log('Kyafkuh. You have to say it like that. ');

  //begin tech debt
  createTopic = function(result) {
    for(i in result.result) {

      if(result.result[i].name === ""){
        document.write("No Topics Found!");
      } else {
        var topicName = result.result[i].name;
        var replicationNum = result.result[i].replication;
        var partitionNum = result.result[i].partitions;
      }

      var partitionClick = function(topicName, partition, partitionLength) {
        return function() {
          partitionLength -= 1;
          var lenMinusFive = partitionLength - 5;
          var partitionRange = "" + lenMinusFive + "-" + partitionLength;
          var url = "/topics/"+topicName+"/"+partition+"/"+partitionRange;
          $.get(url, function(result) {
            console.log(result);
            $('#'+topicName+"Data").html(result.join("<br>"));
          });
        };
      }

      var clickFun = function(topicName) {
        return function() {
          // var url = "http://private-e3c89-kafkahttp.apiary-mock.com/topics/"+topicName
          var url = "/topics/"+topicName
            data=$("#"+topicName).val();
          $.post( url,
              {data:data},
              function( result ) {
                console.log(topicName)
                  $("#"+topicName).val("");
                  $('main').html("");
                  loadTopics();
              })
          .fail(function() {
            $('.rightFloat').append("<div class='errorBox'<p>Error POSTing!</p>");
          })
        }
      }

      //Create variables for divs & classes
      var newContainer = $( "<div class='container'/>" );
      var newSubTitle = $( "<div class='subTitle' />");
      var newTopic = $( "<div class='topic' />" );
      var newLeft = $( "<div class='leftFloat' />");
      var newPartition = $( "<div class='partition z-depth-1'/>");
      var newRight = $( "<div class='rightFloat' />");
      var newExport = $( "<div class='partitionDownload' /> <a class='waves-effect waves-light btn small'>Download</a><div class='android-input-wrapper'><input type='text' id='"+topicName+"' placeholder='Add Data to Partition' class='android-input'  name='customerEmail' /></div>");
      var newSubmitBtn = $( "<input class='btn' type='submit' value='Submit'/><br></form>");
      var newDataArea = $( "<div id='"+topicName+"Data';></div>");

      var partitionAddButton = $("<div class='partitionButtons'><a class='btn-floating btn-medium waves-effect waves-light lightteal'><i class='mdi-content-add'></i></a> </div>");

      newSubmitBtn.click(clickFun(topicName));

      $('main').append(newContainer);
      $('main').append(newContainer);
      newContainer.append(newSubTitle);
      newContainer.append(newTopic);
      newTopic.append(newLeft);
      newTopic.append(newRight);
      newLeft.append(partitionAddButton);
      newRight.append(newExport);
      newRight.append(newSubmitBtn);
      newRight.append(newDataArea);

      newSubTitle.append("<h5>"+topicName+"</h5>");
      var newAddButton = $( "<div class='partitionButtons' /><a class='btn-floating btn-medium waves-effect waves-light lightteal'><i class='mdi-content-add'></i></a><br><br>");
      newSubTitle.append("<h6>"+"partition(s): "+partitionNum+",  "+"replication factor: "+replicationNum+"</h6>");


      for(j in result.result[i].partition_info){
        partitionLength = result.result[i].partition_info[j].length;
        var partitionHTML = $("<div class='btn partition z-depth-1'>"+partitionLength+"</div>")
        newLeft.append(partitionHTML);
        partitionHTML.click(partitionClick(topicName, j, partitionLength));
      }
    }
  };


  //end tech debt
  // var url = "http://private-292b6-kafkahttp.apiary-mock.com/topics"
  loadTopics();
  //To Do list:
  //On-off KYAFKUHH toggle button
  //Create topic API 
  //Submit data to KYAVFKYUH

})

function loadTopics() {
  var url = "/topics"
  $.ajax({
    url: url,
    success: createTopic

  });
}


