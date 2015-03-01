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

			var clickFun = function(topicName) {
				return function() {
					$.post( "http://private-e3c89-kafkahttp.apiary-mock.com/topics/"+topicName, {data:"hi mom!"},
							function( data ) {
  								console.log('thisworked!')
						}); 
				}
			}

			//Create variables for divs & classes
			var newContainer = $( "<div class='container'/>" );
			var newSubTitle = $( "<div class='subTitle' />");
			var newTopic = $( "<div class='topic' />" );
			var newLeft = $( "<div class='leftFloat' />");
			var newPartition = $( "<div class='partition z-depth-1'/>");
			var newRight = $( "<div class='rightFloat' />");
			var newExport = $( "<div class='partitionDownload' /> <a class='waves-effect waves-light btn small'>Download</a><div class='android-input-wrapper'><input type='text' placeholder='Add Data to Partition' class='android-input'  name='customerEmail' /></div>");
			var newSubmitBtn = $( "<input class='btn' type='submit' value='Submit'/><br></form>");

			newSubmitBtn.click(clickFun(topicName));

			$('main').append(newContainer);
			$('main').append(newContainer);
			newContainer.append(newSubTitle);
			newContainer.append(newTopic);
			newTopic.append(newLeft);
			newTopic.append(newRight);
			newRight.append(newExport);
			newRight.append(newSubmitBtn);

	
			newSubTitle.append("<h5>"+topicName+"</h5>");
			var newAddButton = $( "<div class='partitionButtons' /><span>Add partition for"+topicName+"</span><a class='btn-floating btn-medium waves-effect waves-light lightteal'><i class='mdi-content-add'></i></a><br><br>");
			var partitionAddButton = $("<div class='partitionButtons'><span>Add partition for "+topicName+" "+"</span><a class='btn-floating btn-medium waves-effect waves-light lightteal'><i class='mdi-content-add'></i></a> </div>");
			newContainer.append(partitionAddButton);
			newSubTitle.append("<h6>"+"partition(s): "+partitionNum+",  "+"replication factor(s): "+replicationNum+"</h6>");

			for(j in result.result[i].partition_info){
				partitionVal = result.result[i].partition_info[j].length;
				newLeft.append("<div class='partition z-depth-1'>"+partitionVal+"</div>");
			}
		}
	};


//end tech debt
	$.ajax({url: "http://private-292b6-kafkahttp.apiary-mock.com/topics",
		success: createTopic

		});

//To Do list:
//On-off KYAFKUHH toggle button
//Create topic API 
//Add partition API
//Submit data to KYAVFKYUH
	
})