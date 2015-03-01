$(document).ready(function(){
	console.log('dirdel');


	//Create the divs & assign classes
	var newContainer = $( "<div class='container'/>" );
	var newSubTitle = $( "<div class='subTitle' />");







	var partitionAddButton = "<div class='partitionButtons'><span>Add partition for [Topic]</span><a class='btn-floating btn-medium waves-effect waves-light lightteal'><i class='mdi-content-add'></i></a> </div>";

	//Add the divs created according the the data provided by Kafka
	$('main').append(newContainer);
	$('.container').append(newSubTitle);
		//need to grab kafaka Topic name & replication Factor amount and place HTML



        $.getJSON(
          "http://private-292b6-kafkahttp.apiary-mock.com/topics",
          function(data) {
            console.log(data);
          }).fail(function(a,b,c) {
            console.log(a,b,c);
          });

        $.ajax({
          url: "/topics",
          // url: "http://private-292b6-kafkahttp.apiary-mock.com/topics",
          success: function(result){
            console.log(result);
          },
          error: function(a, b, c) {
            console.log(a,b,c)
          }
        });






	// $('.container').append(partitionAddButton);

	











	
})
