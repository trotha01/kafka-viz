var selectedPartition = 0;
var currentTopic = "";

$(document).ready(function(){
  loadTopics(pollTopic);
  $("#topicDropdownBtn").click(function(){
    showTopicDropdown();
  });
})

var searchTopic = function(currentTopic, keyword) {
  var searchSocket = new WebSocket("ws://localhost:8090/topics/socket/"+currentTopic+"/"+encodeURIComponent(keyword));
  var topicSearchResults = $("#"+currentTopic+"SearchResults");
  topicSearchResults.html("");

  searchSocket.onopen = function (event) {
    if (currentTopic !== "" && keyword !== "") {
        searchSocket.send(currentTopic);
        searchSocket.send(keyword);
    }
  }


  var dataList = $("<ul class='collection'>");
  topicSearchResults.append(dataList);

  var datum;
  searchSocket.onmessage = function (event) {
    data = JSON.parse(event.data);
    topicSearchResults.show();
    dataList.append( $( "<li class='collection-item'><span class='title'>Partition: "+data.partition+" Offset: "+data.offset+"</span><p>"+data.message+"</p></li>"));
  }
}

var pollTopic = function(currentTopic) {
  var topicSocket = new WebSocket("ws://localhost:8090/topics/"+currentTopic+"/poll");

  topicSocket.onopen = function (event) {
    if (currentTopic !== "") {
      topicSocket.send(currentTopic);
    }
  };

  topicSocket.onmessage = function (event) {
    data = JSON.parse(event.data);
    data = data.result[0];
    left = $("#"+data.name+"Left");
    showPartitions(data, left);
  }
}

var showTopicDropdown = function() {
  $("#topics").show();
}

var loadTopics = function(successFunc) {
  var url = "/topics"
    $.ajax({
      url: url,
      success: createTopics
    });
}

var publishMessage = function(topicName) {
  return function() {
    // var url = "http://private-e3c89-kafkahttp.apiary-mock.com/topics/"+topicName
    var url = "/topics/"+topicName;
    data=$("#"+topicName).val();
    $.post( url,
        {data:data},
        function( result ) {
          url = "/topics?topic="+topicName;
          $.get( url,
              function(result) {
                left = $("#"+topicName+"Left");
                showPartitions(result.result[0], left);
              })
        })
    .fail(function() {
      $('.rightFloat').append("<div class='errorBox'<p>Error POSTing!</p>");
    })
  }
}

// Returns partition range from topic input box
// Returns default newest 5, if not specified
var partitionRange = function(topicName, partitionLength) {
  if (partitionLength === 0) {
    $('#'+topicName+"PartitionRange").val("0");
    return 0;
  }
  partitionLength -= 1;
  var lenMinusFive = partitionLength < 5 ? 0 : partitionLength - 5;
  range = "" + lenMinusFive + "-" + partitionLength;
  $('#'+topicName+"PartitionRange").val(range);
  return range;
}

var showPartitionData = function(topicName, partition, range) {

    var dataList = $("<ul class='dataList'>");
    var dataDiv = $('#'+topicName+"Data"); //.html(result.join("<br>"));
    dataDiv.html("");

    var url = "/topics/"+topicName+"/"+partition+"/"+range;
    $.get(url, function(result) {
      for (i in result) {
        message = result[i].message;
        offset = result[i].offset;
        var datum = $( "<li><span class='pull-left'>"+offset+"</span><span class='pull-right'>"+message+"<span></li>");
        datum.append("<hr/>");
        dataList.append(datum);
      }
      dataDiv.append(dataList);
      dataDiv.show();
    });
}

var partitionClick = function(topicName, partition, partitionLength) {
  return function() {
    selectedPartition = partition;
    range = partitionRange(topicName, partitionLength);
    showPartitionData(topicName, partition, range);
  }
}

var selectTopic = function(topic, result) {
  return function() {
    // Display single topic
    for (i in result.result) {
      if (result.result[i].name === topic) {
        $('main').html("");
        showTopicData(result.result[i]);
        return;
      }
    }
  }
}

var fillTopicDropdown = function(result) {
  topicDropdown = $("#topics");
  for(i in result.result) {
    topic = result.result[i].name;
    // console.log(topic);
    var dropdownItem = $( "<li><a href='#!' id='"+topic+"'>"+topic+"</a></li>");
    dropdownItem.click(selectTopic(topic, result));
    topicDropdown.append(dropdownItem);
  }

  $('.dropdown-button').dropdown({
    inDuration: 300,
    outDuration: 225,
    constrain_width: false, // Does not change width of dropdown to that of the activator
    hover: false, // Activate on click
    alignment: 'left', // Aligns dropdown to left or right edge (works with constrain_width)
    gutter: 0, // Spacing from edge
    belowOrigin: false // Displays dropdown below the button
  });
}

var createTopics = function(result) {
  fillTopicDropdown(result);
  showResultData(result);
}

var showResultData = function(result) {
  // for(i in result.result) {
    topic = result.result[0];
    showTopicData(topic);
  // }
}

var showTopicData = function(topic) {
  if(topic.name === ""){
    document.write("No Topics Found!");
    return;
  }
  $("#topicDropdownButton").html(topic.name);

  currentTopic = topic.name;

  var topicName = topic.name;
  var replicationNum = topic.replication;
  var partitionNum = topic.partitions;

  //Create variables for divs & classes
  var newContainer = $( "<div class='container'/>" );
  var newSubTitle = $( "<div class='subTitle' />");
  var newTopic = $( "<div class='topic' />" );
  var newLeft = $( "<div class='leftFloat' id='"+topicName+"Left'/>");
  var newRight = $( "<div class='rightFloat' />");
  var newExport = $( "<div class='dataInput android-input-wrapper'>"+
      "<input type='text' id='"+topicName+"'"+
      "placeholder='Add Data to Topic' class='android-input dataInput'"+
      "name='customerEmail' />"+
      "</div>");
  var newPartitionRange = $( "<div class='partitionRange'><input type=text id='"+topicName+"PartitionRange' placeholder='partition range'/></div>" );
  var newSubmitBtn = $( "<input class='btn dataSubmit' type='submit' value='Submit'/><br></form>");
  var newDataArea = $( "<div class='data', id='"+topicName+"Data';></div>");
  var clear = $( "<div class='clear'></div>" );
  var newTopicNavBar = $( "<div class='subNavBar'></div>");
  newDataArea.hide();

  var partitionAddButton = $("<div class='partitionButtons'><a class='btn-floating btn-medium waves-effect waves-light lightteal'><i class='mdi-content-add'></i></a> </div>");

  newSubmitBtn.click(publishMessage(topicName));
  newPartitionRange.bind('keypress', partitionRangeKeyPress(topic));

  $('main').append(newContainer);
  $('main').append(newContainer);
  newContainer.append(newSubTitle);
  newContainer.append(newTopic);
  newTopic.append(newTopicNavBar);
  newTopic.append(newLeft);
  newTopic.append(newRight);
  newTopicNavBar.append(partitionAddButton);
  newTopicNavBar.append(newPartitionRange);
  newRight.append(newDataArea);

  newSubTitle.append("<h5>"+topicName+"</h5>");
  newSubTitle.append(newExport);
  newSubTitle.append(newSubmitBtn);
  var newAddButton = $( "<div class='partitionButtons' /><a class='btn-floating btn-medium waves-effect waves-light lightteal'><i class='mdi-content-add'></i></a><br><br>");
  newSubTitle.append("<h6>"+"partition(s): "+partitionNum+",  "+"replication factor: "+replicationNum+"</h6>");

  var topicSearch = $("<div><input type=text id='"+topicName+"Search' placeholder='searchTopic'></div>");
  var topicSearchResults = $("<div id='"+topicName+"SearchResults' class='searchResults'></div>");
  topicSearchResults.hide();
  topicSearch.bind('keypress', topicSearchKeyPress(topic));
  newSubTitle.append(topicSearch);
  newSubTitle.append(topicSearchResults);

  var newPartitionRange = $( "<div class='partitionRange'><input type=text id='"+topicName+"PartitionRange' placeholder='partition range'/></div>" );
  newPartitionRange.bind('keypress', partitionRangeKeyPress(topic));


  showPartitions(topic, newLeft);
  pollTopic(topic.name);
}

var showPartitions = function(topic, newLeft) {
  newLeft.html("");
  for(j in topic.partition_info){
    partitionLength = topic.partition_info[j].length;
    partitionId = topic.partition_info[j].id;
    var partitionHTML = $("<div class='btn partition z-depth-1'>"+partitionLength+"</div>");
    newLeft.append(partitionHTML);

    partitionHTML.click(partitionClick(topic.name, partitionId, partitionLength));

  }
}

var partitionRangeKeyPress = function(topic){
  return function(e) {
    if(e.which === 13){
      var range = $(this).children("input").val();
      showPartitionData(topic.name, selectedPartition, range);
    }
  }
}

var topicSearchKeyPress = function(topic){
  return function(e) {
    if(e.which === 13){
      var keyword = $(this).children("input").val();
      searchTopic(topic.name, keyword);
    }
  }
}
