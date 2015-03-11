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

  //// Create variables for divs & classes ////
  var newContainer = $( "<div class='container'/>" );

  // Create Topic Header & Info
  var newTopicRow = $("<div class='row'><div>");
  var newTopicHeader = $("<div class='col s6'><h5>"+topicName+"</h5></div>");
  var newTopicInfo = $(
      "<div class='col s6'>"+
      "<h6> partition(s): "+partitionNum+",  "+"replication factor: "+replicationNum+"</h6>"+
      "</div>");
  newTopicRow.append(newTopicHeader);
  newTopicRow.append(newTopicInfo);
  newContainer.append(newTopicRow);

  // Add ability to insert into a kafka topic
  var newInsertDataRow = $("<div class='row' />");
  var newInsertBoxCol = $("<div class='col s5' />");
  var newInsertInputBox = $(
      "<div class='dataInput input-field'>"+
        "<label for='"+topicName+"'>Add Data To Topic</label>"+
        "<input type='text' id='"+topicName+"'"+
        "class='android-input dataInput' name='customerEmail' />"+
      "</div>");
  var newSubmitBtnCol = $("<div class='col s3' />");
  var newSubmitBtn = $( "<input class='btn dataSubmit' type='submit' value='Submit'/><br></form>");

  newSubmitBtn.click(publishMessage(topicName));

  newInsertBoxCol.append(newInsertInputBox);
  newSubmitBtnCol.append(newSubmitBtn);
  newInsertDataRow.append(newInsertBoxCol);
  newInsertDataRow.append(newSubmitBtnCol);
  newContainer.append(newInsertDataRow);


  // Add ability to search through kafka topic
  var newSearchDataRow = $("<div class='row' />");
  var newSearchBoxCol = $("<div class='col s12' />");
  var newSearchInputBox = $(
      "<div class='input-field'>"+
        "<label for='"+topicName+"Search'>Search Topic</label>"+
        "<input type=text id='"+topicName+"Search'>"+
      "</div>");
  newSearchInputBox.bind('keypress', topicSearchKeyPress(topic));

  newSearchBoxCol.append(newSearchInputBox);
  newSearchDataRow.append(newSearchBoxCol);
  newContainer.append(newSearchDataRow);

  // Add container for topic search results
  var topicSearchResults = $("<div id='"+topicName+"SearchResults' class='searchResults row'></div>");
  topicSearchResults.hide();
  newContainer.append(topicSearchResults);

  // Add topic partition card
  var partitionsCardRow = $("<div class='row' />");
  var partitionsCardCol = $("<div class='col s12' />");
  var partitionsCard = $("<div class='card blue' />");
  var partitionsCardContent = $("<div class='card-content white-text row' />");
  var partitions = $("<div class='col s8'></p>"); // left side of card
  var partitionsData = $("<div class='col s4'></p>"); // right side of card
  partitionsCardContent.append(partitions);
  partitionsCardContent.append(partitionsData);
  partitionsCard.append(partitionsCardContent);
  partitionsCardCol.append(partitionsCard);
  partitionsCardRow.append(partitionsCardCol);
  newContainer.append(partitionsCardRow);

  // Add partitions to partition card
  showPartitions(topic, partitions);

  // Display data range in card
  var newPartitionRange = $(
      "<div class='partitionRange input-field row'>"+
      "<label for='"+topicName+"PartitionRange'>Partition Range</label>"+
      "<input type='text' id='"+topicName+"PartitionRange' />"+
      "</div>" );
  var newDataArea = $("<div class='data row' id='"+topicName+"Data' />");
  newDataArea.hide();
  newPartitionRange.bind('keypress', partitionRangeKeyPress(topic));
  partitionsData.append(newPartitionRange);
  partitionsData.append(newDataArea);


  // var newSubTitle = $( "<div class='subTitle' />");
  var newTopic = $( "<div class='topic' />" );
  var newLeft = $( "<div class='leftFloat' id='"+topicName+"Left'/>");
  var newRight = $( "<div class='rightFloat' />");

  $('main').append(newContainer);
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
