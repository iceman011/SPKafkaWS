
var socket; //= new SockJS('/gs-guide-websocket');
var stompClient;// = Stomp.over(socket);
var stompClient1;

function setConnected(connected) {
    $("#connect").prop("disabled", connected);
    $("#disconnect").prop("disabled", !connected);
    /*if (connected) {
        $("#conversation").show();
    }
    else {
        $("#conversation").hide();
    }
    $("#vehicle-logs").html("");*/
}

  
	
function connect() {
   socket = new SockJS('/gs-guide-websocket');
    stompClient = Stomp.over(socket);
	
    stompClient.connect({}, function (frame) {
        setConnected(true);
        console.log('Logs Connected: ' + frame);
        stompClient.subscribe('/topic/greetings', function (message) {
			var parsed_msg = JSON.parse(message.body);
			//console.log("client parsed msg "+parsed_msg+" VID > "+parsed_msg.vehicle_id + " Event >" +parsed_msg.event);
			showVehicleLog(parsed_msg.vehicle_id,parsed_msg.event,parsed_msg.timestamp);
        });
    });
}

function disconnect() {
    if (stompClient !== null) {
        stompClient.disconnect();
    }
    setConnected(false);
    console.log("Disconnected");
}

function connect_by_group() {
    //var socket = new SockJS('/gs-guide-websocket-by-group');
    //stompClient1 = Stomp.over(socket);
	stompClient.subscribe('/topic/track/bygroup', function (message) {
			var parsed_msg = JSON.parse(message.body);
			console.log("client parsed msg "+parsed_msg+" group > " +parsed_msg.group);
			updateTrackingByGroup(parsed_msg);
        });
}

function connect_by_vehicle() {
    //var socket = new SockJS('/gs-guide-websocket-by-group');
    //stompClient1 = Stomp.over(socket);
	stompClient.subscribe('/topic/track/byvehicle', function (message) {
			var parsed_msg = JSON.parse(message.body);
			console.log("client parsed msg "+parsed_msg+" vehicle > " +parsed_msg.vehicle);
			updateTrackingByVehicle(parsed_msg);
        });
}


function connect_by_event() {
    //var socket = new SockJS('/gs-guide-websocket-by-group');
    //stompClient1 = Stomp.over(socket);
	stompClient.subscribe('/topic/track/byevent', function (message) {
			var parsed_msg = JSON.parse(message.body);
			console.log("client parsed msg "+parsed_msg+" event > " +parsed_msg.event);
			updateTrackingByEvent(parsed_msg);
        });
}


function sendName() {
    stompClient.send("/app/hello", {}, JSON.stringify({'name': $("#name").val()}));
}

function showVehicleLog(vehicleid,event,timestamp) {
/*date = new Date(timestamp)toUTCString();// * 1000),
datevalues = [
   date.getFullYear(),
   date.getMonth()+1,
   date.getDate(),
   date.getHours(),
   date.getMinutes(),
   date.getSeconds(),
];*/
    $("#vehicle-logs").append("<tr><td>" + timestamp + "</td><td>"+ vehicleid + "</td><td>" + event +"</td></tr>");
}



function updateTrackingByGroup(msg) {
	//console.log( " msg > "+ msg +" group > " + msg.group +" Equality " + (msg.group == "GP1") );
	
	if (msg.group == "GP1" ){
		$("#track_by_group #GP1").html(msg.count);
	}
	else if (msg.group == "GP2" ){
		$("#track_by_group #GP2").html(msg.count);
	}
	else if (msg.group == "GP3" ){
		$("#track_by_group #GP3").html(msg.count);
	}
}

function updateTrackingByVehicle(msg) {
	//console.log( " msg > "+ msg +" group > " + msg.group +" Equality " + (msg.group == "GP1") );
	
	if (msg.vehicle == "BMW" ){
		$("#track_by_vehicle #BMW").html(msg.count);
	}
	else if (msg.vehicle == "TOYTA" ){
		$("#track_by_vehicle #TOYTA").html(msg.count);
	}
	else if (msg.vehicle == "FORD" ){
		$("#track_by_vehicle #FORD").html(msg.count);
	}
}

function updateTrackingByEvent(msg) {
	//console.log( " msg > "+ msg +" group > " + msg.group +" Equality " + (msg.group == "GP1") );
	
	if (msg.event == "GEO-IN" ){
		$("#track_by_event #GEO-IN").html(msg.count);
	}
	else if (msg.event == "GEO-OUT" ){
		$("#track_by_event #GEO-OUT").html(msg.count);
	}
	else if (msg.event == "SPEED" ){
		$("#track_by_event #SPEED").html(msg.count);
	}
}


$(function () {
    $("form").on('submit', function (e) {
        e.preventDefault();
    });
	/*
    $( "#connect" ).click(function() { connect(); });
    $( "#disconnect" ).click(function() { disconnect(); });
	
	$( "#connect_by_group" ).click(function() { connect_by_group(); });
	$( "#connect_by_vehicle" ).click(function() { connect_by_vehicle(); });
    	
    $( "#send" ).click(function() { sendName(); });
	*/
	
	$.ajax({
        url: "/hellokafka/track"
    }).then(function(data) {
       //$('.greeting-id').append(data.id);
       //$('.greeting-content').append(data.content);
	   console.log("Tracking Enabled at Server...");
	   connect();
	   // Connection opened
		socket.addEventListener('open', function (event) {
			connect_by_group();
			connect_by_vehicle();
			connect_by_event();
		});
	   
	   console.log("Client Subsribed at WSocket Server...");
    });
});

