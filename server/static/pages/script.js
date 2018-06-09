var server_url = "127.0.0.1";

$(document).ready(function () {
    $.onkeyup
});

function suggestion() {
    $("article", "#dataList").remove();
    var data = $("#searchInput").val();
    $.ajax({
        async: false,
        url: server_url + "/api/suggestions/" + data,
        success: function (data) {
            console.log(data);
        },
        error: function (data) {
            console.log("error");
            console.log(data);
        }
    });
}