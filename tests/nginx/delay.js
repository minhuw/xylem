// Answer after 1 ms, so a connection with one request in flight is busy for
// at least that long: open-loop tests need a service time to wait for
function respond(r) {
    setTimeout(function () {
        r.headersOut['Content-Type'] = 'text/plain';
        r.return(200, "OK\n");
    }, 1);
}

export default { respond };
