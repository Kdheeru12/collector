Resource SchemaURL: https://opentelemetry.io/schemas/1.4.0
Resource attributes:
     -> service.name: Str(user-service)
ScopeSpans #0
ScopeSpans SchemaURL: 
InstrumentationScope github.com/XSAM/otelsql 0.14.1
Span #0
    Trace ID       : ab8013623652634a31264b34abd3cc95
    Parent ID      : 31563643cfd31517
    ID             : 5fcefbf2f9e459fa
    Name           : sql.conn.reset_session
    Kind           : Client
    Start time     : 2023-06-02 22:59:14.682301 +0000 UTC
    End time       : 2023-06-02 22:59:14.682303958 +0000 UTC
    Status code    : Unset
    Status message : 
Span #1
    Trace ID       : ab8013623652634a31264b34abd3cc95
    Parent ID      : 31563643cfd31517
    ID             : 00a3ccdb04e2fe19
    Name           : sql.conn.prepare
    Kind           : Client
    Start time     : 2023-06-02 22:59:14.682312 +0000 UTC
    End time       : 2023-06-02 22:59:14.686184125 +0000 UTC
    Status code    : Unset
    Status message : 
Attributes:
     -> db.statement: Str(update USERS set AMOUNT = AMOUNT + ? where ID = ?)
Span #2
    Trace ID       : ab8013623652634a31264b34abd3cc95
    Parent ID      : 31563643cfd31517
    ID             : a3b770c81a24386a
    Name           : sql.conn.reset_session
    Kind           : Client
    Start time     : 2023-06-02 22:59:14.68622 +0000 UTC
    End time       : 2023-06-02 22:59:14.686220791 +0000 UTC
    Status code    : Unset
    Status message : 
Span #3
    Trace ID       : ab8013623652634a31264b34abd3cc95
    Parent ID      : 31563643cfd31517
    ID             : 242db11da84a221e
    Name           : sql.stmt.exec
    Kind           : Client
    Start time     : 2023-06-02 22:59:14.686244 +0000 UTC
    End time       : 2023-06-02 22:59:14.692504083 +0000 UTC
    Status code    : Unset
    Status message : 
Attributes:
     -> db.statement: Str(update USERS set AMOUNT = AMOUNT + ? where ID = ?)
ScopeSpans #1
ScopeSpans SchemaURL: 
InstrumentationScope user-service 
Span #0
    Trace ID       : ab8013623652634a31264b34abd3cc95
    Parent ID      : 5b831eefd122c7a4
    ID             : 31563643cfd31517
    Name           : update user amount
    Kind           : Internal
    Start time     : 2023-06-02 22:59:14.68229 +0000 UTC
    End time       : 2023-06-02 22:59:14.692541333 +0000 UTC
    Status code    : Unset
    Status message : 
ScopeSpans #2
ScopeSpans SchemaURL: 
InstrumentationScope go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux semver:0.32.0
Span #0
    Trace ID       : ab8013623652634a31264b34abd3cc95
    Parent ID      : 72ef0c4ed57df8fb
    ID             : 5b831eefd122c7a4
    Name           : /users/{userID}
    Kind           : Server
    Start time     : 2023-06-02 22:59:14.682178 +0000 UTC
    End time       : 2023-06-02 22:59:14.692543667 +0000 UTC
    Status code    : Unset
    Status message : 
Attributes:
     -> net.transport: Str(ip_tcp)
     -> net.peer.ip: Str(127.0.0.1)
     -> net.peer.port: Int(59744)
     -> net.host.name: Str(localhost)
     -> net.host.port: Int(8080)
     -> http.method: Str(PUT)
     -> http.target: Str(/users/18)
     -> http.server_name: Str(user-service)
     -> http.route: Str(/users/{userID})
     -> http.user_agent: Str(Go-http-client/1.1)
     -> http.request_content_length: Int(16)
     -> http.scheme: Str(http)
     -> http.host: Str(localhost:8080)
     -> http.flavor: Str(1.1)
     -> http.status_code: Int(200)
        {"kind": "exporter", "data_type": "traces", "name": "logging"}


Resource SchemaURL: https://opentelemetry.io/schemas/1.4.0

Resource attributes:
     -> service.name: Str(user-service)
ScopeSpans #0
ScopeSpans SchemaURL: 
InstrumentationScope github.com/XSAM/otelsql 0.14.1
Span #0
    Trace ID       : 271220db85f96a91148029ca8dc6a370
    Parent ID      : ffdea3da840fbe94
    ID             : ddb154e66bdb9d64
    Name           : sql.conn.reset_session
    Kind           : Client
    Start time     : 2023-06-02 23:01:48.659339 +0000 UTC
    End time       : 2023-06-02 23:01:48.659339416 +0000 UTC
    Status code    : Unset
    Status message : 
Span #1
    Trace ID       : 271220db85f96a91148029ca8dc6a370
    Parent ID      : ffdea3da840fbe94
    ID             : f26b49c2b00d8800
    Name           : sql.conn.prepare
    Kind           : Client
    Start time     : 2023-06-02 23:01:48.659346 +0000 UTC
    End time       : 2023-06-02 23:01:48.663181583 +0000 UTC
    Status code    : Error
    Status message : 
Attributes:
     -> db.statement: Str(INSERT INTO USERS(USER_NAME, ACCOUNT) VALUES (?, ?,""))
Events:
SpanEvent #0
     -> Name: exception
     -> Timestamp: 2023-06-02 23:01:48.66318 +0000 UTC
     -> DroppedAttributesCount: 0
     -> Attributes::
          -> exception.type: Str(*mysql.MySQLError)
          -> exception.message: Str(Error 1136: Column count doesn't match value count at row 1)
ScopeSpans #1
ScopeSpans SchemaURL: 
InstrumentationScope user-service 
Span #0
    Trace ID       : 271220db85f96a91148029ca8dc6a370
    Parent ID      : b77c80d54606673d
    ID             : ffdea3da840fbe94
    Name           : create user
    Kind           : Internal
    Start time     : 2023-06-02 23:01:48.659333 +0000 UTC
    End time       : 2023-06-02 23:01:48.663230209 +0000 UTC
    Status code    : Unset
    Status message : 
ScopeSpans #2
ScopeSpans SchemaURL: 
InstrumentationScope go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux semver:0.32.0
Span #0
    Trace ID       : 271220db85f96a91148029ca8dc6a370
    Parent ID      : 
    ID             : b77c80d54606673d
    Name           : /users
    Kind           : Server
    Start time     : 2023-06-02 23:01:48.659267 +0000 UTC
    End time       : 2023-06-02 23:01:48.663233792 +0000 UTC
    Status code    : Error
    Status message : 
Attributes:
     -> net.transport: Str(ip_tcp)
     -> net.peer.ip: Str(127.0.0.1)
     -> net.peer.port: Int(59751)
     -> net.host.name: Str(localhost)
     -> net.host.port: Int(8080)
     -> http.method: Str(POST)
     -> http.target: Str(/users)
     -> http.server_name: Str(user-service)
     -> http.route: Str(/users)
     -> http.user_agent: Str(Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36)
     -> http.request_content_length: Int(41)
     -> http.scheme: Str(http)
     -> http.host: Str(localhost:8080)
     -> http.flavor: Str(1.1)
     -> http.status_code: Int(500)
        {"kind": "exporter", "data_type": "traces", "name": "logging"}