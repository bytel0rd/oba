Oba is a real-time data store for streaming events. The initial use case was for streaming logs to clients without worrying about WebSockets and server-side events. The project can be used in two ways
- The encoding library for custom encoding over raw TCP connections found at `network/src/encoding.rs`
- The actual datastore application is found at `server/*`, The example of client implementation can be found at `client/*`

This project works, the data encoding is the most interesting part. Abstract over TCP, any way you like. 
One Love from Nigeria - O.A
