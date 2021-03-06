# Why is clusterd written in C?

I believe in high-level languages. Most of my software is written in
Haskell. However, my Haskell software is complicated. It is meant to
handle really complicated problems. Complicated problems are things
like modeling user interactions well, or parsing a network byte stream
and handling all corner cases, or providing a simple but extensible
API to express relational queries across a variety of RDBMSs. Those
are difficult, and require higher-level languages.

In contrast, clusterd is easy. The most complicated part of clusterd
is the clusterd-controller state manager's use of Raft to maintain a
global state. Other than that, clusterd's main operation is to
fork/exec() a process on a suitable node in a cluster and then to sit
there and do nothing until something interesting happens. When
something interesting happens, it looks at its state, decides which
services are no longer up, and then starts those services again by
fork/exec()ing them somewhere else. Then, it goes back to sleep and
does nothing. Yes... really, most of the time it does nothing. That's
good. The most reliable software in the world does nothing.

C is a simple programming language, but it is perfectly suitable for
writing low-level code like the simple stuff clusterd does. For the
user-facing part of the code, the CLI utils and the
clusterd-controller API, we use shell-script and lua respectively,
which are higher level languages that can be used for more complicated
behavior. But, internally, clusterd is not complicated.

# Why lua instead of direct SQLite access

SQL is a great programming language, but it is very difficult to
express control flow, looping, etc. Yet, allowing clients to execute
arbitrary state-changing code against the master database is
important, and we'd like to keep network traffic down to a
minimum. Moreover, any wire protocol we'd develop to send rows back
and forth would just be ripe for security issues.

Lua is a well-written, battle-tested, easily-embeddable, and very
small scripting language that provides us with all the higher-level
abstractions anyone would need to compose short chains of
clusterd-controller updates.

Behind the scenes, the lua code is not part of the Raft state
machine. Only SQL statements that actually write the database are
committed to the Raft log.
